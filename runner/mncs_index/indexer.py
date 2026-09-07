"""Snapshot construction: full builds and incremental updates.

Full build: every file through the concurrent pipeline, then canonical
merge. Incremental: hint-triage (size + crc32) selects a candidate reuse
set, but hints are never content truth — every hint-hit path is still run
through the pipeline for authoritative MNCS-content validation, and stored
records are reused only when the recomputed MNCS digest agrees; every
keep/add/remove/modify verdict is an MNCS `classify_change` decision.
"""

from __future__ import annotations

from .bridge import Bridge
from .discover import CorpusSnapshot, discover_concurrent
from .extract import (
    build_rich,
    extract_many,
    filerich_from_records,
    globalize,
    path_extension,
)
from .kernels import Kernels
from .model import (
    DocRecord,
    Snapshot,
    TermRecord,
    canonical_bytes,
    canonical_bytes_v2,
    index_hash_of,
    mncs_fingerprint_of,
)
from .pipeline import BuildConfig, BuildFailed, FileResult, Pipeline


def results_to_records(results: list[FileResult]) -> tuple[list, list]:
    docs: list[DocRecord] = []
    terms: list[TermRecord] = []
    for res in results:
        plan = res.plan
        docs.append(
            DocRecord(
                kind=plan.kind,
                path=plan.path,
                digest=res.digest,
                size=len(plan.data),
                words=res.words,
                lines=res.lines,
            )
        )
        for seq, (tok, tid) in enumerate(res.terms):
            terms.append(TermRecord(path=plan.path, token=tok, tid=tid, seq=seq))
    docs.sort(key=lambda d: d.sort_key())
    terms.sort(key=lambda t: t.sort_key())
    return docs, terms


def finalize(
    kernels: Kernels,
    snapshot_id: str,
    generation: int,
    docs,
    terms,
    with_mncs_digest: bool = True,
    fp_workers: int = 8,
) -> tuple[Snapshot, bytes]:
    snap = Snapshot(
        snapshot_id=snapshot_id, generation=generation, docs=docs, terms=terms
    )
    data = canonical_bytes(snapshot_id, snap.sorted_records())
    snap.index_hash = index_hash_of(data)
    snap.mncs_fingerprint = (
        f"{mncs_fingerprint_of(kernels, data, fp_workers):016x}"
        if with_mncs_digest
        else ""
    )
    return snap, data


def finalize_v2(
    kernels: Kernels,
    snapshot_id: str,
    generation: int,
    docs,
    terms,
    tables,
    with_mncs_digest: bool = True,
    fp_workers: int = 8,
) -> tuple[Snapshot, bytes]:
    """Assemble a canonical-v2 snapshot: v1 tables plus rich tables.

    Hashes cover the v2 bytes (v2 marker + v1-identical doc/term section
    + rich section). Degradation rule (RFC 0007): when every rich table
    is empty there is no v2 meaning to carry, so this delegates to
    `finalize` and returns byte-identical v1 bytes — an empty v2
    projection hashes exactly like its v1 snapshot, and `Store.publish`
    (which classifies by row presence) validates it on the v1 path
    instead of rejecting it.
    """
    if not (tables.syms or tables.headings or tables.rels or tables.press):
        return finalize(
            kernels,
            snapshot_id,
            generation,
            docs,
            terms,
            with_mncs_digest=with_mncs_digest,
            fp_workers=fp_workers,
        )
    snap = Snapshot(
        snapshot_id=snapshot_id,
        generation=generation,
        docs=docs,
        terms=terms,
        syms=tables.syms,
        headings=tables.headings,
        rels=tables.rels,
        press=tables.press,
    )
    data = canonical_bytes_v2(snap.sorted_all_records(), snapshot_id)
    snap.index_hash = index_hash_of(data)
    snap.mncs_fingerprint = (
        f"{mncs_fingerprint_of(kernels, data, fp_workers):016x}"
        if with_mncs_digest
        else ""
    )
    return snap, data


def _rich_files(corpus: CorpusSnapshot, kernels: Kernels) -> list[tuple[str, int, bytes]]:
    """(path, kind, data) triples for extraction; kind ranks come from
    the same extension rule as pipeline planning."""
    return [
        (it.path, kernels.classify_kind(path_extension(it.path)), it.data)
        for it in corpus.items
    ]


def build_snapshot_v2(
    root: str,
    generation: int,
    kernels: Kernels,
    config: BuildConfig,
) -> tuple[Snapshot, bytes, CorpusSnapshot, Bridge]:
    """Full v2 build: the untouched v1 full build plus concurrent rich
    extraction over the same corpus, merged deterministically."""
    snap_v1, _data_v1, corpus, bridge = build_snapshot(root, generation, kernels, config)
    tables = build_rich(_rich_files(corpus, kernels), kernels, config.workers)
    snap, data = finalize_v2(
        kernels,
        corpus.snapshot_id,
        generation,
        snap_v1.docs,
        snap_v1.terms,
        tables,
        with_mncs_digest=config.mncs_digest,
        fp_workers=config.workers,
    )
    return snap, data, corpus, bridge


def _acquire(root: str, config: BuildConfig) -> CorpusSnapshot:
    """Genuinely concurrent host content acquisition (temporary
    PRESS-backed infra, PRESS-001/002/003): enumeration -> bounded read
    queue -> parallel readers. Deterministic source identity is preserved
    (sorted items, identity from sorted entries); the caller's
    worker/queue budget bounds both this stage and the kernel stage."""
    return discover_concurrent(
        root,
        readers=max(1, config.workers),
        queue_size=config.queue_size,
        cancel_event=config.cancel_event,
    )


def build_snapshot(
    root: str,
    generation: int,
    kernels: Kernels,
    config: BuildConfig,
) -> tuple[Snapshot, bytes, CorpusSnapshot, Bridge]:
    corpus = _acquire(root, config)
    pipeline = Pipeline(kernels, config)
    results = pipeline.build_all(corpus)
    docs, terms = results_to_records(results)
    snap, data = finalize(
        kernels,
        corpus.snapshot_id,
        generation,
        docs,
        terms,
        with_mncs_digest=config.mncs_digest,
        fp_workers=config.workers,
    )
    return snap, data, corpus, kernels.b


def incremental_snapshot(
    root: str,
    generation: int,
    previous: Snapshot,
    previous_crc: dict[str, int],
    kernels: Kernels,
    config: BuildConfig,
) -> tuple[Snapshot, bytes, CorpusSnapshot, dict[str, int]]:
    """Update `previous` to the current corpus state.

    Size/CRC32 are cheap triage hints only, never content truth: a
    hint-hit path is queued for authoritative MNCS-content validation
    through the pipeline, and its stored record is reused only if the
    recomputed MNCS digest agrees (verdict 0). A same-size CRC32
    collision therefore surfaces as a modify (3), never stale reuse.

    Returns (snapshot, canonical_bytes, corpus, change_verdicts) where
    change_verdicts maps path -> MNCS classify_change code.
    """
    corpus = _acquire(root, config)
    old_docs = {d.path: d for d in previous.docs}
    old_terms: dict[str, list[TermRecord]] = {}
    for t in previous.terms:
        old_terms.setdefault(t.path, []).append(t)

    current = {it.path: it for it in corpus.items}
    verdicts: dict[str, int] = {}
    reused_docs: list[DocRecord] = []
    reused_terms: list[TermRecord] = []
    fresh: list[tuple[str, bytes]] = []

    for path in sorted(set(old_docs) | set(current)):
        old = old_docs.get(path)
        new = current.get(path)
        if (
            old is not None
            and new is not None
            and len(new.data) == old.size
            and new.crc == previous_crc.get(path)
        ):
            # Hint-hit: candidate for reuse, NOT proof of sameness.
            # Queue the current bytes for authoritative validation; the
            # verdict is decided after the fresh MNCS digest lands, and
            # reuse happens only on digest agreement (see below).
            fresh.append((path, new.data))
            verdicts[path] = -1  # decided after the fresh digest lands
        elif old is not None and new is not None:
            fresh.append((path, new.data))
            verdicts[path] = -1  # decided after the fresh digest lands
        elif old is None and new is not None:
            fresh.append((path, new.data))
            verdicts[path] = -1
        else:
            if old is None or new is not None:
                raise BuildFailed(f"incremental bookkeeping error at {path}")
            verdicts[path] = kernels.classify_change(True, old.digest, False, 0)
            if verdicts[path] != 2:
                raise BuildFailed(f"removal verdict wrong at {path}")

    fresh_results = Pipeline(kernels, config).build_files(fresh) if fresh else []
    fresh_docs, fresh_terms = results_to_records(fresh_results)
    fresh_by_path = {d.path: d for d in fresh_docs}
    for path, doc in fresh_by_path.items():
        old = old_docs.get(path)
        if old is None:
            code = kernels.classify_change(False, 0, True, doc.digest)
            if code != 1:
                raise BuildFailed(f"addition verdict wrong at {path}")
        else:
            code = kernels.classify_change(True, old.digest, True, doc.digest)
            if code not in (0, 3):
                raise BuildFailed(f"change verdict wrong at {path}: {code}")
            if code == 0:
                # Authoritative MNCS digests agree — content-identical,
                # whether the hints hit (validated reuse) or moved (hint
                # change with identical content): reuse stored records
                # verbatim. Any digest disagreement is verdict 3, so a
                # same-size CRC32 collision can never reuse stale records.
                fresh_docs = [d for d in fresh_docs if d.path != path]
                fresh_terms = [t for t in fresh_terms if t.path != path]
                reused_docs.append(old)
                reused_terms.extend(old_terms.get(path, []))
        verdicts[path] = code

    docs = sorted(reused_docs + fresh_docs, key=lambda d: d.sort_key())
    terms = sorted(reused_terms + fresh_terms, key=lambda t: t.sort_key())
    snap, data = finalize(
        kernels,
        corpus.snapshot_id,
        generation,
        docs,
        terms,
        with_mncs_digest=config.mncs_digest,
        fp_workers=config.workers,
    )
    return snap, data, corpus, verdicts


def incremental_snapshot_v2(
    root: str,
    generation: int,
    previous: Snapshot,
    previous_crc: dict[str, int],
    kernels: Kernels,
    config: BuildConfig,
) -> tuple[Snapshot, bytes, CorpusSnapshot, dict[str, int]]:
    """Update a v2 snapshot to the current corpus state.

    The v1 incremental transaction (untouched above) decides
    docs/terms/verdicts. Rich rows follow content identity: verdict-0
    paths reuse the previous rich rows verbatim (extraction is a pure
    function of content, so identical bytes mean identical rows);
    added/modified paths are re-extracted; removed paths drop. The merge
    is the same deterministic `globalize`, so incremental == rebuild.
    """
    snap_v1, _data_v1, corpus, verdicts = incremental_snapshot(
        root, generation, previous, previous_crc, kernels, config
    )
    current = {it.path: it for it in corpus.items}
    per_file = {}
    fresh: list[tuple[str, int, bytes]] = []
    for path, item in current.items():
        if verdicts.get(path) == 0:
            per_file[path] = filerich_from_records(path, previous)
        else:
            fresh.append(
                (path, kernels.classify_kind(path_extension(path)), item.data)
            )
    per_file.update(extract_many(fresh, kernels, config.workers))
    merged = globalize(per_file)
    snap, data = finalize_v2(
        kernels,
        corpus.snapshot_id,
        generation,
        snap_v1.docs,
        snap_v1.terms,
        merged,
        with_mncs_digest=config.mncs_digest,
        fp_workers=config.workers,
    )
    return snap, data, corpus, verdicts
