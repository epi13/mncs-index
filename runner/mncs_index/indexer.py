"""Snapshot construction: full builds and incremental updates.

Full build: every file through the concurrent pipeline, then canonical
merge. Incremental: hint-triage (size + crc32) reuses stored records for
unchanged paths and runs the pipeline only for added/changed paths; every
keep/add/remove/modify verdict is an MNCS `classify_change` decision.
"""

from __future__ import annotations

from .bridge import Bridge
from .discover import CorpusSnapshot, discover
from .kernels import Kernels
from .model import (
    DocRecord,
    Snapshot,
    TermRecord,
    canonical_bytes,
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


def build_snapshot(
    root: str,
    generation: int,
    kernels: Kernels,
    config: BuildConfig,
) -> tuple[Snapshot, bytes, CorpusSnapshot, Bridge]:
    corpus = discover(root)
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

    Returns (snapshot, canonical_bytes, corpus, change_verdicts) where
    change_verdicts maps path -> MNCS classify_change code.
    """
    corpus = discover(root)
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
            code = kernels.classify_change(True, old.digest, True, old.digest)
            if code != 0:
                raise BuildFailed(f"hint-unchanged path disagrees: {path}")
            verdicts[path] = code
            reused_docs.append(old)
            reused_terms.extend(old_terms.get(path, []))
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
                # Content-identical despite a hint change (size/crc moved
                # but MNCS digests agree): reuse stored records verbatim.
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
