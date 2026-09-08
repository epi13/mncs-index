"""Scale campaign (bounded proof): family-style corpus, convergence,
mutation batch, and PRESS-010 economics.

Hermetic by design: the corpus stages six of ``mncs-index``'s own
``*.mncs`` sources (three ``src/*.mncs`` kernels plus three small
``pressure/reproducers/*.mncs`` probes, ~10 KB), so this test runs
anywhere the suite runs — no sibling checkouts needed. The cross-repo
``family``-tier campaign over all local ``mncs-*`` repos lives in
``scripts/run_ecosystem.py`` (evidence, not fixtures) and shares
``runner/mncs_index/ecosystem.py`` with this test.

Every verdict below is a real MNCS kernel call through the standard
build/incremental/graph path (PRESS-010: one subprocess per call);
economics are measured from ``Bridge.stats``, never estimated.
"""

import os
import shutil
import time

from mncs_index.ecosystem import (
    apply_mutation_batch,
    assert_tables_equal,
    economics,
    invalidation_roots,
    snapshot_metrics,
    verify_invalidation_agreement,
)
from mncs_index.indexer import build_snapshot_v2, incremental_snapshot_v2
from mncs_index.lineage import resolve as resolve_lineage
from mncs_index.pipeline import BuildConfig
from mncs_index.query import QueryEngine
from mncs_index.store import Store

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Three small kernels (module/fn decls, PRESS mentions) plus three tiny
# probes: ~10 KB, rich enough for defines/press rows, small enough that
# the worker matrix stays bounded under the PRESS-010 cost model.
STAGE_SOURCES = [
    "src/digest.mncs",
    "src/graph.mncs",
    "src/scan.mncs",
    "pressure/reproducers/int-bitwise-xor.mncs",
    "pressure/reproducers/probe-host-read.mncs",
    "pressure/reproducers/traversal-u64-domain.mncs",
]


def _stage_self(tmp_path) -> str:
    """Copy the hermetic family-style corpus into a fresh directory."""
    stage = str(tmp_path / "scale-corpus")
    for rel in STAGE_SOURCES:
        dest = os.path.join(stage, "mncs-index", rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copyfile(os.path.join(REPO, rel), dest)
    return stage


def _build(kernels, corpus_dir, generation, workers, seed):
    calls_before = kernels.b.stats.calls
    t0 = time.monotonic()
    snap, canon, corpus, _ = build_snapshot_v2(
        corpus_dir, generation, kernels,
        BuildConfig(workers=workers, seed=seed, mncs_digest=False),
    )
    dt = time.monotonic() - t0
    m = snapshot_metrics(
        snap, corpus, dt, kernels.b.stats.calls - calls_before
    )
    m["workers"] = workers
    m["seed"] = seed
    return snap, canon, corpus, m


def test_scale_worker_convergence_metrics_economics(kernels, tmp_path):
    """Self-corpus converges across worker counts; metrics + economics."""
    stage = _stage_self(tmp_path)
    kernels.b.stats.max_in_flight = 0
    snap4, _, _, m4 = _build(kernels, stage, 0, 4, 7)
    assert kernels.b.stats.max_in_flight >= 2, (
        "scale build never overlapped kernel calls"
    )
    snap1, _, _, m1 = _build(kernels, stage, 0, 1, 7)
    assert snap1.index_hash == snap4.index_hash, (
        f"workers=1 diverged: {snap1.index_hash} != {snap4.index_hash}"
    )
    for m in (m4, m1):
        print(f"\nscale metrics={m}")
        assert m["files"] == len(STAGE_SOURCES)
        assert m["declarations"] > 0, "rich extraction found no declarations"
        assert m["relationships"] > 0, "rich extraction found no relationships"
        assert m["pressure_mentions"] > 0, "press rows missing"
        assert m["mncs_calls"] > 0, "no real MNCS kernel calls happened"
        eco = economics(
            m["mncs_calls"], m["wall_s"], m["files"], m["bytes_indexed"]
        )
        print(f"scale PRESS-010 economics={eco}")
        assert eco["calls_per_file"] > 0 and eco["calls_per_kb"] > 0


def test_scale_mutation_batch_incremental_equals_rebuild(kernels, tmp_path):
    """Five-class realistic edits: incremental == rebuild, rich included.

    change (append fn) / add (module + use edge) / remove (delete file) /
    rename (byte-preserving) / RFC+pressure (heading + rfc-ref + press):
    verdicts are checked per path, every table (docs/terms/syms/headings/
    rels/press) is compared canonically, and traversal + invalidation
    sets agree under both the MNCS kernel verdict and the host mirror.
    """
    corpus_dir = str(tmp_path / "corpus")
    os.makedirs(corpus_dir)
    stage = _stage_self(tmp_path / "stage-src")
    for dirpath, _dn, filenames in os.walk(stage):
        for name in filenames:
            full = os.path.join(dirpath, name)
            dest = os.path.join(corpus_dir, os.path.relpath(full, stage))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copyfile(full, dest)
    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir)

    store = Store(store_dir)
    snap0, canon0, corpus0, m0 = _build(kernels, corpus_dir, 0, 2, 7)
    print(f"\nscale gen0 metrics={m0}")
    store.publish(
        snap0, canon0,
        {d.path: 0 for d in snap0.docs},
        {it.path: it.crc for it in corpus0.items},
        None,
    )
    batch = apply_mutation_batch(corpus_dir)

    prev, established, prev_crc = store.load_head()
    inc, _inc_canon, _cc, verdicts = incremental_snapshot_v2(
        corpus_dir, 1, prev, prev_crc, kernels,
        BuildConfig(workers=2, seed=7, mncs_digest=False),
    )
    for path, expect in batch["verdicts"].items():
        assert verdicts.get(path) == expect, (
            f"mutation verdict wrong at {path}: "
            f"{verdicts.get(path)} != {expect}"
        )

    ref, _ref_canon, _rc, m_ref = _build(kernels, corpus_dir, 0, 1, 21)
    print(f"\nscale mutation rebuild metrics={m_ref}")
    rows = assert_tables_equal(inc, ref)
    print(f"scale mutation rows={rows}")
    assert rows["headings"] >= 1, "rfc-press edit left no heading rows"
    assert any(
        r.path == "scale/notes.md" and r.pid == "PRESS-010" for r in ref.press
    ), "rfc-press edit left no PRESS-010 row"

    # Rename lineage: the byte-preserving rename is provable 1:1 content
    # identity, so the advisory layer reports `moved`.
    removed = [
        (d.path, d.digest) for d in snap0.docs if verdicts.get(d.path) == 2
    ]
    added = [(d.path, d.digest) for d in inc.docs if verdicts.get(d.path) == 1]
    moves = [item for item in resolve_lineage(removed, added)
             if item.kind == "moved"]
    assert any(
        item.old == batch["renamed"][0] and item.new == batch["renamed"][1]
        for item in moves
    ), f"rename lineage missing moved link: {moves}"

    roots = invalidation_roots(
        ref.rels, sorted(set(batch["verdicts"]) | {batch["changed"]})
    )
    assert "scale/app.mncs" in roots or "scale/dep.mncs" in roots
    verify_invalidation_agreement(inc.rels, ref.rels, roots, kernels)

    eng_inc = QueryEngine(inc, kernels)
    eng_ref = QueryEngine(ref, kernels)
    assert eng_inc.depends_on("scale/app.mncs").total == 1
    assert (
        eng_inc.depends_on("scale/app.mncs").records
        == eng_ref.depends_on("scale/app.mncs").records
    )
    assert eng_inc.dependents("scale.dep").total == 1
    assert eng_inc.rfc_refs("0007").total >= 1
    assert eng_inc.pressure("PRESS-010").total >= 1
    assert (
        eng_inc.invalidation_set(["scale/dep.mncs"])
        == eng_ref.invalidation_set(["scale/dep.mncs"])
    )
