"""Founding-invariant tests: identical logical input must yield identical
canonical meaning under any worker count, schedule, or enumeration order.
"""

from conftest import SRC, rebuild_bytes
from mncs_index.bridge import Bridge
from mncs_index.kernels import Kernels


def test_worker_counts_converge(kernels, workdir):
    _, corpus_dir, _ = workdir
    hashes = set()
    prints = set()
    for workers in (1, 2, 4, 8):
        snap, _ = rebuild_bytes(
            kernels, corpus_dir, workers=workers, seed=7, mncs_digest=False
        )
        hashes.add(snap.index_hash)
        prints.add(snap.mncs_fingerprint)
    assert len(hashes) == 1, hashes
    assert len(prints) == 1, prints


def test_repeatability(kernels, workdir):
    _, corpus_dir, _ = workdir
    first, _ = rebuild_bytes(kernels, corpus_dir, workers=4, seed=1, mncs_digest=False)
    for _ in range(2):
        snap, _ = rebuild_bytes(
            kernels, corpus_dir, workers=4, seed=1, mncs_digest=False
        )
        assert snap.index_hash == first.index_hash


def test_kernel_call_counts_reproducible(binary, workdir):
    """Identical builds issue identical MNCS call counts at workers=8.

    Producer memo reads take the kernel lock, so consumers cannot slip
    a duplicate K_TOKVAL/K_TOKDIG submission past a torn read: PRESS-010
    economics are reproducible under concurrency, not just convergent
    in meaning. Fresh handles (cold caches) each run.
    """
    _, corpus_dir, _ = workdir
    counts = []
    for _ in range(2):
        fresh = Kernels(Bridge(binary=binary, src_dir=SRC))
        snap, _ = rebuild_bytes(
            fresh, corpus_dir, workers=8, seed=7, mncs_digest=False
        )
        counts.append((snap.index_hash, fresh.b.stats.calls))
    assert counts[0][0] == counts[1][0]
    assert counts[0][1] == counts[1][1] > 0


def test_seed_perturbation_converges(kernels, workdir):
    _, corpus_dir, _ = workdir
    hashes = set()
    for seed in (1, 7, 99):
        snap, _ = rebuild_bytes(
            kernels, corpus_dir, workers=4, seed=seed, mncs_digest=False
        )
        hashes.add((snap.index_hash, snap.mncs_fingerprint))
    assert len(hashes) == 1


def test_delay_and_queue_perturbation_converges(kernels, workdir):
    _, corpus_dir, _ = workdir
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=4, seed=7, mncs_digest=False)
    for cfg in (
        {"queue_size": 1},
        {"queue_size": 4, "delay_ms": 2.0},
        {"workers": 8, "delay_ms": 3.0},
    ):
        snap, _ = rebuild_bytes(kernels, corpus_dir, seed=7, mncs_digest=False, **cfg)
        assert snap.index_hash == ref.index_hash
        assert snap.mncs_fingerprint == ref.mncs_fingerprint


def test_empty_corpus_is_deterministic(kernels, tmp_path):
    corpus = tmp_path / "empty-corpus"
    corpus.mkdir()
    a, _ = rebuild_bytes(kernels, str(corpus), workers=1, mncs_digest=False)
    b, _ = rebuild_bytes(kernels, str(corpus), workers=8, seed=3, mncs_digest=False)
    assert a.index_hash == b.index_hash
    assert len(a.docs) == 0 and len(a.terms) == 0


def test_canonical_bytes_stable(kernels, workdir):
    _, corpus_dir, _ = workdir
    _, canon_a = rebuild_bytes(
        kernels, corpus_dir, workers=2, seed=5, mncs_digest=False
    )
    _, canon_b = rebuild_bytes(
        kernels, corpus_dir, workers=8, seed=9, mncs_digest=False
    )
    assert canon_a == canon_b
    assert canon_a.startswith(b"mncs-index canonical-v1\n")
    assert canon_a.endswith(b"end\n")


def test_real_concurrency_happened(kernels, workdir):
    """The pipeline must overlap MNCS execution, not serialize it."""
    from mncs_index.indexer import build_snapshot
    from mncs_index.pipeline import BuildConfig

    _, corpus_dir, _ = workdir
    kernels.b.stats.max_in_flight = 0
    calls_before = kernels.b.stats.calls
    build_snapshot(corpus_dir, 0, kernels, BuildConfig(workers=8, seed=7))
    assert kernels.b.stats.calls > calls_before
    assert kernels.b.stats.max_in_flight >= 2


def test_mncs_fingerprint_agrees_across_workers(kernels, workdir):
    """The MNCS-computed canonical digest agrees across worker counts."""
    _, corpus_dir, _ = workdir
    a, _ = rebuild_bytes(kernels, corpus_dir, workers=1, seed=7)
    b, _ = rebuild_bytes(kernels, corpus_dir, workers=8, seed=13)
    assert a.mncs_fingerprint and b.mncs_fingerprint
    assert a.mncs_fingerprint == b.mncs_fingerprint
    assert a.index_hash == b.index_hash
