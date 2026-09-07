"""Concurrent content acquisition: bounded read queue + parallel readers.

The production build path (`indexer._acquire` -> `discover_concurrent`)
is enumeration -> bounded read queue -> parallel readers -> (existing
bounded semantic queue -> MNCS kernel workers in `pipeline.py`). These
tests pin the acquisition stage: deterministic identity under any reader
count/queue bound, real reader overlap, failure/cancellation behavior
with blocked readers, and robustness across skewed layouts. End-to-end
canonical equivalence across worker counts is additionally covered by
`test_determinism.py`, which now runs through the concurrent readers.
"""

from __future__ import annotations

import os
import threading
import time

import pytest
from mncs_index.discover import (
    DiscoveryError,
    ReadStats,
    discover,
    discover_concurrent,
    read_one_file,
)
from mncs_index.errors import BuildCancelled
from mncs_index.indexer import build_snapshot
from mncs_index.pipeline import BuildConfig
from mncs_index.store import Store


def _snap_key(snap):
    return (
        snap.snapshot_id,
        snap.skipped,
        [(it.path, it.size, it.crc, it.data) for it in snap.items],
    )


def _write(root: str, rel: str, data: bytes) -> None:
    full = os.path.join(root, rel)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "wb") as fh:
        fh.write(data)


def _tiny_corpus(root: str, n: int = 60) -> None:
    os.makedirs(root, exist_ok=True)
    for i in range(n):
        _write(root, f"doc-{i:03d}.md", f"# Doc {i}\n\nwords here {i}\n".encode())


def test_concurrent_matches_sequential(workdir):
    """Reader count and queue bound never affect source identity."""
    _, corpus_dir, _ = workdir
    expected = _snap_key(discover(corpus_dir))
    for readers in (1, 2, 8):
        for qs in (1, 2, 64):
            stats = ReadStats()
            got = discover_concurrent(
                corpus_dir, readers=readers, queue_size=qs, stats=stats
            )
            assert _snap_key(got) == expected
            assert stats.files == len(got.items) == 8


def test_empty_corpus_concurrent(tmp_path):
    root = str(tmp_path / "empty")
    os.makedirs(root)
    got = discover_concurrent(root, readers=4, queue_size=1)
    assert _snap_key(got) == _snap_key(discover(root))
    assert got.items == [] and got.skipped == 0


def test_slow_readers_overlap(tmp_path):
    """Parallel readers genuinely overlap: a 4-way barrier must trip."""
    root = str(tmp_path / "corpus")
    _tiny_corpus(root, 4)
    real = {}
    for name in sorted(os.listdir(root)):
        full = os.path.join(root, name)
        real[full] = read_one_file(full)
    barrier = threading.Barrier(4, timeout=30)

    def slow(full):
        barrier.wait(timeout=30)
        return real[full]

    stats = ReadStats()
    got = discover_concurrent(root, readers=4, queue_size=64, read_bytes=slow, stats=stats)
    assert _snap_key(got) == _snap_key(discover(root))
    assert stats.max_in_flight >= 2


def test_failed_reader_fails_discovery(tmp_path):
    """One unreadable file fails the whole acquisition, never partial."""
    root = str(tmp_path / "corpus")
    _tiny_corpus(root, 8)

    def flaky(full):
        if full.endswith("doc-003.md"):
            raise OSError("injected read failure")
        return read_one_file(full)

    with pytest.raises(DiscoveryError):
        discover_concurrent(root, readers=4, queue_size=4, read_bytes=flaky)
    # Failure while several readers are in flight still reports the error.
    with pytest.raises(DiscoveryError):
        discover_concurrent(root, readers=8, queue_size=64, read_bytes=flaky)


def test_failed_reader_end_to_end_publishes_nothing(
    kernels, workdir, monkeypatch
):
    """A read failure aborts the build before any kernel work publishes."""
    _, corpus_dir, store_dir = workdir
    import importlib

    discover_module = importlib.import_module("mncs_index.discover")
    real_read = read_one_file

    def poisoned(full):
        if full.endswith("b.md"):
            raise OSError("injected read failure")
        return real_read(full)

    # Object form: `mncs_index.discover` as an attribute is the `discover`
    # function (re-exported in `__init__`), not the submodule.
    monkeypatch.setattr(discover_module, "read_one_file", poisoned)
    with pytest.raises(DiscoveryError):
        build_snapshot(corpus_dir, 0, kernels, BuildConfig(workers=4, seed=7))
    assert Store(store_dir).head() is None


def test_queue_saturation_small_queue(tmp_path):
    """queue_size=1 (saturated read queue) still completes identically."""
    root = str(tmp_path / "corpus")
    _tiny_corpus(root, 60)
    expected = _snap_key(discover(root))
    stats = ReadStats()
    got = discover_concurrent(root, readers=4, queue_size=1, stats=stats)
    assert _snap_key(got) == expected
    assert stats.files == 60
    # A roomy queue agrees exactly: the bound is backpressure, not meaning.
    roomy = discover_concurrent(root, readers=4, queue_size=64)
    assert _snap_key(roomy) == expected


def test_precancelled_acquisition_reads_nothing(tmp_path):
    root = str(tmp_path / "corpus")
    _tiny_corpus(root, 8)
    cancel = threading.Event()
    cancel.set()
    seen = []

    def counting(full):
        seen.append(full)
        return read_one_file(full)

    with pytest.raises(BuildCancelled):
        discover_concurrent(root, readers=4, read_bytes=counting, cancel_event=cancel)
    assert seen == []


def test_cancellation_releases_blocked_readers(tmp_path):
    """Cancel fires while readers block in slow reads: prompt BuildCancelled."""
    root = str(tmp_path / "corpus")
    _tiny_corpus(root, 6)
    cancel = threading.Event()
    release = threading.Event()
    stats = ReadStats()

    def blocking(full):
        while not release.is_set():
            if cancel.is_set():
                raise BuildCancelled("cancel requested")
            release.wait(0.05)
        return read_one_file(full)

    outcome: dict = {}

    def run():
        try:
            discover_concurrent(
                root, readers=2, queue_size=2, read_bytes=blocking,
                cancel_event=cancel, stats=stats,
            )
            outcome["result"] = "completed"
        except BaseException as exc:  # noqa: BLE001 — recorded for assert
            outcome["result"] = exc

    worker = threading.Thread(target=run)
    start = time.monotonic()
    worker.start()
    try:
        deadline = time.monotonic() + 30
        while stats.max_in_flight < 2 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert stats.max_in_flight >= 2, "readers never blocked in hook"
        cancel.set()
        worker.join(timeout=30)
        elapsed = time.monotonic() - start
        assert not worker.is_alive(), "blocked readers were not released"
        assert isinstance(outcome.get("result"), BuildCancelled), outcome
        assert elapsed < 15, f"cancellation took too long: {elapsed:.1f}s"
    finally:
        release.set()
        cancel.set()
        worker.join(timeout=30)


def test_skewed_sizes(tmp_path):
    """One large file among tiny ones: identical under any reader count."""
    root = str(tmp_path / "corpus")
    os.makedirs(root)
    _write(root, "big.txt", b"lorem ipsum dolor sit amet\n" * 12000)  # ~300 KB
    for i in range(6):
        _write(root, f"tiny-{i}.md", f"# T{i}\n".encode())
    expected = _snap_key(discover(root))
    assert len(expected[2]) == 7
    for readers in (1, 8):
        got = discover_concurrent(root, readers=readers, queue_size=2)
        assert _snap_key(got) == expected


def test_many_tiny_vs_few_large(tmp_path):
    """Opposite layouts both acquire completely and deterministically."""
    many = str(tmp_path / "many")
    os.makedirs(many)
    for i in range(120):
        _write(many, f"m-{i:03d}.md", f"# M{i}\nvalue {i}\n".encode())
    few = str(tmp_path / "few")
    os.makedirs(few)
    for i in range(3):
        _write(few, f"big-{i}.txt", bytes(65 + ((i * 7 + k) % 26) for k in range(40000)))

    for root, count in ((many, 120), (few, 3)):
        expected = _snap_key(discover(root))
        assert len(expected[2]) == count
        for readers, qs in ((1, 1), (8, 2), (8, 64)):
            stats = ReadStats()
            got = discover_concurrent(root, readers=readers, queue_size=qs, stats=stats)
            assert _snap_key(got) == expected
            assert stats.files == count
        # Repeated runs are stable: completion order never escapes.
        again = discover_concurrent(root, readers=8, queue_size=8)
        assert _snap_key(again) == expected


def test_end_to_end_build_agrees_across_reader_counts(kernels, tmp_path):
    """A real (small) build is canonical under workers=1 and workers=8."""
    corpus = str(tmp_path / "corpus")
    os.makedirs(corpus)
    _write(corpus, "a.mncs", b"module demo;\n")
    _write(corpus, "b.md", b"# Title\n\nsome words here\n")
    _write(corpus, "c.json", b'{"key": "value"}\n')
    _write(corpus, "empty.txt", b"")
    _write(corpus, "sub/nested.txt", b"nested content words\n")
    one, canon_one, _, _ = build_snapshot(
        corpus, 0, kernels, BuildConfig(workers=1, seed=7, mncs_digest=False)
    )
    eight, canon_eight, _, _ = build_snapshot(
        corpus, 0, kernels, BuildConfig(workers=8, seed=7, mncs_digest=False)
    )
    assert one.index_hash == eight.index_hash
    assert canon_one == canon_eight
    assert [(d.path, d.digest) for d in one.docs] == [
        (d.path, d.digest) for d in eight.docs
    ]
    assert len(one.docs) == 5
