"""Snapshot-consistent concurrent query/write behavior.

A `QueryEngine` binds exactly one loaded snapshot: publishing newer
generations never mutates an existing engine, and every load observes
one complete, hash-validated snapshot (never a torn write). Many
concurrent readers therefore always agree with *some* published
generation. `limit` is truncation metadata (`total` + `limited` +
prefix), never cancellation: a limited query reports the full total,
stays repeatable, and leaves the engine fully usable.
"""

import shutil
import threading

import pytest
from conftest import write_file
from mncs_index.indexer import build_snapshot_v2, incremental_snapshot_v2
from mncs_index.pipeline import BuildConfig
from mncs_index.query import QueryEngine
from mncs_index.store import Store

HUB = (
    b"mncs 0.8;\n"
    b"\n"
    b"module iso.hub;\n"
    b"\n"
    b"fn hub_fn() -> (result: i64) {\n"
    b"    return 1;\n"
    b"}\n"
)


def _spoke(name: str) -> bytes:
    return (
        b"mncs 0.8;\n"
        b"\n"
        b"module iso." + name.encode() + b";\n"
        b"\n"
        b"use iso.hub as hub\n"
        b"\n"
        b"fn " + name.encode() + b"_fn() -> (result: i64) {\n"
        b"    return 0;\n"
        b"}\n"
    )


@pytest.fixture(scope="module")
def iso_base(kernels, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("iso")
    corpus = tmp / "corpus"
    corpus.mkdir()
    write_file(str(corpus), "hub.mncs", HUB)
    write_file(str(corpus), "a.mncs", _spoke("a"))
    write_file(str(corpus), "b.mncs", _spoke("b"))
    store = tmp / "store"
    store.mkdir()
    cfg = BuildConfig(workers=4, seed=7, mncs_digest=False)
    snap, canon, corpus_snap, _ = build_snapshot_v2(str(corpus), 0, kernels, cfg)
    Store(str(store)).publish(
        snap,
        canon,
        {d.path: 0 for d in snap.docs},
        {it.path: it.crc for it in corpus_snap.items},
        None,
    )
    return str(corpus), str(store)


@pytest.fixture()
def iso_copy(iso_base, tmp_path):
    corpus_src, store_src = iso_base
    corpus = tmp_path / "corpus"
    store = tmp_path / "store"
    shutil.copytree(corpus_src, corpus)
    shutil.copytree(store_src, store)
    return str(corpus), str(store)


def _publish_next(kernels, corpus_dir, store_dir, generation):
    store = Store(store_dir)
    prev, established, prev_crc = store.load_head()
    snap, canon, corpus, _ = incremental_snapshot_v2(
        corpus_dir,
        generation,
        prev,
        prev_crc,
        kernels,
        BuildConfig(workers=4, seed=7, mncs_digest=False),
    )
    estat = dict(established)
    for d in snap.docs:
        estat.setdefault(d.path, generation)
    store.publish(
        snap, canon, estat, {it.path: it.crc for it in corpus.items}, prev.generation
    )
    return snap


def _engine(kernels, store_dir, workers=2):
    snap, established, _ = Store(store_dir).load_head()
    return QueryEngine(snap, kernels, workers=workers, established=established)


def test_reader_binds_one_snapshot_across_publish(kernels, iso_copy):
    corpus_dir, store_dir = iso_copy
    eng0 = _engine(kernels, store_dir)
    before_id = eng0.snap.snapshot_id
    before_hash = eng0.snap.index_hash
    before_total = eng0.term_substring("return").total
    assert before_total == 3

    write_file(corpus_dir, "c.mncs", _spoke("c"))
    _publish_next(kernels, corpus_dir, store_dir, 1)

    # The old engine is untouched: same identity, same answers.
    assert eng0.snap.generation == 0
    assert eng0.snap.snapshot_id == before_id
    assert eng0.snap.index_hash == before_hash
    assert eng0.by_path("c.mncs").total == 0
    assert eng0.term_substring("return").total == before_total
    assert eng0.dependents("iso.hub").total == 2

    # A fresh load binds the new snapshot and sees the new file.
    eng1 = _engine(kernels, store_dir)
    assert eng1.snap.generation == 1
    assert eng1.snap.snapshot_id != before_id
    assert eng1.by_path("c.mncs").total == 1
    assert eng1.dependents("iso.hub").total == 3


def test_many_concurrent_readers_see_only_complete_snapshots(kernels, iso_copy):
    corpus_dir, store_dir = iso_copy
    stop = threading.Event()
    barrier = threading.Barrier(8, timeout=60)
    observations: list = []
    lock = threading.Lock()

    def reader():
        barrier.wait(timeout=60)
        local = []
        while not stop.is_set():
            eng = _engine(kernels, store_dir)
            res = eng.term_substring("return")
            deps = eng.dependents("iso.hub")
            keys = [d.sort_key() for d in res.records]
            assert keys == sorted(keys)
            assert res.total >= 3
            assert (res.snapshot_id, res.generation) == (
                deps.snapshot_id,
                deps.generation,
            )
            local.append((res.generation, res.snapshot_id, res.total))
        with lock:
            observations.extend(local)

    threads = [threading.Thread(target=reader) for _ in range(8)]
    for t in threads:
        t.start()
    try:
        write_file(corpus_dir, "c.mncs", _spoke("c"))
        _publish_next(kernels, corpus_dir, store_dir, 1)
        write_file(corpus_dir, "d.mncs", _spoke("d"))
        _publish_next(kernels, corpus_dir, store_dir, 2)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=60)
    assert all(not t.is_alive() for t in threads)
    assert observations, "readers observed nothing"
    # Loads are hash-validated complete snapshots of real generations.
    assert {gen for gen, _, _ in observations} <= {0, 1, 2}
    by_gen = {}
    for gen, sid, total in observations:
        by_gen.setdefault(gen, set()).add((sid, total))
    for gen, seen in by_gen.items():
        assert len(seen) == 1, f"generation {gen} was observed inconsistently: {seen}"
    totals = {gen: next(iter(seen))[1] for gen, seen in by_gen.items()}
    # One new .mncs file per generation, each contributing one
    # "return" token: totals grow 3 -> 4 -> 5 across publishes.
    assert totals.get(0, 3) == 3
    if 1 in totals:
        assert totals[1] == 4
    if 2 in totals:
        assert totals[2] == 5


def test_limit_is_truncation_not_cancellation(kernels, iso_copy):
    _, store_dir = iso_copy
    eng = _engine(kernels, store_dir)
    full = eng.term_substring("return")
    assert full.total == 3 and full.limited is False

    lim = eng.term_substring("return", limit=1)
    # The full total survives the limit; the flag says truncated.
    assert lim.total == full.total == 3
    assert lim.limited is True
    assert [d.path for d in lim.records] == [
        d.path for d in full.records[:1]
    ]

    # Repeatable: no cancellation state, no poisoning — the engine
    # answers identically afterwards, limited or not.
    again = eng.term_substring("return", limit=1)
    assert [d.path for d in again.records] == [d.path for d in lim.records]
    assert again.total == 3 and again.limited is True
    assert eng.term_substring("return").total == 3
    assert eng.by_path("hub.mncs").total == 1
    assert eng.depends_on("a.mncs").total == 1
