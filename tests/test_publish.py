"""Compare-and-publish: a stale generation must never become HEAD.

`Store.publish` is compare-and-swap on HEAD: every writer declares the
base generation its candidate was built on (`expected_base`), and the
base re-check plus HEAD swap run under an exclusive file lock, so
overlapping writers — threads or separate processes — serialize.
Exactly one writer per base wins; every stale writer loses with
`StaleGenerationError` and HEAD keeps the winner's content. Failed and
cancelled builds never reach `publish`, and concurrent readers only ever
observe complete, hash-validated snapshots.
"""

from __future__ import annotations

import os
import threading

import pytest
from conftest import full_build, write_file
from mncs_index.indexer import build_snapshot, finalize, incremental_snapshot
from mncs_index.model import DocRecord, TermRecord
from mncs_index.pipeline import BuildConfig, BuildFailed
from mncs_index.store import StaleGenerationError, Store, StoreError


def _doc(path, digest, size=3, words=1, lines=1, kind=1):
    return DocRecord(
        kind=kind, path=path, digest=digest, size=size, words=words, lines=lines
    )


def _seeded_store(store_dir):
    """Publish a kernel-free generation-0 seed; returns (store, snapshot)."""
    store = Store(store_dir)
    docs = [_doc("a.md", 0xA1), _doc("b.md", 0xB2)]
    terms = [TermRecord(path="a.md", token="alpha", tid=0xA1, seq=0)]
    snap, canon = finalize(None, "seed-corpus", 0, docs, terms, with_mncs_digest=False)
    store.publish(snap, canon, {d.path: 0 for d in docs}, {}, None)
    return store, snap


def _variant(seed_snap, generation, tag, digest):
    """A distinct generation-N candidate built on `seed_snap`'s base."""
    docs = list(seed_snap.docs) + [_doc(f"writer-{tag}.md", digest)]
    terms = list(seed_snap.terms)
    snap, canon = finalize(
        None, seed_snap.snapshot_id, generation, docs, terms, with_mncs_digest=False
    )
    return snap, canon, {d.path: generation for d in docs}


def test_stale_error_is_store_error():
    assert issubclass(StaleGenerationError, StoreError)


def test_publish_contract_rejections(tmp_path):
    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir, exist_ok=True)
    store, seed = _seeded_store(store_dir)

    # Duplicate base: HEAD already moved past 0.
    snap, canon, est = _variant(seed, 1, "dup", 0xD0)
    store.publish(snap, canon, est, {}, 0)
    with pytest.raises(StaleGenerationError):
        store.publish(snap, canon, est, {}, 0)
    assert store.head() == 1

    # Initial-base publish onto a non-empty store loses as stale.
    other, other_canon, other_est = _variant(seed, 0, "zero", 0xE0)
    with pytest.raises(StaleGenerationError):
        store.publish(other, other_canon, other_est, {}, None)
    assert store.head() == 1

    # Skipped generation number is a programming error, never a publish.
    skip, skip_canon, skip_est = _variant(seed, 3, "skip", 0xE1)
    with pytest.raises(StoreError):
        store.publish(skip, skip_canon, skip_est, {}, 1)
    assert store.head() == 1

    # Tampered canonical bytes are rejected before any state changes.
    good, good_canon, good_est = _variant(seed, 2, "ok", 0xE2)
    bad_canon = bytearray(good_canon)
    bad_canon[-2] ^= 0xFF
    with pytest.raises(StoreError):
        store.publish(good, bytes(bad_canon), good_est, {}, 1)
    assert store.head() == 1
    head_snap, _, _ = store.load_head()
    assert head_snap.index_hash == snap.index_hash


def test_two_writers_reversed_completion_stale_loses(tmp_path):
    """The writer that built first but publishes second must lose."""
    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir, exist_ok=True)
    _, seed = _seeded_store(store_dir)

    slow_snap, slow_canon, slow_est = _variant(seed, 1, "slow", 0x51)
    fast_snap, fast_canon, fast_est = _variant(seed, 1, "fast", 0xF4)
    assert slow_snap.index_hash != fast_snap.index_hash

    # Fast (built second) publishes first and wins.
    Store(store_dir).publish(fast_snap, fast_canon, fast_est, {}, 0)
    # Slow (built first, now stale) publishes second and loses.
    with pytest.raises(StaleGenerationError, match="stale base 0"):
        Store(store_dir).publish(slow_snap, slow_canon, slow_est, {}, 0)

    store = Store(store_dir)
    assert store.head() == 1
    head_snap, _, _ = store.load_head()
    assert head_snap.index_hash == fast_snap.index_hash
    # The loser's bytes never reached the live generation file.
    live, _, _ = store.load(1)
    assert live.index_hash == fast_snap.index_hash


def test_three_overlapping_candidates_one_winner(tmp_path):
    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir, exist_ok=True)
    _, seed = _seeded_store(store_dir)
    candidates = [_variant(seed, 1, f"w{i}", 0x100 + i) for i in range(3)]
    assert len({c[0].index_hash for c in candidates}) == 3

    barrier = threading.Barrier(3)
    outcomes = [None] * 3

    def attempt(i):
        snap, canon, est = candidates[i]
        barrier.wait(timeout=30)
        try:
            # Fresh Store per writer: no shared client state, only the
            # directory and its lock coordinate the race.
            Store(store_dir).publish(snap, canon, est, {}, 0)
            outcomes[i] = ("won", snap.index_hash)
        except StaleGenerationError:
            outcomes[i] = ("stale", None)

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert all(o is not None for o in outcomes)

    winners = [o for o in outcomes if o[0] == "won"]
    assert len(winners) == 1
    assert sum(1 for o in outcomes if o[0] == "stale") == 2
    store = Store(store_dir)
    assert store.head() == 1
    head_snap, _, _ = store.load_head()
    assert head_snap.index_hash == winners[0][1]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork (POSIX)")
def test_cross_process_race_one_winner(tmp_path):
    """Two processes publishing from the same base: one wins, one is stale."""
    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir, exist_ok=True)
    _, seed = _seeded_store(store_dir)
    snap_a, canon_a, est_a = _variant(seed, 1, "proc-a", 0xAAA)
    snap_b, canon_b, est_b = _variant(seed, 1, "proc-b", 0xBBB)
    assert snap_a.index_hash != snap_b.index_hash

    ready_r, ready_w = os.pipe()
    go_r, go_w = os.pipe()
    res_r, res_w = os.pipe()
    pid = os.fork()
    if pid == 0:  # child: writer B
        try:
            os.close(ready_r)
            os.close(go_w)
            os.close(res_r)
            os.write(ready_w, b"r")
            assert os.read(go_r, 1) == b"g"
            try:
                Store(store_dir).publish(snap_b, canon_b, est_b, {}, 0)
                os.write(res_w, b"W" + snap_b.index_hash.encode())
            except StaleGenerationError:
                os.write(res_w, b"S")
            except Exception:  # noqa: BLE001 — must still report, never hang
                os.write(res_w, b"E")
        finally:
            os._exit(0)
    os.close(ready_w)
    os.close(go_r)
    os.close(res_w)
    assert os.read(ready_r, 1) == b"r"
    os.write(go_w, b"g")
    try:
        Store(store_dir).publish(snap_a, canon_a, est_a, {}, 0)
        parent_outcome = ("won", snap_a.index_hash)
    except StaleGenerationError:
        parent_outcome = ("stale", None)
    _, status = os.waitpid(pid, 0)
    assert status == 0
    child_raw = b""
    while True:
        chunk = os.read(res_r, 128)
        if not chunk:
            break
        child_raw += chunk
    assert child_raw[:1] in (b"W", b"S")
    child_outcome = (
        ("won", child_raw[1:].decode()) if child_raw[:1] == b"W" else ("stale", None)
    )

    results = [parent_outcome, child_outcome]
    assert sum(1 for r in results if r[0] == "won") == 1
    assert sum(1 for r in results if r[0] == "stale") == 1
    winner_hash = next(r[1] for r in results if r[0] == "won")
    store = Store(store_dir)
    assert store.head() == 1
    head_snap, _, _ = store.load_head()
    assert head_snap.index_hash == winner_hash


def test_readers_during_publish_see_only_complete_snapshots(tmp_path, kernels):
    """Concurrent readers never observe torn state while HEAD advances."""
    from mncs_index.query import QueryEngine

    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir, exist_ok=True)
    _, seed = _seeded_store(store_dir)
    generations = 12
    expected = {}
    stop = threading.Event()
    observed: set = set()
    errors: list = []
    lock = threading.Lock()

    def read_loop():
        try:
            while not stop.is_set():
                snap, _, _ = Store(store_dir).load_head()
                assert snap is not None
                with lock:
                    observed.add(snap.generation)
        except Exception as exc:  # noqa: BLE001 — any tear is a failure
            with lock:
                errors.append(exc)

    readers = [threading.Thread(target=read_loop) for _ in range(4)]
    for t in readers:
        t.start()
    try:
        prev = seed
        for gen in range(1, generations + 1):
            snap, canon, est = _variant(prev, gen, f"seq{gen}", 0x200 + gen)
            Store(store_dir).publish(snap, canon, est, {}, gen - 1)
            expected[gen] = snap.index_hash
            # Hold each generation briefly so readers overlap every publish.
            stop.wait(0.005)
            prev = snap
    finally:
        stop.set()
    for t in readers:
        t.join(timeout=60)
    assert not errors
    assert observed and observed <= set(range(generations + 1))

    store = Store(store_dir)
    assert store.head() == generations
    head_snap, _, _ = store.load_head()
    assert head_snap.index_hash == expected[generations]
    # A snapshot object predates later publishes: querying it after HEAD
    # advanced still answers from its own generation.
    old_snap, _, _ = store.load(2)
    engine = QueryEngine(old_snap, kernels, workers=2)
    assert engine.by_path("a.md").generation == 2


@pytest.fixture(scope="module")
def baseline(kernels, tmp_path_factory):
    """One real generation-0 build; kernel tests copy it for isolation."""
    import shutil

    from conftest import FIXTURES

    tmp = tmp_path_factory.mktemp("publish_baseline")
    corpus = tmp / "corpus"
    shutil.copytree(FIXTURES, corpus)
    store = tmp / "store"
    store.mkdir()
    full_build(
        kernels, str(corpus), str(store), generation=0, workers=4, seed=7,
        mncs_digest=False,
    )
    return str(corpus), str(store)


@pytest.fixture()
def branch(baseline, tmp_path):
    import shutil

    corpus_src, store_src = baseline
    corpus = tmp_path / "corpus"
    shutil.copytree(corpus_src, corpus)
    store = tmp_path / "store"
    shutil.copytree(store_src, store)
    return str(corpus), str(store)


def _fresh_kernels(binary):
    from conftest import SRC
    from mncs_index.bridge import Bridge
    from mncs_index.kernels import Kernels

    return Kernels(Bridge(binary=binary, src_dir=SRC))


def test_failed_writer_publishes_nothing_while_winner_publishes(
    kernels, binary, branch, monkeypatch, tmp_path
):
    """A poisoned build overlapping a winning publish changes nothing."""
    import shutil

    corpus_dir, store_dir = branch
    winner_corpus = str(tmp_path / "winner-corpus")
    shutil.copytree(corpus_dir, winner_corpus)
    write_file(winner_corpus, "b.md", b"# Winner\n\nwinner content\n")

    loser_kernels = _fresh_kernels(binary)
    real_leaf = loser_kernels.leaf_window

    def poisoned(state, chunk, open_in):
        if b"quick brown" in chunk:
            raise RuntimeError("injected worker failure")
        return real_leaf(state, chunk, open_in)

    monkeypatch.setattr(loser_kernels, "leaf_window", poisoned)
    gate: threading.Barrier = threading.Barrier(3)
    outcomes: dict = {}

    def loser():
        gate.wait(timeout=60)
        try:
            build_snapshot(
                corpus_dir,
                1,
                loser_kernels,
                BuildConfig(workers=4, seed=7, mncs_digest=False),
            )
            outcomes["loser"] = "built"
        except BuildFailed:
            outcomes["loser"] = "failed"
        except Exception as exc:  # noqa: BLE001 — record, do not leak
            outcomes["loser"] = f"other:{type(exc).__name__}"

    def winner():
        gate.wait(timeout=60)
        store = Store(store_dir)
        prev, established, prev_crc = store.load_head()
        snap, canon, corpus, _ = incremental_snapshot(
            winner_corpus,
            1,
            prev,
            prev_crc,
            kernels,
            BuildConfig(workers=4, seed=7, mncs_digest=False),
        )
        estat = dict(established)
        for d in snap.docs:
            estat.setdefault(d.path, 1)
        store.publish(
            snap, canon, estat, {it.path: it.crc for it in corpus.items},
            prev.generation,
        )
        outcomes["winner"] = snap.index_hash

    losers = threading.Thread(target=loser)
    winners = threading.Thread(target=winner)
    losers.start()
    winners.start()
    gate.wait(timeout=60)
    losers.join(timeout=300)
    winners.join(timeout=300)
    assert outcomes.get("loser") == "failed"
    assert "winner" in outcomes
    store = Store(store_dir)
    assert store.head() == 1
    head_snap, _, _ = store.load_head()
    assert head_snap.index_hash == outcomes["winner"]


def test_cancelled_writer_publishes_nothing_while_winner_publishes(
    kernels, binary, branch, tmp_path
):
    """A cancelled build overlapping a winning publish changes nothing."""
    import shutil

    corpus_dir, store_dir = branch
    winner_corpus = str(tmp_path / "winner-corpus")
    shutil.copytree(corpus_dir, winner_corpus)
    write_file(winner_corpus, "b.md", b"# Winner\n\nwinner content\n")

    cancel = threading.Event()
    cancel.set()  # cancelled before starting: deterministic, never racy
    gate: threading.Barrier = threading.Barrier(3)
    outcomes: dict = {}

    def loser():
        from mncs_index.pipeline import BuildCancelled

        gate.wait(timeout=60)
        try:
            build_snapshot(
                corpus_dir,
                1,
                _fresh_kernels(binary),
                BuildConfig(workers=4, cancel_event=cancel, mncs_digest=False),
            )
            outcomes["loser"] = "built"
        except (BuildCancelled, BuildFailed):
            outcomes["loser"] = "cancelled"
        except Exception as exc:  # noqa: BLE001 — record, do not leak
            outcomes["loser"] = f"other:{type(exc).__name__}"

    def winner():
        gate.wait(timeout=60)
        store = Store(store_dir)
        prev, established, prev_crc = store.load_head()
        snap, canon, corpus, _ = incremental_snapshot(
            winner_corpus,
            1,
            prev,
            prev_crc,
            kernels,
            BuildConfig(workers=4, seed=7, mncs_digest=False),
        )
        estat = dict(established)
        for d in snap.docs:
            estat.setdefault(d.path, 1)
        store.publish(
            snap, canon, estat, {it.path: it.crc for it in corpus.items},
            prev.generation,
        )
        outcomes["winner"] = snap.index_hash

    losers = threading.Thread(target=loser)
    winners = threading.Thread(target=winner)
    losers.start()
    winners.start()
    gate.wait(timeout=60)
    losers.join(timeout=300)
    winners.join(timeout=300)
    assert outcomes.get("loser") == "cancelled"
    assert "winner" in outcomes
    store = Store(store_dir)
    assert store.head() == 1
    head_snap, _, _ = store.load_head()
    assert head_snap.index_hash == outcomes["winner"]
