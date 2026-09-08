"""Multiprocess MVCC (RFC 0009): snapshot handles, pins, reclamation.

Readers open an explicit `PinnedSnapshot` handle (`Store.open` /
`open_head`) — pinning the generation before loading it — so they
never chase HEAD and a concurrent `reclaim` cannot delete the data
under them. `reclaim` deletes only unpinned, non-HEAD generations
below HEAD (reaping dead-process pins first); HEAD and
newer-than-HEAD crash artifacts are never deleted. Races are
exercised across ACTUAL processes (`os.fork`): 1-writer+N-readers
and N-writer+M-readers, plus readers open across a real-death crash
and recovery. Kernel-free (`finalize(..., with_mncs_digest=False)`).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time

import pytest

from mncs_index.cli import main as cli_main
from mncs_index.indexer import finalize
from mncs_index.model import DocRecord, TermRecord
from mncs_index.store import (
    CRASH_EXIT_CODE,
    StaleGenerationError,
    Store,
    StoreError,
)

RUNNER = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/runner"

requires_fork = pytest.mark.skipif(
    not hasattr(os, "fork"), reason="requires os.fork (POSIX)"
)


def _doc(path, digest, size=3, words=1, lines=1, kind=1):
    return DocRecord(
        kind=kind, path=path, digest=digest, size=size, words=words, lines=lines
    )


def _seeded_store(store_dir, durability="L2"):
    store = Store(store_dir, durability=durability)
    docs = [_doc("a.md", 0xA1), _doc("b.md", 0xB2)]
    terms = [TermRecord(path="a.md", token="alpha", tid=0xA1, seq=0)]
    snap, canon = finalize(None, "seed-corpus", 0, docs, terms, with_mncs_digest=False)
    store.publish(snap, canon, {d.path: 0 for d in docs}, {}, None)
    return store, snap


def _candidate(generation, tag, tag_digest):
    docs = [_doc("a.md", 0xA1), _doc(f"{tag}.md", tag_digest)]
    terms = [TermRecord(path="a.md", token="alpha", tid=0xA1, seq=0)]
    return finalize(None, "seed-corpus", generation, docs, terms, with_mncs_digest=False)


def _publish(store_dir, generation, base, tag, tag_digest, durability="L2"):
    snap, canon = _candidate(generation, tag, tag_digest)
    docs = [_doc("a.md", 0xA1), _doc(f"{tag}.md", tag_digest)]
    Store(store_dir, durability=durability).publish(
        snap, canon, {d.path: generation for d in docs}, {}, base
    )
    return snap


def _pins_on_disk(store_dir):
    pins = os.path.join(store_dir, ".pins")
    try:
        return sorted(os.listdir(pins))
    except FileNotFoundError:
        return []


# -- handles --------------------------------------------------------------


def test_handle_binds_one_snapshot_across_publish(tmp_path):
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    store = Store(store_dir)
    with store.open_head() as h:
        assert h.generation == 0
        before_hash = h.snap.index_hash
        assert os.path.basename(h.pin_path) in _pins_on_disk(store_dir)
        assert 0 in store.pinned_generations()
        _publish(store_dir, 1, 0, "n1", 0xC1)
        assert store.head() == 1
        # The open handle is untouched: same generation, same bytes.
        assert h.generation == 0
        assert h.snap.index_hash == before_hash
        snap0, _, _ = store.load(h.generation)
        assert snap0.index_hash == before_hash
    assert h.closed
    h.close()  # idempotent
    assert _pins_on_disk(store_dir) == []
    assert store.pinned_generations() == set()


def test_explicit_generation_open_after_advance(tmp_path):
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    _publish(store_dir, 2, 1, "n2", 0xC2)
    store = Store(store_dir)
    with store.open(0) as h:
        assert h.generation == 0
        assert len(h.snap.docs) == 2
    with store.open_head() as h:
        assert h.generation == 2


def test_open_retry_contract_is_typed(tmp_path, monkeypatch):
    """`open()` retries on the missing-generation TYPE, not its wording.

    A reworded `GenerationMissingError` must still trigger the
    reclaim-race retry: if the retry keyed off `"file missing"`, this
    test (reworded message) would fail instead of binding HEAD.
    """
    from mncs_index.store import GenerationMissingError

    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    store = Store(store_dir)
    with pytest.raises(GenerationMissingError):
        store.load(999)
    try:
        store.load(999)
    except StoreError as exc:
        assert type(exc) is GenerationMissingError
    fired = []
    real_load = Store.load

    def flaky_load(self, generation):
        if not fired:
            fired.append(generation)
            raise GenerationMissingError("totally reworded boom")
        return real_load(self, generation)

    monkeypatch.setattr(Store, "load", flaky_load)
    with store.open_head() as h:
        assert h.generation == 1
    assert fired == [1]


def test_pid_reuse_delays_but_never_deletes(tmp_path):
    """Injected liveness probe models PID reuse in both directions.

    Forced-alive (a dead pid reused by an unrelated process): reclaim
    must skip the generation — delayed reclamation. Forced-dead: the
    pin is reaped and the generation goes. Neither direction ever
    deletes a generation out from under a real live pin.
    """
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    dead_pid = proc.pid
    pins = os.path.join(store_dir, ".pins")
    os.makedirs(pins, exist_ok=True)
    pin_name = f"gen-000000.{dead_pid}.{'b' * 32}.pin"
    with open(os.path.join(pins, pin_name), "w") as fh:
        fh.write("0\n%d\n0\n" % dead_pid)
    store = Store(store_dir)
    assert os.path.exists(os.path.join(store_dir, "gen-000000.json"))
    # PID-reuse model: the dead pid looks alive — generation survives.
    rep = store.reclaim(pid_alive=lambda pid: True)
    assert rep["removed"] == []
    assert os.path.exists(os.path.join(store_dir, "gen-000000.json"))
    # Accurate probe: the dead pin is reaped, the generation goes.
    rep = store.reclaim(pid_alive=lambda pid: False)
    assert rep["removed"] == [0]
    assert not os.path.exists(os.path.join(store_dir, "gen-000000.json"))
    assert store.check()["ok"]


def test_open_empty_store_fails_closed(tmp_path):
    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir)
    with pytest.raises(StoreError):
        Store(store_dir).open_head()
    with pytest.raises(StoreError):
        Store(store_dir).open(0)
    assert _pins_on_disk(store_dir) == []  # failed open leaks no pin


def test_open_missing_generation_unpins_again(tmp_path):
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    with pytest.raises(StoreError):
        Store(store_dir).open(7)
    assert _pins_on_disk(store_dir) == []


def test_pin_rejects_bad_generations(tmp_path):
    store = Store(str(tmp_path / "store"))
    for bad in (-1, "0", None, True):
        with pytest.raises(StoreError):
            store.pin(bad)
    with pytest.raises(StoreError):
        store.reclaim(keep_recent=-1)


# -- reclamation ----------------------------------------------------------


def test_reclaim_keeps_head_and_pinned_only(tmp_path):
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    _publish(store_dir, 2, 1, "n2", 0xC2)
    _publish(store_dir, 3, 2, "n3", 0xC3)
    store = Store(store_dir)
    h0 = store.open(0)
    h1 = store.open(1)
    try:
        report = store.reclaim()
        assert report["head"] == 3
        assert report["removed"] == [2]
        assert report["pinned"] == [0, 1]
        assert store.head() == 3
        store.load(0)
        store.load(1)
        with pytest.raises(StoreError):
            store.load(2)
        assert store.check()["ok"]
    finally:
        h0.close()
        h1.close()
    report = store.reclaim()
    assert report["removed"] == [0, 1]
    with pytest.raises(StoreError):
        store.load(0)
    head_snap, _, _ = store.load_head()
    assert head_snap.generation == 3
    assert store.check()["ok"]


def test_reclaim_keep_recent_tail(tmp_path):
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    _publish(store_dir, 2, 1, "n2", 0xC2)
    _publish(store_dir, 3, 2, "n3", 0xC3)
    report = Store(store_dir).reclaim(keep_recent=1)
    assert report["removed"] == [0, 1]
    Store(store_dir).load(2)  # newest unpinned tail retained
    assert Store(store_dir).check()["ok"]


def test_reclaim_preserves_newer_than_head(tmp_path):
    """A valid generation past HEAD (crash between the generation
    rename and the HEAD rename) is never reclaimed: it stays for an
    operator instead of being silently deleted."""
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    with open(os.path.join(store_dir, "HEAD"), "w") as fh:
        fh.write("0")
    report = Store(store_dir).reclaim()
    assert report["removed"] == []
    Store(store_dir).load(1)
    codes = {i["code"] for i in Store(store_dir).check()["issues"]}
    assert codes == {"NEWER_THAN_HEAD"}


def test_reclaim_empty_store(tmp_path):
    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir)
    assert Store(store_dir).reclaim() == {
        "head": None,
        "removed": [],
        "reaped_stale": [],
        "pinned": [],
    }


def test_manual_delete_still_reports_gap(tmp_path):
    """Only `reclaim` excuses a missing generation: an operator (or
    bit-rot) deleting a file still fails closed with GEN_GAP."""
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    os.unlink(os.path.join(store_dir, "gen-000000.json"))
    codes = {i["code"] for i in Store(store_dir).check()["issues"]}
    assert "GEN_GAP" in codes


def test_check_ok_with_pins_and_repair_preserves_them(tmp_path):
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    store = Store(store_dir)
    with store.open_head():
        report = store.check()
        assert report["ok"]
        assert store.repair() == []
        assert len(_pins_on_disk(store_dir)) == 1
        snap, _, _ = store.load(0)
        assert snap.generation == 0


# -- stale pins -----------------------------------------------------------


@requires_fork
def test_orphan_pin_reaped_unblocks_reclamation(tmp_path):
    """A reader that dies without closing (real death via fork +
    `os._exit`) leaves an orphan pin: it must not count as live,
    and `reclaim` must reap it and free the generation."""
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    store = Store(store_dir)
    pid = os.fork()
    if pid == 0:  # child: pin gen 0, die without closing.
        try:
            Store(store_dir).pin(0)
        finally:
            os._exit(0)
    _, status = os.waitpid(pid, 0)
    assert status == 0
    assert len(_pins_on_disk(store_dir)) == 1
    assert store.list_pins() == []  # dead pid: not live (non-mutating).
    assert store.pinned_generations() == set()
    _publish(store_dir, 1, 0, "n1", 0xC1)
    report = store.reclaim()
    assert report["removed"] == [0]
    assert len(report["reaped_stale"]) == 1
    assert _pins_on_disk(store_dir) == []
    assert store.check()["ok"]


# -- readers across real-death crash / recovery ---------------------------

MVCC_PROBE = (
    "import os, sys; "
    "sys.path.insert(0, os.environ['MNCS_PROBE_RUNNER']); "
    "from mncs_index.indexer import finalize; "
    "from mncs_index.model import DocRecord, TermRecord; "
    "from mncs_index.store import Store; "
    "store = Store(os.environ['MNCS_PROBE_STORE']); "
    "docs = [DocRecord(kind=1, path='a.md', digest=0xA1, size=3, words=1, lines=1),"
    " DocRecord(kind=1, path='n1.md', digest=0xC1, size=3, words=1, lines=1)]; "
    "terms = [TermRecord(path='a.md', token='alpha', tid=0xA1, seq=0)]; "
    "snap, canon = finalize(None, 'seed-corpus', 1, docs, terms,"
    " with_mncs_digest=False); "
    "store.publish(snap, canon, {d.path: 1 for d in docs}, {}, 0)"
)


def test_pinned_reader_survives_crash_and_recovery(tmp_path):
    """A handle pinned before a mid-commit process death stays
    valid; post-crash state is old-complete, and recovery
    (`check`/`repair`) never touches pinned generations."""
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    store = Store(store_dir)
    with store.open_head() as h:
        assert h.generation == 0
        env = dict(os.environ)
        env["MNCS_PROBE_RUNNER"] = RUNNER
        env["MNCS_PROBE_STORE"] = store_dir
        env["MNCS_INDEX_CRASH_AT"] = "after-gen-rename"
        proc = subprocess.run(
            [sys.executable, "-c", MVCC_PROBE],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert proc.returncode == CRASH_EXIT_CODE  # real death, not an error.
        report = store.check()
        assert {i["code"] for i in report["issues"]} == {"NEWER_THAN_HEAD"}
        assert store.repair() == []  # no staging files in this window.
        # Pinned pre-crash handle is intact; fresh readers see old-complete.
        assert h.snap.index_hash == store.load(0)[0].index_hash
        with store.open_head() as fresh:
            assert fresh.generation == 0
            assert fresh.snap.index_hash == h.snap.index_hash
    # The orphaned newer generation republishes cleanly onto HEAD=0.
    _publish(store_dir, 1, 0, "n1", 0xC1)
    assert store.head() == 1
    assert store.check()["ok"]


# -- multiprocess races ---------------------------------------------------

# Reader/writer process counts (asserted structurally below):
# - test_single_writer_many_readers_processes: 1 writer + 4 readers.
# - test_multi_writer_multi_reader_processes: 3 writers + 2 readers.


def _reader_loop(store_dir, result_path, deadline, max_iters=400):
    """Pin/load/validate loop for a forked reader child. Always
    writes its result file; raises on any torn observation."""
    seen: dict = {}
    iters = 0
    while time.monotonic() < deadline and iters < max_iters:
        with Store(store_dir).open_head() as h:
            snap = h.snap
            key = (snap.generation, snap.index_hash, len(snap.docs))
            seen.setdefault(snap.generation, key)
            assert seen[snap.generation] == key, (
                f"generation {snap.generation} observed inconsistently"
            )
        iters += 1
    with open(result_path, "w") as fh:
        json.dump({"iters": iters, "seen": {str(k): list(v) for k, v in seen.items()}}, fh)


def _writer_once(store_dir, generation, base, tag, tag_digest, result_path, delay):
    time.sleep(delay)
    snap, canon = _candidate(generation, tag, tag_digest)
    docs = [_doc("a.md", 0xA1), _doc(f"{tag}.md", tag_digest)]
    try:
        Store(store_dir).publish(
            snap, canon, {d.path: generation for d in docs}, {}, base
        )
        outcome = {"outcome": "won", "hash": snap.index_hash}
    except StaleGenerationError:
        outcome = {"outcome": "stale"}
    with open(result_path, "w") as fh:
        json.dump(outcome, fh)


def _wait_clean(pid):
    _, status = os.waitpid(pid, 0)
    assert status == 0, f"child {pid} exited with status {status}"


@requires_fork
def test_single_writer_many_readers_processes(tmp_path):
    """1 writer (parent) publishes gen 1..2 while 4 ACTUAL reader
    processes pin/load/validate in a loop: every observation is one
    complete, hash-validated generation; per-generation content is
    consistent; no reader ever fails."""
    store_dir = str(tmp_path / "store")
    resdir = str(tmp_path / "results")
    os.makedirs(resdir)
    _seeded_store(store_dir)
    n_readers = 4
    deadline = time.monotonic() + 2.5
    pids = []
    for i in range(n_readers):
        pid = os.fork()
        if pid == 0:  # child reader.
            try:
                _reader_loop(store_dir, os.path.join(resdir, f"reader-{i}.json"), deadline)
            except Exception as exc:  # noqa: BLE001 — report, never hang.
                with open(os.path.join(resdir, f"reader-{i}.json"), "w") as fh:
                    json.dump({"error": repr(exc)}, fh)
                os._exit(1)
            os._exit(0)
        pids.append(pid)
    try:
        time.sleep(0.3)
        _publish(store_dir, 1, 0, "n1", 0xC1)
        time.sleep(0.3)
        _publish(store_dir, 2, 1, "n2", 0xC2)
    finally:
        for pid in pids:
            _wait_clean(pid)
    assert Store(store_dir).head() == 2
    total_iters = 0
    by_gen: dict = {}
    for i in range(n_readers):
        with open(os.path.join(resdir, f"reader-{i}.json")) as fh:
            res = json.load(fh)
        assert "error" not in res, res
        assert res["iters"] > 0, f"reader {i} observed nothing"
        total_iters += res["iters"]
        for gen, key in res["seen"].items():
            by_gen.setdefault(gen, set()).add(tuple(key))
    assert total_iters > 0
    assert set(by_gen) <= {"0", "1", "2"}
    for gen, keys in by_gen.items():
        assert len(keys) == 1, f"generation {gen} inconsistent: {keys}"
    totals = {g: k[2] for g, (k,) in ((g, list(v)) for g, v in by_gen.items())}
    assert totals.get("0", 2) == 2
    if "1" in totals:
        assert totals["1"] == 2
    if "2" in totals:
        assert totals["2"] == 2
    # Every reader closed its handles: no live pins linger.
    assert Store(store_dir).pinned_generations() == set()
    report = Store(store_dir).reclaim()
    assert report["removed"] == [0, 1]
    assert Store(store_dir).check()["ok"]


@requires_fork
def test_multi_writer_multi_reader_processes(tmp_path):
    """3 writers race one CAS slot (gen 1 from base 0) while 2
    readers pin/load concurrently: exactly one writer wins, HEAD
    holds the winner's hash-validated content, readers see only
    complete generations."""
    store_dir = str(tmp_path / "store")
    resdir = str(tmp_path / "results")
    os.makedirs(resdir)
    _seeded_store(store_dir)
    tags = [("wa", 0xAA), ("wb", 0xBB), ("wc", 0xCC)]
    deadline = time.monotonic() + 2.5
    pids = []
    for i in range(2):  # reader children first.
        pid = os.fork()
        if pid == 0:
            try:
                _reader_loop(store_dir, os.path.join(resdir, f"reader-{i}.json"), deadline)
            except Exception as exc:  # noqa: BLE001 — report, never hang.
                with open(os.path.join(resdir, f"reader-{i}.json"), "w") as fh:
                    json.dump({"error": repr(exc)}, fh)
                os._exit(1)
            os._exit(0)
        pids.append(pid)
    for i, (tag, digest) in enumerate(tags):
        pid = os.fork()
        if pid == 0:  # writer child.
            try:
                _writer_once(
                    store_dir, 1, 0, tag, digest,
                    os.path.join(resdir, f"writer-{i}.json"), 0.2 + 0.05 * i,
                )
            except Exception as exc:  # noqa: BLE001 — report, never hang.
                with open(os.path.join(resdir, f"writer-{i}.json"), "w") as fh:
                    json.dump({"outcome": "error", "detail": repr(exc)}, fh)
                os._exit(1)
            os._exit(0)
        pids.append(pid)
    try:
        for pid in pids:
            _wait_clean(pid)
    finally:
        for pid in pids:
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
    outcomes = []
    for i in range(3):
        with open(os.path.join(resdir, f"writer-{i}.json")) as fh:
            outcomes.append(json.load(fh))
    assert all(o["outcome"] in ("won", "stale") for o in outcomes), outcomes
    winners = [o for o in outcomes if o["outcome"] == "won"]
    assert len(winners) == 1
    assert sum(1 for o in outcomes if o["outcome"] == "stale") == 2
    store = Store(store_dir)
    assert store.head() == 1
    head_snap, _, _ = store.load_head()
    assert head_snap.index_hash == winners[0]["hash"]
    for i in range(2):
        with open(os.path.join(resdir, f"reader-{i}.json")) as fh:
            res = json.load(fh)
        assert "error" not in res, res
        assert res["iters"] > 0
        assert set(res["seen"]) <= {"0", "1"}
    assert store.check()["ok"]


def test_concurrent_thread_readers_during_publish(tmp_path):
    """Threads pinning/loading while the writer advances: no handle
    ever raises, and every bound snapshot is complete."""
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    stop = threading.Event()
    errors: list = []
    seen: dict = {}
    lock = threading.Lock()

    def reader():
        try:
            while not stop.is_set():
                with Store(store_dir).open_head() as h:
                    key = (h.snap.generation, h.snap.index_hash, len(h.snap.docs))
                    with lock:
                        prev = seen.setdefault(h.generation, key)
                        assert prev == key
        except Exception as exc:  # noqa: BLE001 — collected, asserted below.
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    try:
        time.sleep(0.1)
        _publish(store_dir, 1, 0, "n1", 0xC1)
        _publish(store_dir, 2, 1, "n2", 0xC2)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=60)
    assert all(not t.is_alive() for t in threads)
    assert errors == []
    assert seen, "readers observed nothing"
    assert set(seen) <= {0, 1, 2}


# -- CLI ---------------------------------------------------------------


def test_cli_reclaim(tmp_path, capsys):
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    _publish(store_dir, 2, 1, "n2", 0xC2)
    assert cli_main(["reclaim", "--store", store_dir]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["removed"] == [0, 1] and out["head"] == 2
    assert Store(store_dir).check()["ok"]
    assert cli_main(["reclaim", "--store", store_dir, "--keep-recent", "1"]) == 0
