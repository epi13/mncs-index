"""Deterministic compaction (RFC 0010): in-place generational GC.

`Store.compact(schedule)` reclaims schedule-selected obsolete
generations plus orphan staging tmps and superseded sidecars, with
query-before == query-after (and identical `index_hash`) on HEAD,
storage metrics (counts, bytes, amplification, reclaimed bytes,
duration), multiple deterministic schedules, safety under live
readers (pinned generations are never deleted), and interruption
safety (record-first `.reclaimed` updates: a crash mid-run resumes
to the same end state, and `check()` never reports `GEN_GAP` for
compaction-deleted files). No second database, no copy phase.
Kernel-free (`finalize(..., with_mncs_digest=False)`); queries run
through a host-substring stub kernel (same stub before/after, so
equivalence is meaningful).
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
from mncs_index.query import QueryEngine
from mncs_index.store import (
    COMPACT_CRASH_POINTS,
    COMPACT_SCHEDULES,
    CRASH_EXIT_CODE,
    Store,
    StoreError,
)

RUNNER = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/runner"

requires_fork = pytest.mark.skipif(
    not hasattr(os, "fork"), reason="requires os.fork (POSIX)"
)


class _StubKernels:
    """Host-substring stand-in for `contains8` (short-term path).

    The same stub queries HEAD before and after compaction, so any
    divergence would be compaction-caused, not kernel-caused. The
    long-term host path in `QueryEngine` needs no kernel at all.
    """

    def contains8(self, hay: bytes, needle: bytes) -> bool:
        return needle in hay


def _doc(path, digest, size=3, words=1, lines=1, kind=1):
    return DocRecord(
        kind=kind, path=path, digest=digest, size=size, words=words, lines=lines
    )


def _terms():
    return [
        TermRecord(path="a.md", token="alpha", tid=0xA1, seq=0),
        TermRecord(path="b.md", token="bravo-long-token", tid=0xB2, seq=1),
    ]


def _seeded_store(store_dir, durability="L2"):
    store = Store(store_dir, durability=durability)
    docs = [_doc("a.md", 0xA1), _doc("b.md", 0xB2)]
    snap, canon = finalize(None, "seed-corpus", 0, docs, _terms(), with_mncs_digest=False)
    store.publish(snap, canon, {d.path: 0 for d in docs}, {}, None)
    return store, snap


def _candidate(generation, tag, tag_digest):
    docs = [_doc("a.md", 0xA1), _doc("b.md", 0xB2), _doc(f"{tag}.md", tag_digest)]
    return finalize(
        None, "seed-corpus", generation, docs, _terms(), with_mncs_digest=False
    )


def _publish(store_dir, generation, base, tag, tag_digest, durability="L2"):
    snap, canon = _candidate(generation, tag, tag_digest)
    docs = [_doc("a.md", 0xA1), _doc("b.md", 0xB2), _doc(f"{tag}.md", tag_digest)]
    Store(store_dir, durability=durability).publish(
        snap, canon, {d.path: generation for d in docs}, {}, base
    )
    return snap


def _six_gen_store(store_dir):
    """Generations 0..5, HEAD=5; returns the store."""
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    _publish(store_dir, 2, 1, "n2", 0xC2)
    _publish(store_dir, 3, 2, "n3", 0xC3)
    _publish(store_dir, 4, 3, "n4", 0xC4)
    _publish(store_dir, 5, 4, "n5", 0xC5)
    return Store(store_dir)


def _query_fingerprint(store_dir):
    """Full observable query state of HEAD: hash + every query class."""
    store = Store(store_dir)
    snap, established, _ = store.load_head()
    eng = QueryEngine(snap, _StubKernels(), workers=2, established=established)
    fp = {"index_hash": snap.index_hash, "generation": snap.generation}
    results = {
        "by_path_hit": eng.by_path("a.md"),
        "by_path_miss": eng.by_path("missing.md"),
        "by_kind": eng.by_kind(1),
        "by_digest": eng.by_digest_query(0xA1),
        "term_short": eng.term_substring("alpha"),
        "term_long": eng.term_substring("bravo-long-token"),
        "term_short_limit": eng.term_substring("a", limit=1),
    }
    for key, res in results.items():
        fp[key] = (
            res.total,
            res.limited,
            [
                (d.sort_key(), d.kind, d.path, d.digest, d.size, d.words, d.lines)
                for d in res.records
            ],
        )
    fp["provenance_hit"] = eng.provenance("a.md")
    fp["provenance_miss"] = eng.provenance("missing.md")
    return fp


def _gen_sizes(store_dir):
    sizes = {}
    for name in os.listdir(store_dir):
        if name.startswith("gen-") and name.endswith(".json"):
            gen = int(name[4:10])
            sizes[gen] = os.path.getsize(os.path.join(store_dir, name))
    return sizes


# -- schedules ------------------------------------------------------------


def test_schedules_select_victims(tmp_path):
    store_dir = str(tmp_path / "store")
    report = _six_gen_store(store_dir).compact("prune")
    assert report["removed_generations"] == [0, 1, 2, 3, 4]
    assert report["retained"] == []
    assert report["head"] == 5
    assert Store(store_dir).check()["ok"]
    Store(store_dir).load(5)


def test_schedule_keep_recent_tail(tmp_path):
    store_dir = str(tmp_path / "store")
    report = _six_gen_store(store_dir).compact("keep-recent:2")
    assert report["removed_generations"] == [0, 1, 2]
    assert report["retained"] == [3, 4]
    Store(store_dir).load(3)
    Store(store_dir).load(4)
    with pytest.raises(StoreError):
        Store(store_dir).load(0)
    assert Store(store_dir).check()["ok"]


def test_schedule_checkpoint(tmp_path):
    store_dir = str(tmp_path / "store")
    report = _six_gen_store(store_dir).compact("checkpoint:2")
    assert report["removed_generations"] == [1, 3]
    assert report["retained"] == [0, 2, 4]
    for gen in (0, 2, 4, 5):
        Store(store_dir).load(gen)
    assert Store(store_dir).check()["ok"]


def test_compact_rejects_bad_schedules(tmp_path):
    store_dir = str(tmp_path / "store")
    _six_gen_store(store_dir)
    bad = [
        "bogus",
        "PRUNE",
        "",
        "keep-recent",
        "keep-recent:",
        "keep-recent:-1",
        "keep-recent:x",
        "keep-recent:1:2",
        "checkpoint",
        "checkpoint:0",
        "checkpoint:-2",
        "checkpoint:x",
        None,
        0,
        True,
        ["prune"],
    ]
    for schedule in bad:
        with pytest.raises(StoreError):
            Store(store_dir).compact(schedule)
        with pytest.raises(StoreError):
            Store(store_dir).compact(schedule, dry_run=True)
    # Nothing moved: all generations intact, no manifest, no record.
    assert sorted(_gen_sizes(store_dir)) == [0, 1, 2, 3, 4, 5]
    assert not os.path.exists(os.path.join(store_dir, ".compact.json"))
    assert not os.path.exists(os.path.join(store_dir, ".reclaimed"))
    with pytest.raises(StoreError):
        Store(store_dir).compact("prune", dry_run="yes")


def test_compact_empty_store(tmp_path):
    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir)
    report = Store(store_dir).compact("prune")
    assert report["head"] is None
    assert report["removed_generations"] == []
    assert report["generations_before"] == report["generations_after"] == 0
    assert report["bytes_before"] == report["bytes_after"] == 0
    assert report["reclaimed_bytes"] == 0
    assert report["amplification_before"] is None
    assert report["amplification_after"] is None
    assert report["manifest"] is None
    assert report["duration_s"] >= 0
    assert not os.path.exists(os.path.join(store_dir, ".compact.json"))


def test_compact_dry_run_changes_nothing(tmp_path):
    store_dir = str(tmp_path / "store")
    _six_gen_store(store_dir)
    before = _gen_sizes(store_dir)
    report = Store(store_dir).compact("checkpoint:2", dry_run=True)
    assert report["dry_run"] is True
    assert report["removed_generations"] == [1, 3]
    assert report["retained"] == [0, 2, 4]
    assert report["manifest"] is None
    assert _gen_sizes(store_dir) == before
    assert not os.path.exists(os.path.join(store_dir, ".compact.json"))
    assert not os.path.exists(os.path.join(store_dir, ".reclaimed"))
    again = Store(store_dir).compact("checkpoint:2", dry_run=True)
    assert {k: v for k, v in again.items() if k != "duration_s"} == {
        k: v for k, v in report.items() if k != "duration_s"
    }
    # The real run then removes exactly the previewed victims.
    real = Store(store_dir).compact("checkpoint:2")
    assert real["removed_generations"] == report["removed_generations"]
    assert real["bytes_after"] == report["bytes_after"]


# -- query-before == query-after + hash equivalence ------------------------


def test_query_and_hash_equivalence_prune(tmp_path):
    store_dir = str(tmp_path / "store")
    _six_gen_store(store_dir)
    before = _query_fingerprint(store_dir)
    report = Store(store_dir).compact("prune")
    assert report["removed_generations"] == [0, 1, 2, 3, 4]
    assert _query_fingerprint(store_dir) == before


def test_query_and_hash_equivalence_schedules(tmp_path):
    for schedule in ("keep-recent:1", "checkpoint:3", "keep-recent:0"):
        store_dir = str(tmp_path / schedule.replace(":", "-"))
        _six_gen_store(store_dir)
        before = _query_fingerprint(store_dir)
        Store(store_dir).compact(schedule)
        assert _query_fingerprint(store_dir) == before
        assert Store(store_dir).check()["ok"]


# -- storage metrics ------------------------------------------------------


def test_storage_metrics_are_truthful(tmp_path):
    store_dir = str(tmp_path / "store")
    _six_gen_store(store_dir)
    du = _gen_sizes(store_dir)
    total = sum(du.values())
    report = Store(store_dir).compact("prune")
    assert report["generations_before"] == 6
    assert report["generations_after"] == 1
    assert report["bytes_before"] == total
    assert report["bytes_after"] == du[5]
    assert report["reclaimed_bytes"] == total - du[5]
    assert report["tmp_bytes_reclaimed"] == 0
    assert report["amplification_before"] == pytest.approx(total / du[5])
    assert report["amplification_after"] == pytest.approx(1.0)
    assert isinstance(report["duration_s"], float) and report["duration_s"] >= 0
    # The manifest on disk is the report itself (JSON round-trip exact).
    with open(os.path.join(store_dir, ".compact.json")) as fh:
        assert json.load(fh) == report


def test_metrics_keep_recent_and_checkpoint(tmp_path):
    store_dir = str(tmp_path / "store")
    _six_gen_store(store_dir)
    du = _gen_sizes(store_dir)
    report = Store(store_dir).compact("keep-recent:2")
    assert report["bytes_after"] == du[3] + du[4] + du[5]
    assert report["reclaimed_bytes"] == report["bytes_before"] - report["bytes_after"]
    assert report["amplification_after"] == pytest.approx(
        report["bytes_after"] / du[5]
    )
    assert report["amplification_after"] < report["amplification_before"]


# -- pinned generations are never deleted ---------------------------------


def test_compact_never_deletes_pinned(tmp_path):
    store_dir = str(tmp_path / "store")
    _six_gen_store(store_dir)
    before = _query_fingerprint(store_dir)
    store = Store(store_dir)
    with store.open(1) as h1, store.open(3) as h3:
        hash1 = h1.snap.index_hash
        report = store.compact("prune")
        assert report["pinned"] == [1, 3]
        assert report["removed_generations"] == [0, 2, 4]
        assert report["retained"] == [1, 3]
        # Pinned handles stay valid; HEAD queries unchanged.
        assert store.load(1)[0].index_hash == hash1
        assert store.load(3)[0].index_hash == h3.snap.index_hash
        assert _query_fingerprint(store_dir) == before
        assert store.check()["ok"]
    report = store.compact("prune")
    assert report["removed_generations"] == [1, 3]
    assert _query_fingerprint(store_dir) == before
    assert store.check()["ok"]


def test_compact_checkpoint_respects_pins(tmp_path):
    store_dir = str(tmp_path / "store")
    _six_gen_store(store_dir)
    store = Store(store_dir)
    with store.open(1):
        report = store.compact("checkpoint:2")
        # Gen 1 is a checkpoint victim but pinned: it survives.
        assert report["removed_generations"] == [3]
        assert 1 in report["retained"]
        store.load(1)


@requires_fork
def test_compact_under_live_reader_process(tmp_path):
    """A forked child holds gen 2 pinned while the parent compacts:
    the pinned generation survives; after the child exits, a second
    compaction reclaims it. File rendezvous, no timing guesses."""
    store_dir = str(tmp_path / "store")
    resdir = str(tmp_path / "results")
    os.makedirs(resdir)
    _six_gen_store(store_dir)
    before = _query_fingerprint(store_dir)
    ready = os.path.join(resdir, "ready")
    done = os.path.join(resdir, "done")
    abort = os.path.join(resdir, "abort")
    result = os.path.join(resdir, "child.json")
    pid = os.fork()
    if pid == 0:  # child reader: pin gen 2 explicitly, hold it.
        try:
            with Store(store_dir).open(2) as h:
                pinned_hash = h.snap.index_hash
                with open(ready, "w") as fh:
                    fh.write("ready")
                deadline = time.monotonic() + 60
                while (
                    not os.path.exists(done)
                    and not os.path.exists(abort)
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)
                assert os.path.exists(done), "parent never compacted"
                assert h.snap.index_hash == pinned_hash
                assert Store(store_dir).load(2)[0].index_hash == pinned_hash
                with open(result, "w") as fh:
                    json.dump({"ok": True, "hash": pinned_hash}, fh)
        except Exception as exc:  # noqa: BLE001 — report, never hang.
            with open(result, "w") as fh:
                json.dump({"ok": False, "error": repr(exc)}, fh)
            os._exit(1)
        os._exit(0)
    parent_failed = True
    try:
        deadline = time.monotonic() + 60
        while not os.path.exists(ready) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert os.path.exists(ready), "child never pinned gen 2"
        report = Store(store_dir).compact("prune")
        assert 2 in report["retained"] and 2 in report["pinned"]
        assert report["removed_generations"] == [0, 1, 3, 4]
        assert _query_fingerprint(store_dir) == before
        with open(done, "w") as fh:
            fh.write("done")
        parent_failed = False
    finally:
        if parent_failed:
            with open(abort, "w") as fh:
                fh.write("abort")
        _, status = os.waitpid(pid, 0)
    assert status == 0, f"reader child exited with status {status}"
    with open(result) as fh:
        res = json.load(fh)
    assert res.get("ok"), res
    report = Store(store_dir).compact("prune")
    assert report["removed_generations"] == [2]
    assert _query_fingerprint(store_dir) == before
    assert Store(store_dir).check()["ok"]


def test_compact_under_threaded_readers(tmp_path):
    """Threads pinning/loading random below-HEAD generations while
    the writer compacts in a loop: no handle ever goes torn, and
    the store converges healthy. `StoreError` on `open` is the
    documented benign race (generation reclaimed first); anything
    else is a failure."""
    import random

    store_dir = str(tmp_path / "store")
    _six_gen_store(store_dir)
    before = _query_fingerprint(store_dir)
    stop = threading.Event()
    errors: list = []
    lock = threading.Lock()

    def reader(seed):
        rng = random.Random(seed)
        try:
            while not stop.is_set():
                gen = rng.randrange(0, 5)
                try:
                    with Store(store_dir).open(gen) as h:
                        key = (h.snap.generation, h.snap.index_hash)
                        snap, _, _ = Store(store_dir).load(h.generation)
                        assert snap.index_hash == key[1]
                except StoreError:
                    continue  # reclaimed between pin and load: retry.
        except Exception as exc:  # noqa: BLE001 — collected, asserted below.
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=reader, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    try:
        for _ in range(10):
            Store(store_dir).compact("prune")
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=60)
    assert all(not t.is_alive() for t in threads)
    assert errors == []
    assert _query_fingerprint(store_dir) == before
    assert Store(store_dir).check()["ok"]


# -- interrupted compaction safety -----------------------------------------

COMPACT_PROBE = (
    "import os, sys; "
    "sys.path.insert(0, os.environ['MNCS_PROBE_RUNNER']); "
    "from mncs_index.store import Store; "
    "Store(os.environ['MNCS_PROBE_STORE'],"
    " crash_at={os.environ['MNCS_PROBE_POINT']}).compact('prune')"
)


def _run_compact_crash_probe(store_dir, point):
    env = dict(os.environ)
    env["MNCS_PROBE_RUNNER"] = RUNNER
    env["MNCS_PROBE_STORE"] = store_dir
    env["MNCS_PROBE_POINT"] = point
    return subprocess.run(
        [sys.executable, "-c", COMPACT_PROBE],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _four_gen_store(store_dir):
    _seeded_store(store_dir)
    _publish(store_dir, 1, 0, "n1", 0xC1)
    _publish(store_dir, 2, 1, "n2", 0xC2)
    _publish(store_dir, 3, 2, "n3", 0xC3)
    return Store(store_dir)


def test_interrupted_compact_delete_is_safe(tmp_path):
    """Crash after the first recorded deletion: `check()` stays
    quiet (no GEN_GAP — the deletion was recorded first), HEAD
    queries are unchanged, and a resumed compaction converges to
    exactly the uninterrupted end state."""
    assert "after-compact-delete" in COMPACT_CRASH_POINTS
    store_dir = str(tmp_path / "store")
    _four_gen_store(store_dir)
    before = _query_fingerprint(store_dir)
    proc = _run_compact_crash_probe(store_dir, "after-compact-delete")
    assert proc.returncode == CRASH_EXIT_CODE  # real death, not an error.
    store = Store(store_dir)
    assert store.check()["ok"]
    assert store.head() == 3
    assert _query_fingerprint(store_dir) == before
    # Exactly the first victim went; it is recorded-reclaimed.
    with pytest.raises(StoreError):
        store.load(0)
    store.load(1)
    store.load(2)
    assert store.check()["reclaimed"] == [0]
    # Resume converges: the rest goes, queries still identical.
    report = store.compact("prune")
    assert report["removed_generations"] == [1, 2]
    assert _query_fingerprint(store_dir) == before
    assert store.check()["ok"]
    assert store.check()["reclaimed"] == [0, 1, 2]


def test_interrupted_compact_manifest_tmp_is_safe(tmp_path):
    """Crash between manifest staging and rename: all deletions are
    recorded, `.compact.tmp` reports ORPHAN_TMP, queries unchanged,
    and resume removes the leftover tmp and writes the manifest."""
    assert "after-compact-manifest-write" in COMPACT_CRASH_POINTS
    store_dir = str(tmp_path / "store")
    _four_gen_store(store_dir)
    before = _query_fingerprint(store_dir)
    proc = _run_compact_crash_probe(store_dir, "after-compact-manifest-write")
    assert proc.returncode == CRASH_EXIT_CODE
    store = Store(store_dir)
    assert {i["code"] for i in store.check()["issues"]} == {"ORPHAN_TMP"}
    assert not os.path.exists(os.path.join(store_dir, ".compact.json"))
    assert _query_fingerprint(store_dir) == before
    report = store.compact("prune")
    assert report["removed_generations"] == []
    assert report["removed_tmps"] == [".compact.tmp"]
    assert _query_fingerprint(store_dir) == before
    assert store.check()["ok"]
    with open(os.path.join(store_dir, ".compact.json")) as fh:
        assert json.load(fh)["removed_tmps"] == [".compact.tmp"]


def test_interrupted_compact_before_manifest(tmp_path):
    """Crash after all deletions but before manifest staging: no
    manifest, no leftover tmp, `check()` quiet, resume writes it."""
    assert "before-compact-manifest" in COMPACT_CRASH_POINTS
    store_dir = str(tmp_path / "store")
    _four_gen_store(store_dir)
    before = _query_fingerprint(store_dir)
    proc = _run_compact_crash_probe(store_dir, "before-compact-manifest")
    assert proc.returncode == CRASH_EXIT_CODE
    store = Store(store_dir)
    assert store.check()["ok"]
    assert not os.path.exists(os.path.join(store_dir, ".compact.json"))
    assert _query_fingerprint(store_dir) == before
    report = store.compact("prune")
    assert report["removed_generations"] == []
    assert os.path.exists(os.path.join(store_dir, ".compact.json"))
    assert _query_fingerprint(store_dir) == before
    assert store.check()["ok"]


# -- orphan tmps / sidecars -------------------------------------------------


@requires_fork
def test_compact_triages_staging_tmps(tmp_path):
    """`.HEAD.tmp` goes (orphan under the lock by construction); a
    dead writer's `.gen-*.tmp` goes (real fork death, no pid
    guessing); a LIVE writer's staging is skipped and kept; an
    unattributable name is left for the operator `repair`."""
    store_dir = str(tmp_path / "store")
    _four_gen_store(store_dir)
    before = _query_fingerprint(store_dir)
    # Dead writer's staging: forked child writes it, then dies.
    pid = os.fork()
    if pid == 0:  # child stages, then dies without publishing.
        try:
            name = f".gen-000007.{os.getpid()}.1.tmp"
            with open(os.path.join(store_dir, name), "w") as fh:
                fh.write('{"unfinished": true}\n')
                fh.flush()
                os.fsync(fh.fileno())
        finally:
            os._exit(0)
    _, status = os.waitpid(pid, 0)
    assert status == 0
    (dead_tmp_name,) = [
        name
        for name in os.listdir(store_dir)
        if name.startswith(".gen-000007.")
    ]
    # Live writer's staging: our own pid is definitionally alive.
    live_tmp = f".gen-000007.{os.getpid()}.0.tmp"
    with open(os.path.join(store_dir, live_tmp), "w") as fh:
        fh.write('{"inflight": true}\n')
    with open(os.path.join(store_dir, ".HEAD.tmp"), "w") as fh:
        fh.write("3")
    weird_tmp = ".gen-weird.tmp"
    with open(os.path.join(store_dir, weird_tmp), "w") as fh:
        fh.write("junk\n")
    report = Store(store_dir).compact("prune")
    assert dead_tmp_name in report["removed_tmps"]
    assert ".HEAD.tmp" in report["removed_tmps"]
    assert report["skipped_live_tmps"] == [live_tmp]
    assert report["skipped_unknown_tmps"] == [weird_tmp]
    # The live writer's staging survived byte-identical (its rename
    # would still succeed); the writer aborting cleans it up itself.
    with open(os.path.join(store_dir, live_tmp)) as fh:
        assert fh.read() == '{"inflight": true}\n'
    os.unlink(os.path.join(store_dir, live_tmp))
    assert _query_fingerprint(store_dir) == before
    # The two intentionally kept tmps are still staging as far as
    # `check` can tell (it has no pid rule — compaction does), so
    # they report ORPHAN_TMP until the writer finishes / repair runs.
    assert {i["code"] for i in Store(store_dir).check()["issues"]} == {
        "ORPHAN_TMP"
    }
    # The unattributable leftover is `repair`'s job, not compaction's.
    assert Store(store_dir).repair() == [weird_tmp]
    assert Store(store_dir).check()["ok"]


def test_preview_matches_real_with_dead_pin(tmp_path):
    """Dry-run victims equal the real run's removals with a dead pin.

    Dead pins never protect, so preview and real agree on victims;
    the preview additionally reports `would_reap_stale` (the real run
    reports it as `reaped_stale` after deleting).
    """
    store_dir = str(tmp_path / "store")
    _four_gen_store(store_dir)
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    pins = os.path.join(store_dir, ".pins")
    os.makedirs(pins, exist_ok=True)
    pin_name = f"gen-000001.{proc.pid}.{'c' * 32}.pin"
    with open(os.path.join(pins, pin_name), "w") as fh:
        fh.write("1\n%d\n0\n" % proc.pid)
    preview = Store(store_dir).compact("prune", dry_run=True)
    assert preview["would_reap_stale"] == [pin_name]
    assert preview["reaped_stale"] == []
    real = Store(store_dir).compact("prune")
    assert real["removed_generations"] == preview["removed_generations"]
    assert real["reaped_stale"] == [pin_name]
    assert Store(store_dir).check()["ok"]


def test_compact_preserves_newer_than_head(tmp_path):
    """A valid generation past HEAD (crash between the generation
    rename and the HEAD rename) is never compacted away: it stays
    for an operator; below-HEAD victims still go."""
    store_dir = str(tmp_path / "store")
    _four_gen_store(store_dir)
    before = _query_fingerprint(store_dir)
    with open(os.path.join(store_dir, "HEAD"), "w") as fh:
        fh.write("2")
    report = Store(store_dir).compact("prune")
    assert report["head"] == 2
    assert report["removed_generations"] == [0, 1]
    assert report["newer_than_head"] == [3]
    Store(store_dir).load(3)
    assert {i["code"] for i in Store(store_dir).check()["issues"]} == {
        "NEWER_THAN_HEAD"
    }
    with open(os.path.join(store_dir, "HEAD"), "w") as fh:
        fh.write("3")
    assert _query_fingerprint(store_dir) == before


@requires_fork
def test_compact_reaps_orphan_pin(tmp_path):
    """A reader that dies without closing leaves an orphan pin: the
    next compaction reaps it (reported) and frees the generation."""
    store_dir = str(tmp_path / "store")
    _seeded_store(store_dir)
    pid = os.fork()
    if pid == 0:  # child: pin gen 0, die without closing.
        try:
            Store(store_dir).pin(0)
        finally:
            os._exit(0)
    _, status = os.waitpid(pid, 0)
    assert status == 0
    _publish(store_dir, 1, 0, "n1", 0xC1)
    report = Store(store_dir).compact("prune")
    assert len(report["reaped_stale"]) == 1
    assert report["removed_generations"] == [0]
    assert Store(store_dir).check()["ok"]


# -- determinism / manifest / CLI --------------------------------------------


def test_compact_is_deterministic(tmp_path):
    """Two identically built stores compact to identical reports
    and manifests (modulo the measured `duration_s`)."""
    dirs = [str(tmp_path / "a"), str(tmp_path / "b")]
    for store_dir in dirs:
        _six_gen_store(store_dir)
    reports = [Store(d).compact("checkpoint:2") for d in dirs]
    assert [
        {k: v for k, v in r.items() if k != "duration_s"} for r in reports
    ][0] == [
        {k: v for k, v in r.items() if k != "duration_s"} for r in reports
    ][1]
    manifests = []
    for store_dir in dirs:
        with open(os.path.join(store_dir, ".compact.json")) as fh:
            manifests.append(json.load(fh))
    assert [
        {k: v for k, v in m.items() if k != "duration_s"} for m in manifests
    ][0] == [
        {k: v for k, v in m.items() if k != "duration_s"} for m in manifests
    ][1]


def test_compact_supersedes_manifest(tmp_path):
    """Each run replaces the previous manifest in place: exactly
    one manifest file exists and it describes the latest run."""
    store_dir = str(tmp_path / "store")
    _six_gen_store(store_dir)
    first = Store(store_dir).compact("keep-recent:2")
    assert first["removed_generations"] == [0, 1, 2]
    second = Store(store_dir).compact("prune")
    assert second["removed_generations"] == [3, 4]
    manifests = [n for n in os.listdir(store_dir) if n.startswith(".compact")]
    assert manifests == [".compact.json"]
    with open(os.path.join(store_dir, ".compact.json")) as fh:
        assert json.load(fh)["removed_generations"] == [3, 4]
    assert Store(store_dir).check()["ok"]


def test_compact_schedules_advertised():
    assert set(COMPACT_SCHEDULES) == {"prune", "keep-recent:N", "checkpoint:N"}


def test_cli_compact(tmp_path, capsys):
    store_dir = str(tmp_path / "store")
    _four_gen_store(store_dir)
    assert cli_main(["compact", "--store", store_dir]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["removed_generations"] == [0, 1, 2] and out["head"] == 3
    assert out["amplification_after"] == pytest.approx(1.0)
    assert Store(store_dir).check()["ok"]


def test_cli_compact_schedule_and_dry_run(tmp_path, capsys):
    store_dir = str(tmp_path / "store")
    _four_gen_store(store_dir)
    assert (
        cli_main(
            [
                "compact",
                "--store",
                store_dir,
                "--schedule",
                "keep-recent:1",
                "--dry-run",
            ]
        )
        == 0
    )
    out = json.loads(capsys.readouterr().out)
    assert out["dry_run"] is True
    assert out["removed_generations"] == [0, 1]
    assert sorted(_gen_sizes(store_dir)) == [0, 1, 2, 3]
    # A bad schedule fails closed (raises, never half-compacts).
    with pytest.raises(StoreError):
        cli_main(["compact", "--store", store_dir, "--schedule", "bogus"])
    assert sorted(_gen_sizes(store_dir)) == [0, 1, 2, 3]

