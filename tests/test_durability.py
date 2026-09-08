"""Durable generational store (RFC 0008): explicit durability levels,
fsync commit protocol, real-death crash injection, recovery/check.

`Store.publish` commits generation-first over two atomic renames; the
durability level (L0–L3) selects the fsync barriers. Crash injection
arms `os._exit` at named commit boundaries, exercised here in real
subprocesses — process death, not exceptions — so post-crash state
must always be old-complete or new-complete, never torn. Corruption
(missing/malformed HEAD, dangling HEAD, truncated/hash-invalid
generations, orphan temps, gaps) must fail closed: readers raise
`StoreError`, `check` reports, and `repair` only removes orphan temps
— recovery never guesses.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys

import pytest

import mncs_index.store as store_mod
from mncs_index.cli import main as cli_main
from mncs_index.indexer import finalize
from mncs_index.model import DocRecord, TermRecord
from mncs_index.store import (
    CRASH_EXIT_CODE,
    CRASH_POINTS,
    DURABILITY_LEVELS,
    Store,
    StoreError,
)

RUNNER = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/runner"

PROBE = (
    "import os, sys; "
    "sys.path.insert(0, os.environ['MNCS_PROBE_RUNNER']); "
    "from mncs_index.indexer import finalize; "
    "from mncs_index.model import DocRecord, TermRecord; "
    "from mncs_index.store import Store; "
    "store = Store(os.environ['MNCS_PROBE_STORE']); "
    "docs = [DocRecord(kind=1, path='a.md', digest=0xA1, size=3, words=1, lines=1),"
    " DocRecord(kind=1, path='crash.md', digest=0xC4, size=3, words=1, lines=1)]; "
    "terms = [TermRecord(path='a.md', token='alpha', tid=0xA1, seq=0)]; "
    "snap, canon = finalize(None, 'seed-corpus', 1, docs, terms,"
    " with_mncs_digest=False); "
    "store.publish(snap, canon, {d.path: 1 for d in docs}, {}, 0)"
)


def _doc(path, digest, size=3, words=1, lines=1, kind=1):
    return DocRecord(
        kind=kind, path=path, digest=digest, size=size, words=words, lines=lines
    )


def _seeded_store(store_dir, durability="L2"):
    """Publish a kernel-free generation-0 seed; returns (store, snapshot)."""
    store = Store(store_dir, durability=durability)
    docs = [_doc("a.md", 0xA1), _doc("b.md", 0xB2)]
    terms = [TermRecord(path="a.md", token="alpha", tid=0xA1, seq=0)]
    snap, canon = finalize(None, "seed-corpus", 0, docs, terms, with_mncs_digest=False)
    store.publish(snap, canon, {d.path: 0 for d in docs}, {}, None)
    return store, snap


def _crash_candidate():
    """The exact generation-1 candidate the subprocess probe publishes."""
    docs = [_doc("a.md", 0xA1), _doc("crash.md", 0xC4)]
    terms = [TermRecord(path="a.md", token="alpha", tid=0xA1, seq=0)]
    snap, canon = finalize(None, "seed-corpus", 1, docs, terms, with_mncs_digest=False)
    return snap, canon


def _codes(report):
    return {i["code"] for i in report["issues"]}


# -- durability levels ----------------------------------------------------


def test_durability_levels_explicit(tmp_path):
    assert DURABILITY_LEVELS == ("L0", "L1", "L2", "L3")
    assert Store(str(tmp_path / "dflt")).durability == "L2"
    with pytest.raises(StoreError):
        Store(str(tmp_path / "bad"), durability="L9")


def test_fsync_barriers_per_level(tmp_path, monkeypatch):
    """Each level issues its documented barrier set on a gen-0 publish."""
    file_syncs: list = []
    dir_syncs: list = []
    real_file = store_mod._fsync_file
    real_dir = store_mod._fsync_dir

    def count_file(fh):
        file_syncs.append(fh.fileno())
        return real_file(fh)

    def count_dir(path):
        dir_syncs.append(path)
        return real_dir(path)

    monkeypatch.setattr(store_mod, "_fsync_file", count_file)
    monkeypatch.setattr(store_mod, "_fsync_dir", count_dir)
    expect = {"L0": (0, 0), "L1": (2, 0), "L2": (2, 2), "L3": (2, 2)}
    for level, (n_file, n_dir) in expect.items():
        del file_syncs[:]
        del dir_syncs[:]
        store_dir = str(tmp_path / f"store-{level}")
        store, _ = _seeded_store(store_dir, durability=level)
        assert store.head() == 0
        assert len(file_syncs) == n_file, f"{level}: {file_syncs}"
        assert len(dir_syncs) == n_dir, f"{level}: {dir_syncs}"
        snap, _, _ = store.load_head()
        assert snap.generation == 0


def test_l3_ack_implies_verified_readable(tmp_path, monkeypatch):
    """L3 re-reads and hash-validates before acknowledging the publish."""
    store_dir = str(tmp_path / "store")
    store = Store(store_dir, durability="L3")
    docs = [_doc("a.md", 0xA1)]
    snap, canon = finalize(None, "seed-corpus", 0, docs, [], with_mncs_digest=False)
    assert store.publish(snap, canon, {"a.md": 0}, {}, None) == 0

    evil = _doc("b.md", 0xB2)
    docs2 = [_doc("a.md", 0xA1), evil]
    snap2, canon2 = finalize(
        None, "seed-corpus", 1, docs2, [], with_mncs_digest=False
    )
    bad = finalize(None, "seed-corpus", 0, docs, [], with_mncs_digest=False)[0]
    object.__setattr__(bad, "index_hash", "0" * 64)

    def fake_load(generation):
        assert generation == 1
        return bad, {}, {}

    monkeypatch.setattr(store, "load", fake_load)
    with pytest.raises(StoreError):
        store.publish(snap2, canon2, {d.path: 1 for d in docs2}, {}, 0)


def test_unknown_crash_point_rejected(tmp_path):
    with pytest.raises(StoreError):
        Store(str(tmp_path / "s"), crash_at={"mid-rename"})


# -- real-death crash injection -------------------------------------------


def _run_crash_probe(store_dir, point):
    env = dict(os.environ)
    env["MNCS_PROBE_RUNNER"] = RUNNER
    env["MNCS_PROBE_STORE"] = store_dir
    env["MNCS_INDEX_CRASH_AT"] = point
    return subprocess.run(
        [sys.executable, "-c", PROBE],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_crash_at_every_boundary_old_or_new_never_torn(tmp_path):
    """Kill the publisher with real process death at each of the 8
    commit boundaries; recovery must show old-complete or new-complete,
    and `check` must show only benign leftover codes, never corruption."""
    seed_dir = str(tmp_path / "seed")
    _, seed_snap = _seeded_store(seed_dir)
    old_hash = seed_snap.index_hash
    new_hash = _crash_candidate()[0].index_hash
    assert old_hash != new_hash

    # Expected post-crash HEAD side and check() issue codes per boundary.
    # Boundaries before the HEAD rename keep HEAD=0 (old); at/after the
    # HEAD rename HEAD=1 (new). The atomic renames make every other
    # outcome impossible.
    expect = {
        "after-gen-tmp-write": (0, {"ORPHAN_TMP"}),
        "after-gen-tmp-fsync": (0, {"ORPHAN_TMP"}),
        "after-gen-rename": (0, {"NEWER_THAN_HEAD"}),
        "after-gen-dirsync": (0, {"NEWER_THAN_HEAD"}),
        "after-head-tmp-write": (0, {"NEWER_THAN_HEAD", "ORPHAN_TMP"}),
        "after-head-tmp-fsync": (0, {"NEWER_THAN_HEAD", "ORPHAN_TMP"}),
        "after-head-rename": (1, set()),
        "after-final-dirsync": (1, set()),
    }
    assert set(expect) == set(CRASH_POINTS)

    for point, (want_head, want_codes) in expect.items():
        case = str(tmp_path / f"case-{point}")
        shutil.copytree(seed_dir, case)
        proc = _run_crash_probe(case, point)
        # Real death: the exact os._exit code, not an exception exit.
        assert proc.returncode == CRASH_EXIT_CODE, (
            f"{point}: returncode={proc.returncode} stderr={proc.stderr[-500:]}"
        )
        store = Store(case)
        report = store.check()
        assert _codes(report) == want_codes, (point, report["issues"])
        # Never torn, never missing: HEAD loads and is old or new.
        snap, _, _ = store.load_head()
        assert snap is not None, point
        assert store.head() == want_head, point
        assert snap.index_hash == (new_hash if want_head else old_hash), point


def test_crash_orphans_recovered_by_repair(tmp_path):
    """Staging leftovers from a mid-commit crash are GC'd by repair;
    the surviving HEAD generation is untouched and check goes green."""
    seed_dir = str(tmp_path / "seed")
    _, seed_snap = _seeded_store(seed_dir)
    case = str(tmp_path / "case")
    shutil.copytree(seed_dir, case)
    proc = _run_crash_probe(case, "after-head-tmp-fsync")
    assert proc.returncode == CRASH_EXIT_CODE
    store = Store(case)
    assert _codes(store.check()) == {"NEWER_THAN_HEAD", "ORPHAN_TMP"}
    removed = store.repair()
    assert sorted(removed) == [".HEAD.tmp"]
    report = store.check()
    assert _codes(report) == {"NEWER_THAN_HEAD"}
    snap, _, _ = store.load_head()
    assert snap.index_hash == seed_snap.index_hash


# -- corruption fails closed ----------------------------------------------


def _two_gen_store(store_dir):
    store, seed = _seeded_store(store_dir)
    snap, canon = _crash_candidate()
    store.publish(snap, canon, {d.path: 1 for d in snap.docs}, {}, 0)
    return store, seed, snap


def test_missing_head_with_data_fails_closed(tmp_path):
    store_dir = str(tmp_path / "store")
    _two_gen_store(store_dir)
    os.unlink(os.path.join(store_dir, "HEAD"))
    store = Store(store_dir)
    assert store.head() is None
    with pytest.raises(StoreError):
        store.load_head()
    report = store.check()
    assert not report["ok"]
    assert "HEAD_MISSING" in _codes(report)
    # Repair removes nothing and guesses no HEAD value.
    assert store.repair() == []
    assert "HEAD_MISSING" in _codes(store.check())


def test_malformed_head_fails_closed(tmp_path):
    store_dir = str(tmp_path / "store")
    _, _, head_snap = _two_gen_store(store_dir)
    with open(os.path.join(store_dir, "HEAD"), "w") as fh:
        fh.write("xyz\n")
    store = Store(store_dir)
    with pytest.raises(StoreError):
        store.head()
    with pytest.raises(StoreError):
        store.load_head()
    assert "HEAD_MALFORMED" in _codes(store.check())
    # A new publish on a corrupt HEAD fails closed, never as stale.
    docs = [_doc("a.md", 0xA1), _doc("z.md", 0x9A)]
    snap, canon = finalize(None, "seed-corpus", 2, docs, [], with_mncs_digest=False)
    with pytest.raises(StoreError):
        store.publish(snap, canon, {d.path: 2 for d in docs}, {}, 1)
    assert store.repair() == []
    assert "HEAD_MALFORMED" in _codes(store.check())
    assert head_snap is not None


def test_dangling_head_fails_closed(tmp_path):
    store_dir = str(tmp_path / "store")
    _two_gen_store(store_dir)
    with open(os.path.join(store_dir, "HEAD"), "w") as fh:
        fh.write("7")
    os.unlink(os.path.join(store_dir, "gen-000001.json"))
    store = Store(store_dir)
    assert store.head() == 7
    with pytest.raises(StoreError):
        store.load_head()
    with pytest.raises(StoreError):
        store.load(1)
    report = store.check()
    assert "HEAD_DANGLING" in _codes(report)
    # Repair never invents a HEAD value or deletes valid generations.
    assert store.repair() == []
    assert "HEAD_DANGLING" in _codes(store.check())
    snap0, _, _ = store.load(0)
    assert snap0.generation == 0


def test_truncated_generation_fails_closed(tmp_path):
    store_dir = str(tmp_path / "store")
    _two_gen_store(store_dir)
    path = os.path.join(store_dir, "gen-000001.json")
    size = os.path.getsize(path)
    with open(path, "r+b") as fh:
        fh.truncate(size // 2)
    store = Store(store_dir)
    with pytest.raises(StoreError):
        store.load(1)
    with pytest.raises(StoreError):
        store.load_head()
    assert "GEN_UNREADABLE" in _codes(store.check())
    assert store.repair() == []


def test_hash_invalid_generation_fails_closed(tmp_path):
    store_dir = str(tmp_path / "store")
    _, _, head_snap = _two_gen_store(store_dir)
    path = os.path.join(store_dir, "gen-000001.json")
    with open(path) as fh:
        data = json.load(fh)
    # Keep the JSON valid but change canonical meaning: stored hash
    # must no longer match.
    data["docs"][0]["size"] += 1
    with open(path, "w") as fh:
        json.dump(data, fh, indent=1, sort_keys=True)
        fh.write("\n")
    store = Store(store_dir)
    with pytest.raises(StoreError, match="hash mismatch"):
        store.load(1)
    with pytest.raises(StoreError):
        store.load_head()
    assert "GEN_HASH_MISMATCH" in _codes(store.check())
    assert store.repair() == []
    # The previous generation is still intact and loadable directly.
    snap0, _, _ = store.load(0)
    assert snap0.index_hash != head_snap.index_hash


def test_generation_number_mismatch_fails_closed(tmp_path):
    store_dir = str(tmp_path / "store")
    _two_gen_store(store_dir)
    path = os.path.join(store_dir, "gen-000001.json")
    with open(path) as fh:
        data = json.load(fh)
    data["generation"] = 0
    with open(path, "w") as fh:
        json.dump(data, fh, indent=1, sort_keys=True)
        fh.write("\n")
    store = Store(store_dir)
    with pytest.raises(StoreError):
        store.load(1)
    assert "GEN_NUMBER_MISMATCH" in _codes(store.check())


def test_gap_fails_check_but_head_stays_readable(tmp_path):
    store_dir = str(tmp_path / "store")
    store, _, head_snap = _two_gen_store(store_dir)
    os.unlink(os.path.join(store_dir, "gen-000000.json"))
    assert store.head() == 1
    with pytest.raises(StoreError):
        store.load(0)
    # HEAD itself is complete and readable, but the store is not whole:
    # check fails closed on the gap instead of blessing the store.
    snap, _, _ = store.load_head()
    assert snap.index_hash == head_snap.index_hash
    report = store.check()
    assert not report["ok"]
    assert "GEN_GAP" in _codes(report)
    # Repair deletes no generation data to "fix" a gap.
    assert store.repair() == []
    assert "GEN_GAP" in _codes(store.check())


def test_orphan_tmps_do_not_shadow_head(tmp_path):
    store_dir = str(tmp_path / "store")
    store, _, head_snap = _two_gen_store(store_dir)
    with open(os.path.join(store_dir, ".gen-000002.999.999.tmp"), "w") as fh:
        fh.write("{garbage")
    with open(os.path.join(store_dir, ".HEAD.tmp"), "w") as fh:
        fh.write("999")
    snap, _, _ = store.load_head()
    assert snap.index_hash == head_snap.index_hash
    assert _codes(store.check()) == {"ORPHAN_TMP"}
    assert sorted(store.repair()) == [".HEAD.tmp", ".gen-000002.999.999.tmp"]
    assert store.check()["ok"]


def test_healthy_store_checks_green(tmp_path):
    store_dir = str(tmp_path / "store")
    _two_gen_store(store_dir)
    report = Store(store_dir).check()
    assert report["ok"]
    assert report["head"] == 1
    assert report["generations"] == [0, 1]


# -- check command ----------------------------------------------------------


def test_check_command_reports_and_repairs_tmps(tmp_path, capsys):
    store_dir = str(tmp_path / "store")
    _two_gen_store(store_dir)
    assert cli_main(["check", "--store", store_dir]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] and out["head"] == 1

    with open(os.path.join(store_dir, ".HEAD.tmp"), "w") as fh:
        fh.write("0")
    assert cli_main(["check", "--store", store_dir]) == 1
    out = json.loads(capsys.readouterr().out)
    assert not out["ok"]
    assert [i["code"] for i in out["issues"]] == ["ORPHAN_TMP"]

    assert cli_main(["check", "--store", store_dir, "--repair"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] and out["repaired"] == [".HEAD.tmp"]


def test_check_command_fails_closed_on_corruption(tmp_path, capsys):
    store_dir = str(tmp_path / "store")
    _two_gen_store(store_dir)
    with open(os.path.join(store_dir, "HEAD"), "w") as fh:
        fh.write("7")
    assert cli_main(["check", "--store", store_dir]) == 1
    out = json.loads(capsys.readouterr().out)
    assert not out["ok"]
    assert "HEAD_DANGLING" in {i["code"] for i in out["issues"]}
    # --repair must not resolve corruption by guessing.
    assert cli_main(["check", "--store", store_dir, "--repair"]) == 1
    out = json.loads(capsys.readouterr().out)
    assert not out["ok"] and out["repaired"] == []


# -- reclamation journal integrity ----------------------------------------


def _publish_gen1(store):
    snap, canon = _crash_candidate()
    store.publish(snap, canon, {d.path: 1 for d in snap.docs}, {}, 0)
    return snap


def test_forged_reclaimed_journal_reports_gap(tmp_path):
    """A forged `.reclaimed` entry cannot hide a lost generation.

    `check()` trusts the journal only with a valid checksum; a torn
    or forged record fails closed toward GEN_GAP, never silence.
    """
    store_dir = str(tmp_path / "store")
    store, _seed = _seeded_store(store_dir)
    _publish_gen1(store)
    assert store.reclaim()["removed"] == [0]
    assert store.check()["ok"]
    # Forge: claim gen 0 reclaimed without a valid checksum.
    with open(os.path.join(store_dir, ".reclaimed"), "w") as fh:
        json.dump({"reclaimed": [0], "sha256": "0" * 64}, fh)
    report = Store(store_dir).check()
    assert not report["ok"]
    assert "GEN_GAP" in _codes(report)
    # Repair must not bless the forgery by deleting or rewriting it.
    assert Store(store_dir).repair() == []
    assert "GEN_GAP" in _codes(Store(store_dir).check())


def test_reclaimed_tmp_orphan_reported_and_repaired(tmp_path):
    """A crash inside `_write_reclaimed` leaves reportable staging."""
    store_dir = str(tmp_path / "store")
    store, _seed = _seeded_store(store_dir)
    with open(os.path.join(store_dir, ".reclaimed.tmp"), "w") as fh:
        fh.write('{"reclaimed": []}')
    report = store.check()
    assert "ORPHAN_TMP" in _codes(report)
    assert any(
        i["detail"].endswith(".reclaimed.tmp") for i in report["issues"]
    )
    assert store.repair() == [".reclaimed.tmp"]
    assert store.check()["ok"]


def test_manifest_corrupt_reported_never_repaired(tmp_path):
    """`.compact.json` health is checked; repair never deletes state."""
    store_dir = str(tmp_path / "store")
    store, _seed = _seeded_store(store_dir)
    assert "MANIFEST_CORRUPT" not in _codes(store.check())
    with open(os.path.join(store_dir, ".compact.json"), "w") as fh:
        fh.write("{truncated")
    report = store.check()
    assert "MANIFEST_CORRUPT" in _codes(report)
    assert store.repair() == []
    assert os.path.exists(os.path.join(store_dir, ".compact.json"))
    with open(os.path.join(store_dir, ".compact.json"), "w") as fh:
        json.dump({"schedule": 42}, fh)
    assert "MANIFEST_CORRUPT" in _codes(store.check())


def test_l0_gc_issues_no_barriers(tmp_path, monkeypatch):
    """L0 means no storage barriers on ANY path, including GC."""
    syncs = []
    orig = store_mod._fsync_dir

    def counting(path):
        syncs.append(path)
        return orig(path)

    monkeypatch.setattr(store_mod, "_fsync_dir", counting)
    store, _seed = _seeded_store(str(tmp_path / "s0"), durability="L0")
    _publish_gen1(store)
    assert store.reclaim()["removed"] == [0]
    assert syncs == []
    store2, _seed2 = _seeded_store(str(tmp_path / "s2"), durability="L2")
    _publish_gen1(store2)
    assert store2.reclaim()["removed"] == [0]
    assert len(syncs) >= 1


def test_windows_dirsync_noop_and_msvcrt_lock(tmp_path, monkeypatch):
    """Portability paths: nt dir-sync is a documented no-op; the msvcrt
    lock branch issues exactly lock/unlock (POSIX runs this with a stub)."""
    monkeypatch.setattr(os, "name", "nt")
    assert store_mod._fsync_dir(str(tmp_path)) is False
    calls = []

    class _StubMsvcrt:
        LK_LOCK = 1
        LK_UNLCK = 2

        @staticmethod
        def locking(fd, mode, nbytes):
            calls.append((mode, nbytes))

    monkeypatch.setattr(store_mod, "msvcrt", _StubMsvcrt)
    monkeypatch.setattr(store_mod, "fcntl", None)
    with open(os.path.join(str(tmp_path), "l"), "w") as fh:
        store_mod._lock_file(fh)
        store_mod._unlock_file(fh)
    assert [mode for mode, _ in calls] == [1, 2]
