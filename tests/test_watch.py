"""Watch prototype: polling follows mutations to new generations."""

import threading
import time

from conftest import full_build, full_build_v2, rebuild_bytes, write_file
from mncs_index.pipeline import BuildConfig
from mncs_index.store import Store
from mncs_index.watch import Watcher


def test_watch_detects_hint_collision(kernels, tmp_path, monkeypatch):
    """A same-size content change with lying hints must still publish.

    Simulates an exact CRC32 collision at the hint layer (path, size,
    and crc32 identical; content different) by blinding the watcher's
    acquisition stage. The periodic authoritative validation must still
    surface the change as a modify (3) — never stale reuse.
    """
    import mncs_index.watch as watch_mod

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    write_file(str(corpus), "a.txt", b"a" * 16)
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    full_build(
        kernels, str(corpus), str(store_dir), generation=0,
        workers=2, seed=7, mncs_digest=False,
    )
    store = Store(str(store_dir))
    base_snap, _, base_crc = store.load_head()
    # Same size, different content.
    write_file(str(corpus), "a.txt", b"b" * 16)

    real_acquire = watch_mod._acquire

    def blind_acquire(root, config):
        found = real_acquire(root, config)
        for it in found.items:
            if it.path in base_crc:
                it.crc = base_crc[it.path]
        found.snapshot_id = base_snap.snapshot_id
        return found

    monkeypatch.setattr(watch_mod, "_acquire", blind_acquire)
    watcher = Watcher(
        str(corpus),
        store,
        kernels,
        BuildConfig(workers=2, seed=7, mncs_digest=False),
        interval_s=0.05,
        quiet_polls=1,
        validate_every=1,
    )
    published = watcher.run(max_generations=1)
    assert published == [1]
    assert watcher.validations >= 1
    assert watcher.events[-1]["verdicts"]["a.txt"] == 3
    snap, _, _ = store.load_head()
    ref, _ = rebuild_bytes(
        kernels, str(corpus), workers=2, seed=7, mncs_digest=False
    )
    assert snap.index_hash == ref.index_hash


def test_watch_rich_keeps_tables(kernels, tmp_path):
    """Watching a canonical-v2 store must not degrade it to v1."""
    from mncs_index.indexer import build_snapshot_v2

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    write_file(
        str(corpus), "a.mncs", b"module demo.app;\n\nfn keep_me() -> (r: i64) {\n}\n"
    )
    write_file(str(corpus), "b.md", b"# First\n\ntext\n")
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    base, _canon, _corpus = full_build_v2(
        kernels, str(corpus), str(store_dir), generation=0,
        workers=2, seed=7, mncs_digest=False,
    )
    assert base.has_rich and len(base.syms) >= 1
    # Mutate before watching: the watcher must pick it up and keep tables.
    write_file(str(corpus), "b.md", b"# First\n\ntext\n\n## Second\n\nmore\n")
    store = Store(str(store_dir))
    watcher = Watcher(
        str(corpus),
        store,
        kernels,
        BuildConfig(workers=2, seed=7, mncs_digest=False),
        interval_s=0.05,
        quiet_polls=1,
        rich=True,
    )
    published = watcher.run(max_generations=1)
    assert published == [1]
    snap, _, _ = store.load_head()
    assert snap.has_rich, "watch publication dropped the v2 rich tables"
    assert {s.name for s in snap.syms} >= {"keep_me"}
    assert len(snap.headings) == 2
    cfg = BuildConfig(workers=2, seed=7, mncs_digest=False)
    re_snap, _re, _c, _b = build_snapshot_v2(str(corpus), 1, kernels, cfg)
    assert snap.index_hash == re_snap.index_hash


def test_watch_follows_mutations(kernels, tmp_path):
    import shutil

    from conftest import FIXTURES

    corpus = tmp_path / "corpus"
    shutil.copytree(FIXTURES, corpus)
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    full_build(kernels, str(corpus), str(store_dir), generation=0, workers=4, seed=7)
    store = Store(str(store_dir))
    watcher = Watcher(
        str(corpus),
        store,
        kernels,
        BuildConfig(workers=4, seed=7),
        interval_s=0.2,
        quiet_polls=2,
    )

    def mutate():
        time.sleep(0.6)
        write_file(str(corpus), "watched.md", b"# watched\n\nappears\n")
        time.sleep(1.2)
        write_file(str(corpus), "b.md", b"# b changed by watch test\n")

    thread = threading.Thread(target=mutate)
    thread.start()
    try:
        published = watcher.run(max_generations=2)
    finally:
        watcher.stop()
        thread.join()
    assert len(published) == 2
    snap, _, _ = store.load_head()
    ref, _ = rebuild_bytes(kernels, str(corpus), workers=2, seed=3)
    assert snap.index_hash == ref.index_hash
    assert any(v == 1 for ev in watcher.events for v in ev["verdicts"].values())
