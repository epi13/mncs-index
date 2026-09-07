"""Watch prototype: polling follows mutations to new generations."""

import threading
import time

from conftest import full_build, rebuild_bytes, write_file
from mncs_index.pipeline import BuildConfig
from mncs_index.store import Store
from mncs_index.watch import Watcher


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
