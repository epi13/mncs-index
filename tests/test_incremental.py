"""Incremental equivalence: incremental(A -> B) == rebuild(B), canonically."""

import pytest
from conftest import full_build, rebuild_bytes, write_file
from mncs_index.discover import discover
from mncs_index.indexer import incremental_snapshot
from mncs_index.pipeline import BuildConfig
from mncs_index.store import Store


@pytest.fixture(scope="module")
def baseline(kernels, tmp_path_factory):
    """One published generation-0 template; tests copy it for isolation."""
    import shutil

    from conftest import FIXTURES

    tmp = tmp_path_factory.mktemp("baseline")
    corpus = tmp / "corpus"
    shutil.copytree(FIXTURES, corpus)
    store = tmp / "store"
    store.mkdir()
    full_build(kernels, str(corpus), str(store), generation=0, workers=4, seed=7)
    return str(corpus), str(store)


@pytest.fixture()
def branch(baseline, tmp_path):
    import shutil

    corpus_src, store_src = baseline
    corpus = tmp_path / "corpus"
    store = tmp_path / "store"
    shutil.copytree(corpus_src, corpus)
    shutil.copytree(store_src, store)
    return str(corpus), str(store)


def _incremental(kernels, corpus_dir, store_dir, generation):
    store = Store(store_dir)
    prev, established, prev_crc = store.load_head()
    snap, canon, corpus, verdicts = incremental_snapshot(
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
    store.publish(snap, canon, estat, {it.path: it.crc for it in corpus.items})
    return snap, canon, verdicts


def test_unchanged_corpus(kernels, branch):
    corpus_dir, store_dir = branch
    first, _, _ = Store(store_dir).load_head()
    second, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert all(v == 0 for v in verdicts.values())
    assert second.index_hash == first.index_hash


def test_file_added(kernels, branch):
    corpus_dir, store_dir = branch
    write_file(corpus_dir, "added.md", b"# Added\n\nnew words\n")
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["added.md"] == 1
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=2, seed=3, mncs_digest=False)
    assert inc.index_hash == ref.index_hash


def test_file_removed(kernels, branch):
    import os

    corpus_dir, store_dir = branch
    os.remove(f"{corpus_dir}/f.txt")
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["f.txt"] == 2
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=2, seed=3, mncs_digest=False)
    assert inc.index_hash == ref.index_hash
    assert all(d.path != "f.txt" for d in inc.docs)


def test_file_changed(kernels, branch):
    corpus_dir, store_dir = branch
    write_file(corpus_dir, "b.md", b"# Changed\n\ncompletely different content here\n")
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["b.md"] == 3
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=8, seed=11, mncs_digest=False)
    assert inc.index_hash == ref.index_hash


def test_file_renamed(kernels, branch):
    import os

    corpus_dir, store_dir = branch
    os.rename(f"{corpus_dir}/f.txt", f"{corpus_dir}/renamed.txt")
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["f.txt"] == 2
    assert verdicts["renamed.txt"] == 1
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=2, seed=3, mncs_digest=False)
    assert inc.index_hash == ref.index_hash


def test_multiple_changes_one_batch(kernels, branch):
    import os

    corpus_dir, store_dir = branch
    write_file(corpus_dir, "b.md", b"changed\n")
    os.remove(f"{corpus_dir}/c.json")
    write_file(corpus_dir, "new1.mncs", b"module fresh;\n")
    write_file(corpus_dir, "sub/new2.txt", b"nested new file\n")
    write_file(corpus_dir, "d.toml", b'title = "delta"\ncount = 3\n')
    inc, _, _ = _incremental(kernels, corpus_dir, store_dir, 1)
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=4, seed=7)
    assert inc.index_hash == ref.index_hash


def test_chained_incrementals(kernels, branch):
    corpus_dir, store_dir = branch
    write_file(corpus_dir, "b.md", b"v2\n")
    _incremental(kernels, corpus_dir, store_dir, 1)
    write_file(corpus_dir, "b.md", b"v3 with more\n")
    inc, _, _ = _incremental(kernels, corpus_dir, store_dir, 2)
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=4, seed=7)
    assert inc.index_hash == ref.index_hash


def test_incremental_carries_mncs_fingerprint(kernels, branch):
    """One end-to-end incremental run with full MNCS fingerprints."""
    from mncs_index.indexer import incremental_snapshot
    from mncs_index.pipeline import BuildConfig
    from mncs_index.store import Store

    corpus_dir, store_dir = branch
    write_file(corpus_dir, "b.md", b"# Changed\n\nnew content\n")
    store = Store(store_dir)
    prev, established, prev_crc = store.load_head()
    snap, canon, _, _ = incremental_snapshot(
        corpus_dir, 1, prev, prev_crc, kernels, BuildConfig(workers=4, seed=7)
    )
    estat = dict(established)
    for d in snap.docs:
        estat.setdefault(d.path, 1)
    store.publish(
        snap, canon, estat, {it.path: it.crc for it in discover(corpus_dir).items}
    )
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=2, seed=3)
    assert snap.mncs_fingerprint and ref.mncs_fingerprint
    assert snap.mncs_fingerprint == ref.mncs_fingerprint
    assert snap.index_hash == ref.index_hash
