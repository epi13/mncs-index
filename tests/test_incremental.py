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
    base = prev.generation if prev is not None else None
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
    store.publish(
        snap, canon, estat, {it.path: it.crc for it in corpus.items}, base
    )
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


def _mutate_keep_size(corpus_dir: str, rel: str) -> bytes:
    """Rewrite `rel` with different bytes of identical length."""
    import os

    full = os.path.join(corpus_dir, rel)
    with open(full, "rb") as fh:
        original = fh.read()
    mutated = bytearray(original)
    mutated[0] ^= 0x01
    mutated[-1] ^= 0x02
    assert bytes(mutated) != original
    write_file(corpus_dir, rel, bytes(mutated))
    return original


def test_same_size_change_detected(kernels, branch):
    """Same-size content change: verdict 3 + fresh digest, never stale."""
    corpus_dir, store_dir = branch
    prev, _, _ = Store(store_dir).load_head()
    old_doc = next(d for d in prev.docs if d.path == "b.md")
    original = _mutate_keep_size(corpus_dir, "b.md")
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["b.md"] == 3
    new_doc = next(d for d in inc.docs if d.path == "b.md")
    assert new_doc.size == old_doc.size == len(original)
    assert new_doc.digest != old_doc.digest
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=2, seed=3, mncs_digest=False)
    assert inc.index_hash == ref.index_hash
    assert [(d.path, d.digest) for d in inc.docs] == [
        (d.path, d.digest) for d in ref.docs
    ]


def test_simulated_crc32_collision_detected(kernels, branch):
    """A same-size CRC32 collision can never reuse stale records.

    CRC32 is never canonical truth: the stored hint is forced to match
    the mutated bytes (exactly what a real collision would present —
    never relying on CRC rarity), and the incremental build must still
    report modified (3) with the fresh MNCS digest, identical to a
    clean rebuild.
    """
    from mncs_index.model import crc32_of

    corpus_dir, store_dir = branch
    prev, established, prev_crc = Store(store_dir).load_head()
    old_doc = next(d for d in prev.docs if d.path == "b.md")
    original = _mutate_keep_size(corpus_dir, "b.md")
    with open(f"{corpus_dir}/b.md", "rb") as fh:
        new_bytes = fh.read()
    assert len(new_bytes) == len(original)
    # Simulate the collision: the hint claims the new bytes are unchanged.
    prev_crc = dict(prev_crc)
    prev_crc["b.md"] = crc32_of(new_bytes)
    snap, canon, corpus, verdicts = incremental_snapshot(
        corpus_dir,
        1,
        prev,
        prev_crc,
        kernels,
        BuildConfig(workers=4, seed=7, mncs_digest=False),
    )
    assert verdicts["b.md"] == 3
    new_doc = next(d for d in snap.docs if d.path == "b.md")
    assert new_doc.size == old_doc.size == len(original)
    assert new_doc.digest != old_doc.digest
    estat = dict(established)
    for d in snap.docs:
        estat.setdefault(d.path, 1)
    Store(store_dir).publish(
        snap, canon, estat, {it.path: it.crc for it in corpus.items}, prev.generation
    )
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=2, seed=3, mncs_digest=False)
    assert snap.index_hash == ref.index_hash
    assert [(d.path, d.digest) for d in snap.docs] == [
        (d.path, d.digest) for d in ref.docs
    ]


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
        snap,
        canon,
        estat,
        {it.path: it.crc for it in discover(corpus_dir).items},
        prev.generation,
    )
    ref, _ = rebuild_bytes(kernels, corpus_dir, workers=2, seed=3)
    assert snap.mncs_fingerprint and ref.mncs_fingerprint
    assert snap.mncs_fingerprint == ref.mncs_fingerprint
    assert snap.index_hash == ref.index_hash
