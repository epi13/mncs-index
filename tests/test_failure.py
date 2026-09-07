"""Failure semantics: failed builds must not publish half-valid snapshots."""

import os
import threading

import pytest
from conftest import full_build, rebuild_bytes
from mncs_index.bridge import Bridge, MNCSError
from mncs_index.discover import DiscoveryError
from mncs_index.indexer import build_snapshot
from mncs_index.kernels import Kernels
from mncs_index.pipeline import BuildCancelled, BuildConfig, BuildFailed
from mncs_index.store import Store


def test_failed_build_keeps_previous_generation(kernels, workdir, monkeypatch):
    _, corpus_dir, store_dir = workdir
    first, _, _ = full_build(
        kernels, corpus_dir, store_dir, generation=0, workers=4, seed=7
    )
    store = Store(store_dir)
    assert store.head() == 0
    # Inject a worker failure mid-pipeline: the build must fail and the
    # previous generation must remain the only published snapshot.
    real_leaf = kernels.leaf_window

    def poisoned(state, chunk, open_in):
        if b"quick brown" in chunk:
            raise RuntimeError("injected worker failure")
        return real_leaf(state, chunk, open_in)

    monkeypatch.setattr(kernels, "leaf_window", poisoned)
    with pytest.raises(BuildFailed):
        build_snapshot(corpus_dir, 1, kernels, BuildConfig(workers=4, seed=7))
    assert store.head() == 0
    snap, _, _ = store.load_head()
    assert snap.index_hash == first.index_hash


def test_unreadable_file_fails_discovery(kernels, workdir):
    import stat

    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("permission bits do not restrict root")
    _, corpus_dir, store_dir = workdir
    target = f"{corpus_dir}/b.md"
    os.chmod(target, 0)
    try:
        with pytest.raises(DiscoveryError):
            rebuild_bytes(kernels, corpus_dir, workers=2)
    finally:
        os.chmod(target, stat.S_IRUSR | stat.S_IWUSR)
    assert Store(store_dir).head() is None


def test_mncs_error_propagates_typed(kernels):
    with pytest.raises(MNCSError):
        kernels.b.call("digest.mncs", "mncs.index.digest.v1", "no_such_fn", [])


def test_starved_budget_fails_build(binary, workdir):
    from conftest import SRC

    _, corpus_dir, store_dir = workdir
    starved = Kernels(Bridge(binary=binary, src_dir=SRC, step_budget=1))
    with pytest.raises(BuildFailed):
        build_snapshot(corpus_dir, 0, starved, BuildConfig(workers=2, seed=7))
    assert Store(store_dir).head() is None


def test_cancellation_publishes_nothing(kernels, workdir):
    _, corpus_dir, store_dir = workdir
    cancel = threading.Event()
    cancel.set()  # cancel before starting
    with pytest.raises((BuildCancelled, BuildFailed)):
        build_snapshot(
            corpus_dir, 0, kernels, BuildConfig(workers=4, cancel_event=cancel)
        )
    assert Store(store_dir).head() is None


def test_unreadable_corpus_root(kernels, tmp_path):
    with pytest.raises(DiscoveryError):
        rebuild_bytes(kernels, str(tmp_path / "no-such-dir"), workers=2)


def test_broken_mncs_binary_fails_build(workdir):
    from conftest import SRC

    _, corpus_dir, store_dir = workdir
    broken = Kernels(Bridge(binary="/nonexistent/mncs", src_dir=SRC))
    with pytest.raises(BuildFailed):
        build_snapshot(corpus_dir, 0, broken, BuildConfig(workers=2, seed=7))
    assert Store(store_dir).head() is None
