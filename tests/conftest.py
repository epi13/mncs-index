"""Shared pytest harness: MNCS binary resolution, build helpers."""

from __future__ import annotations

import os
import shutil
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNER = os.path.join(REPO, "runner")
FIXTURES = os.path.join(REPO, "tests", "fixtures", "corpus")
SRC = os.path.join(REPO, "src")

sys.path.insert(0, RUNNER)

from mncs_index.bridge import Bridge
from mncs_index.indexer import build_snapshot
from mncs_index.kernels import Kernels
from mncs_index.pipeline import BuildConfig
from mncs_index.store import Store


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: megabyte-scale stress (use --run-slow)")


def pytest_addoption(parser):
    parser.addoption("--run-slow", action="store_true", help="run slow stress tests")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-slow"):
        skip = pytest.mark.skip(reason="slow test (use --run-slow)")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip)


def mncs_binary() -> str:
    if "MNCS_BIN" in os.environ:
        path = os.environ["MNCS_BIN"]
    else:
        path = os.path.normpath(
            os.path.join(REPO, "..", "mncs-language", "target", "debug", "mncs")
        )
    if not (os.path.isfile(path) and os.access(path, os.X_OK)):
        pytest.fail(
            f"MNCS executor binary not found/executable: {path}. "
            "Build mncs-language (cargo build -p mncs-cli) or set MNCS_BIN."
        )
    return path


@pytest.fixture(scope="session")
def binary() -> str:
    return mncs_binary()


@pytest.fixture(scope="session")
def kernels(binary) -> Kernels:
    return Kernels(Bridge(binary=binary, src_dir=SRC))


@pytest.fixture()
def workdir(tmp_path):
    corpus = tmp_path / "corpus"
    shutil.copytree(FIXTURES, corpus)
    store = tmp_path / "store"
    store.mkdir()
    return tmp_path, str(corpus), str(store)


def full_build(kernels, corpus_dir, store_dir, generation=0, **cfg_kwargs):
    cfg = BuildConfig(**cfg_kwargs)
    snap, canon, corpus, _ = build_snapshot(corpus_dir, generation, kernels, cfg)
    store = Store(store_dir)
    established = {d.path: generation for d in snap.docs}
    store.publish(snap, canon, established, {it.path: it.crc for it in corpus.items})
    return snap, canon, corpus


def rebuild_bytes(kernels, corpus_dir, generation=0, **cfg_kwargs):
    cfg = BuildConfig(**cfg_kwargs)
    snap, canon, _, _ = build_snapshot(corpus_dir, generation, kernels, cfg)
    return snap, canon


def write_file(root: str, rel: str, data: bytes) -> None:
    full = os.path.join(root, rel)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "wb") as fh:
        fh.write(data)
