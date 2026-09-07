"""Stress workloads: sizes, shapes, and worker counts beyond fixtures.

Runtime strategy: the default suite stays bounded for CI (matrix files up
to ~16KB, shared builds). Megabyte-scale determinism is marked `slow` and
runs with `--run-slow`; see conftest.py.
"""

import os
import random
import time

import pytest
from conftest import rebuild_bytes, write_file


def make_stress_corpus(
    root: str, seed: int = 20260906, big_kb: tuple = (8, 16)
) -> dict:
    """Many small files, fewer large ones, duplicates, empties, deep paths,
    skewed sizes. Returns summary stats."""
    r = random.Random(seed)
    words = [
        "fn",
        "module",
        "index",
        "record",
        "worker",
        "alpha",
        "beta",
        "gamma",
        "delta",
        "digest",
        "query",
        "snapshot",
        "token",
    ]
    for i in range(120):
        ext = r.choice(["mncs", "md", "json", "txt", "toml"])
        lines = []
        for _ in range(r.randrange(1, 12)):
            lines.append(" ".join(r.choice(words) for _ in range(r.randrange(1, 9))))
        write_file(root, f"s{i:03d}.{ext}", ("\n".join(lines) + "\n").encode())
    # Duplicated content under different paths.
    dup = b"module dup;\n\nfn shared() -> (result: i64) {\n    return 40 + 2;\n}\n"
    for i in range(10):
        write_file(root, f"dup{i}.mncs", dup)
    # Empties.
    for i in range(5):
        write_file(root, f"empty{i}.txt", b"")
    # Deep paths.
    deep = os.path.join(*[f"level{k}" for k in range(8)])
    for i in range(4):
        write_file(root, f"{deep}/deep{i}.md", b"# deep\n\nnested content\n")
    # Skewed: two large files (sizes parametric; matrix uses small ones).
    big1 = ("word " * (big_kb[0] * 200)).encode()
    write_file(root, "big1.txt", big1)
    big2 = ("fn x() {}\n".join([""] * (big_kb[1] * 200))).encode()
    write_file(root, "big2.mncs", big2)
    total = sum(
        os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(root) for f in fs
    )
    count = sum(len(fs) for _, _, fs in os.walk(root))
    return {"files": count, "bytes": total}


@pytest.fixture(scope="module")
def stress_corpus(tmp_path_factory):
    root = tmp_path_factory.mktemp("stress") / "corpus"
    root.mkdir()
    stats = make_stress_corpus(str(root))
    stats["root"] = str(root)
    return stats


@pytest.fixture(scope="module")
def stress_snap(kernels, stress_corpus):
    snap, _ = rebuild_bytes(
        kernels, stress_corpus["root"], workers=8, seed=7, mncs_digest=False
    )
    return snap


def test_stress_worker_matrix_converges(kernels, stress_corpus):
    root = stress_corpus["root"]
    ref = None
    timings = {}
    for workers in (4, 8, 16):
        started = time.monotonic()
        snap, _ = rebuild_bytes(
            kernels, root, workers=workers, seed=7, mncs_digest=False
        )
        timings[workers] = round(time.monotonic() - started, 2)
        if ref is None:
            ref = snap.index_hash
        assert snap.index_hash == ref
    print(
        f"\nstress files={stress_corpus['files']} bytes={stress_corpus['bytes']} "
        f"timings={timings} calls={kernels.b.stats.calls}"
    )
    assert stress_corpus["files"] >= 140


def test_stress_32_workers(kernels, stress_corpus):
    root = stress_corpus["root"]
    ref, _ = rebuild_bytes(kernels, root, workers=4, seed=7, mncs_digest=False)
    snap, _ = rebuild_bytes(
        kernels, root, workers=32, seed=21, delay_ms=1.0, mncs_digest=False
    )
    assert snap.index_hash == ref.index_hash


def test_stress_fingerprint_agrees(kernels, stress_corpus):
    root = stress_corpus["root"]
    a, _ = rebuild_bytes(kernels, root, workers=2, seed=7)
    b, _ = rebuild_bytes(kernels, root, workers=8, seed=7)
    assert a.mncs_fingerprint and a.mncs_fingerprint == b.mncs_fingerprint


def test_duplicate_content_shares_digest(stress_snap):
    dups = sorted(d.digest for d in stress_snap.docs if d.path.startswith("dup"))
    assert len(dups) == 10 and len(set(dups)) == 1


def test_word_counts_match_wc(stress_snap, stress_corpus):
    """Spot-check MNCS-computed word counts against `wc -w` semantics."""
    import subprocess

    root = stress_corpus["root"]
    checked = 0
    for doc in stress_snap.docs:
        if doc.kind not in (1, 2, 6) or doc.size > 65536:
            continue
        out = subprocess.run(
            ["wc", "-w", f"{root}/{doc.path}"],
            capture_output=True,
            text=True,
            check=True,
        )
        assert int(out.stdout.split()[0]) == doc.words, doc.path
        checked += 1
        if checked >= 15:
            break
    assert checked >= 10


def test_medium_file_determinism(kernels, tmp_path):
    blob = bytes((i * 31 + 7) % 251 for i in range(16 * 1024))
    write_file(str(tmp_path), "medium.txt", blob)
    a, _ = rebuild_bytes(kernels, str(tmp_path), workers=1, mncs_digest=False)
    b, _ = rebuild_bytes(kernels, str(tmp_path), workers=8, seed=5, mncs_digest=False)
    assert a.index_hash == b.index_hash
    assert len(a.docs) == 1 and a.docs[0].size == 16 * 1024


@pytest.mark.slow
def test_megabyte_file_determinism(kernels, tmp_path):
    blob = bytes((i * 31 + 7) % 251 for i in range(1024 * 1024))
    write_file(str(tmp_path), "mega.txt", blob)
    a, _ = rebuild_bytes(kernels, str(tmp_path), workers=4, mncs_digest=False)
    b, _ = rebuild_bytes(kernels, str(tmp_path), workers=16, seed=5, mncs_digest=False)
    assert a.index_hash == b.index_hash
    assert len(a.docs) == 1 and a.docs[0].size == 1024 * 1024
