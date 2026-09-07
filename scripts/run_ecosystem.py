#!/usr/bin/env python3
"""Multi-repo ecosystem index run: every locally available mncs-* repo.

Stages one corpus from the real repos, then builds canonical-v2 rich
snapshots at two worker counts and checks convergence. The default
corpus rule is a uniform survey, recorded in the evidence JSON:

    alphabetically-first *.mncs source per sibling ../mncs-* repo
    (files over --per-file-bytes skipped),
    excluding .git/target/node_modules/__pycache__/caches/build dirs
    and symlinks; staged as <stage>/<repo>/<relpath> so every record
    path names its home repo.

Only *.mncs: the cross-repo MNCS source graph (module defines,
use-driven depends-on/references) at a byte volume real-kernel
execution can still cover in-session (PRESS-010: one subprocess per
kernel call; the full 437-file census needs ~400k calls — see the
evidence note). Pass --max-files-per-repo 0 --per-file-bytes 0 for
the census. One process, one shared Kernels handle (pure memoization
caches, exactly like the session-scoped test fixture), so the second
worker count re-verifies convergence; per-run metrics come from each
snapshot, call counts are process-global.

Usage:
    MNCS_BIN=... python3 scripts/run_ecosystem.py --out evidence/ecosystem.json [--workers 2,8]

Writes the evidence JSON and prints a one-line summary.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "runner"))

from mncs_index.bridge import Bridge  # noqa: E402
from mncs_index.indexer import build_snapshot_v2  # noqa: E402
from mncs_index.kernels import Kernels  # noqa: E402
from mncs_index.pipeline import BuildConfig  # noqa: E402

SKIP_DIRS = frozenset(
    {
        ".git",
        "target",
        "node_modules",
        "__pycache__",
        ".ruff_cache",
        ".pytest_cache",
        ".mypy_cache",
        ".venv",
        "dist",
        "build",
        ".worktrees",
        ".hg",
        ".svn",
        # Never inventory our own staging output: the stage lives inside
        # mncs-index, which sorts after the repos already staged into it.
        ".ecosystem-stage",
        ".mncs-index",
    }
)


def stage_ecosystem(
    projects_dir: str,
    stage: str,
    max_files_per_repo: int = 0,
    per_file_bytes: int = 0,
) -> tuple[dict, list]:
    """Copy repo *.mncs sources into stage/<repo>/... .

    Caps of 0 mean uncapped (full census). Otherwise the inventory per
    repo is sorted by relative path and truncated to `max_files_per_repo`
    entries, and entries larger than `per_file_bytes` are skipped; every
    skip is returned in `excluded` as (repo, rel, bytes, reason).
    """
    if os.path.isdir(stage):
        shutil.rmtree(stage)
    os.makedirs(stage)
    repos: dict = {}
    excluded: list = []
    for repo in sorted(os.listdir(projects_dir)):
        root = os.path.join(projects_dir, repo)
        if not repo.startswith("mncs-") or not os.path.isdir(root):
            continue
        inventory: list[tuple[str, str]] = []
        for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
            dirnames[:] = sorted(
                d
                for d in dirnames
                if d not in SKIP_DIRS
                and not os.path.islink(os.path.join(dirpath, d))
            )
            for name in sorted(filenames):
                if not name.endswith(".mncs"):
                    continue
                full = os.path.join(dirpath, name)
                if os.path.islink(full) or not os.path.isfile(full):
                    continue
                inventory.append((os.path.relpath(full, root), full))
        inventory.sort()
        if max_files_per_repo and len(inventory) > max_files_per_repo:
            for rel, full in inventory[max_files_per_repo:]:
                excluded.append(
                    (repo, rel, os.path.getsize(full), "beyond max_files_per_repo")
                )
            inventory = inventory[:max_files_per_repo]
        files = 0
        size = 0
        for rel, full in inventory:
            nbytes = os.path.getsize(full)
            if per_file_bytes and nbytes > per_file_bytes:
                excluded.append((repo, rel, nbytes, "beyond per_file_bytes"))
                continue
            dest = os.path.join(stage, repo, rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copyfile(full, dest)
            files += 1
            size += nbytes
        if files:
            repos[repo] = {"files": files, "bytes": size}
    return repos, excluded


def language_proof(language_dir: str, lock_path: str) -> dict:
    locked = json.load(open(lock_path))["revision"]
    head = subprocess.run(
        ["git", "-C", language_dir, "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    dirty = (
        subprocess.run(
            ["git", "-C", language_dir, "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip() != ""
    )
    binary = os.environ.get("MNCS_BIN") or os.path.join(
        language_dir, "target", "debug", "mncs"
    )
    return {
        "locked_revision": locked,
        "actual_head": head,
        "head_matches_lock": head == locked,
        "worktree_dirty": dirty,
        "binary": binary,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", default="8,2")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--max-files-per-repo", type=int, default=1)
    ap.add_argument("--per-file-bytes", type=int, default=8192)
    args = ap.parse_args()

    projects = os.path.dirname(REPO)
    language_dir = os.path.join(projects, "mncs-language")
    stage = os.path.join(REPO, ".ecosystem-stage")
    print(f"[ecosystem] staging {projects}/mncs-* -> {stage}", flush=True)
    repos, excluded = stage_ecosystem(
        projects,
        stage,
        max_files_per_repo=args.max_files_per_repo,
        per_file_bytes=args.per_file_bytes,
    )
    total_files = sum(r["files"] for r in repos.values())
    total_bytes = sum(r["bytes"] for r in repos.values())
    print(
        f"[ecosystem] staged {len(repos)} repos, "
        f"{total_files} files, {total_bytes} bytes",
        flush=True,
    )

    bridge = Bridge()
    kernels = Kernels(bridge)
    runs = []
    hashes = set()
    try:
        for w in [int(x) for x in args.workers.split(",")]:
            print(f"[ecosystem] build workers={w} ...", flush=True)
            t0 = time.monotonic()
            snap, _canon, corpus, _ = build_snapshot_v2(
                stage,
                0,
                kernels,
                BuildConfig(workers=w, seed=args.seed, mncs_digest=False),
            )
            dt = time.monotonic() - t0
            hashes.add(snap.index_hash)
            runs.append(
                {
                    "workers": w,
                    "seed": args.seed,
                    "files": len(corpus.items),
                    "bytes_indexed": sum(d.size for d in snap.docs),
                    "docs": len(snap.docs),
                    "terms": len(snap.terms),
                    "declarations": len(snap.syms),
                    "headings": len(snap.headings),
                    "relationships": len(snap.rels),
                    "pressure_mentions": len(snap.press),
                    "skipped": corpus.skipped,
                    "index_hash": snap.index_hash,
                    "wall_s": round(dt, 2),
                }
            )
            print(
                f"[ecosystem] workers={w} done in {dt:.1f}s "
                f"hash={snap.index_hash[:16]}",
                flush=True,
            )
    finally:
        shutil.rmtree(stage, ignore_errors=True)

    record = {
        "corpus": "multi-repo ecosystem: mncs-index + locally available mncs-* repos",
        "corpus_rule": (
            "**/*.mncs per repo (excluding .git/target/caches/build dirs and "
            "symlinks); survey caps: max_files_per_repo="
            f"{args.max_files_per_repo} (alphabetically first), per_file_bytes="
            f"{args.per_file_bytes} (0 = uncapped)"
        ),
        "repos": repos,
        "excluded": [
            {"repo": r, "path": p, "bytes": n, "reason": why}
            for r, p, n, why in excluded
        ],
        "total_files": total_files,
        "total_bytes": total_bytes,
        "runs": runs,
        "converged": len(hashes) == 1,
        "mncs_calls": bridge.stats.calls,
        "max_in_flight": bridge.stats.max_in_flight,
        "mncs_language": language_proof(
            language_dir, os.path.join(REPO, "mncs-language.lock.json")
        ),
        "note": (
            "Survey, not census: one alphabetically-first *.mncs source per "
            "repo keeps cold real-kernel execution in-session (PRESS-010: one "
            "mncs execute subprocess per kernel call; a full 437-file census "
            "needs ~400k calls). Caps and every exclusion are recorded above; "
            "rerun with --max-files-per-repo 0 --per-file-bytes 0 for the "
            "census. One process, one shared Kernels handle (pure "
            "memoization, as in tests): the second worker count re-verifies "
            "hash convergence. Call counts are process-global, hashes are "
            "canonical."
        ),
    }
    with open(args.out, "w") as fh:
        json.dump(record, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(
        json.dumps(
            {
                "out": args.out,
                "converged": record["converged"],
                "hash": next(iter(hashes)) if hashes else None,
                "calls": bridge.stats.calls,
            }
        ),
        flush=True,
    )
    return 0 if record["converged"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
