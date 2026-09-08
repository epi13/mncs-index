#!/usr/bin/env python3
"""Tiered MNCS-family scale campaign: corpus, convergence, mutations.

Stages a tiered corpus from the real sibling ``mncs-*`` repos, then
builds canonical-v2 rich snapshots at several worker counts and checks
convergence. With ``--mutations`` a batched five-class edit batch
(change/add/remove/rename/RFC+pressure) proves incremental == rebuild
including rich tables and invalidation sets.

Corpus tiers (see ``runner/mncs_index/ecosystem.py``): ``survey`` is the
legacy one-file-per-repo rule; ``family`` stages up to
``--max-files-per-repo`` alphabetically-first ``*.mncs`` per repo, each
at most ``--per-file-bytes`` bytes. The full ``census`` (every ``*.mncs``
in every sibling repo) is inventoried for exclusions/tiers but priced
beyond an in-session build (PRESS-010); its projected cost is reported
from measured rates.

Only *.mncs: the cross-repo MNCS source graph (module defines,
use-driven depends-on/references) at a byte volume real-kernel
execution can still cover in-session (PRESS-010: one subprocess per
kernel call). One process, one shared Kernels handle (pure memoization
caches, exactly like the session-scoped test fixture), so later worker
counts re-verify convergence; per-run call deltas come from the shared
bridge counter.

Usage:
    MNCS_BIN=... python3 scripts/run_ecosystem.py --out evidence/ecosystem.json [--workers 2,8]
    MNCS_BIN=... python3 scripts/run_ecosystem.py --tier family --max-files-per-repo 4 --per-file-bytes 4096 --mutations --out evidence/ecosystem-mncs-family.json --workers 1,2

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
from mncs_index.ecosystem import (  # noqa: E402
    TIERS,
    apply_mutation_batch,
    assert_tables_equal,
    census,
    economics,
    invalidation_roots,
    snapshot_metrics,
    stage_tiered,
    verify_invalidation_agreement,
)
from mncs_index.indexer import (  # noqa: E402
    build_snapshot_v2,
    incremental_snapshot_v2,
)
from mncs_index.kernels import Kernels  # noqa: E402
from mncs_index.pipeline import BuildConfig  # noqa: E402
from mncs_index.query import QueryEngine  # noqa: E402
from mncs_index.store import Store  # noqa: E402


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


def _build_stage(
    stage: str,
    generation: int,
    kernels: Kernels,
    workers: int,
    seed: int,
    bridge: Bridge,
) -> tuple:
    calls_before = bridge.stats.calls
    t0 = time.monotonic()
    snap, canon, corpus, _ = build_snapshot_v2(
        stage, generation, kernels,
        BuildConfig(workers=workers, seed=seed, mncs_digest=False),
    )
    dt = time.monotonic() - t0
    return snap, canon, corpus, snapshot_metrics(
        snap, corpus, dt, bridge.stats.calls - calls_before
    )


def _run_mutation_campaign(
    stage: str, kernels: Kernels, seed: int, bridge: Bridge
) -> dict:
    """Publish gen-0, apply the five-class batch, prove incremental.

    Returns the mutation report: expected-vs-actual verdicts, per-table
    row counts, invalidation roots, and the economics of the batch.
    Every traversal/query check uses the MNCS kernel verdict (with the
    host mirror cross-checked), so no meaning moves out of MNCS.
    """
    work = os.path.join(os.path.dirname(stage), ".ecosystem-mutation")
    if os.path.isdir(work):
        shutil.rmtree(work)
    os.makedirs(work)
    corpus_dir = os.path.join(work, "corpus")
    shutil.copytree(stage, corpus_dir)
    store_dir = os.path.join(work, "store")
    os.makedirs(store_dir)

    store = Store(store_dir)
    snap0, canon0, corpus0, m0 = _build_stage(
        corpus_dir, 0, kernels, 2, seed, bridge
    )
    store.publish(
        snap0, canon0,
        {d.path: 0 for d in snap0.docs},
        {it.path: it.crc for it in corpus0.items},
        None,
    )
    batch = apply_mutation_batch(corpus_dir)

    calls_before = bridge.stats.calls
    t0 = time.monotonic()
    prev, established, prev_crc = store.load_head()
    inc, inc_canon, _cc, verdicts = incremental_snapshot_v2(
        corpus_dir, 1, prev, prev_crc, kernels,
        BuildConfig(workers=2, seed=seed, mncs_digest=False),
    )
    inc_wall = time.monotonic() - t0
    inc_calls = bridge.stats.calls - calls_before
    for path, expect in batch["verdicts"].items():
        assert verdicts.get(path) == expect, (
            f"mutation verdict wrong at {path}: "
            f"{verdicts.get(path)} != {expect}"
        )

    calls_before = bridge.stats.calls
    t0 = time.monotonic()
    ref, _rc, _, _ = build_snapshot_v2(
        corpus_dir, 0, kernels,
        BuildConfig(workers=1, seed=seed + 1, mncs_digest=False),
    )
    ref_wall = time.monotonic() - t0
    ref_calls = bridge.stats.calls - calls_before

    rows = assert_tables_equal(inc, ref)
    inv_changed = sorted(
        set(batch["verdicts"]) | {batch["changed"], batch["removed"]}
    )
    roots = invalidation_roots(ref.rels, inv_changed)
    verify_invalidation_agreement(inc.rels, ref.rels, roots, kernels)

    # Query-level agreement on the edge shapes the batch exercises.
    eng_inc = QueryEngine(inc, kernels)
    eng_ref = QueryEngine(ref, kernels)
    assert eng_inc.depends_on("scale/app.mncs").total == 1
    assert (
        eng_inc.depends_on("scale/app.mncs").records
        == eng_ref.depends_on("scale/app.mncs").records
    )
    assert eng_inc.dependents("scale.dep").total == 1
    assert eng_inc.rfc_refs("0007").total >= 1
    assert eng_inc.pressure("PRESS-010").total >= 1
    assert (
        eng_inc.invalidation_set(["scale/dep.mncs"])
        == eng_ref.invalidation_set(["scale/dep.mncs"])
    )

    report = {
        "gen0": {"wall_s": m0["wall_s"], "mncs_calls": m0["mncs_calls"]},
        "batch": {k: v for k, v in batch.items() if k != "verdicts"},
        "verdicts": verdicts,
        "expected_verdicts": batch["verdicts"],
        "rows": rows,
        "invalidation_roots": roots,
        "incremental": {"wall_s": round(inc_wall, 2), "mncs_calls": inc_calls},
        "rebuild": {"wall_s": round(ref_wall, 2), "mncs_calls": ref_calls},
    }
    shutil.rmtree(work, ignore_errors=True)
    return report


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", default="8,2")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument(
        "--tier", default="survey", choices=("survey", "family"),
        help="corpus tier (default %(default)s)",
    )
    ap.add_argument("--max-files-per-repo", type=int, default=None)
    ap.add_argument("--per-file-bytes", type=int, default=None)
    ap.add_argument(
        "--mutations", action="store_true",
        help="run the five-class mutation batch (incremental == rebuild)",
    )
    args = ap.parse_args()

    if args.max_files_per_repo is None:
        args.max_files_per_repo = 1 if args.tier == "survey" else 4
    if args.per_file_bytes is None:
        args.per_file_bytes = 8192 if args.tier == "survey" else 4096

    projects = os.path.dirname(REPO)
    language_dir = os.path.join(projects, "mncs-language")
    stage = os.path.join(REPO, ".ecosystem-stage")
    print(
        f"[ecosystem] tier={args.tier} staging {projects}/mncs-* -> {stage}",
        flush=True,
    )
    full_inventory = census(projects)
    census_files = sum(len(v) for v in full_inventory.values())
    census_bytes = sum(s for v in full_inventory.values() for _, _, s in v)
    repos, excluded = stage_tiered(
        projects,
        stage,
        max_files_per_repo=args.max_files_per_repo,
        per_file_bytes=args.per_file_bytes,
    )
    total_files = sum(r["files"] for r in repos.values())
    total_bytes = sum(r["bytes"] for r in repos.values())
    print(
        f"[ecosystem] staged {len(repos)} repos, "
        f"{total_files} files, {total_bytes} bytes "
        f"(census {census_files} files, {census_bytes} bytes)",
        flush=True,
    )

    bridge = Bridge()
    kernels = Kernels(bridge)
    runs = []
    hashes = set()
    wall_total = 0.0
    try:
        for w in [int(x) for x in args.workers.split(",")]:
            print(f"[ecosystem] build workers={w} ...", flush=True)
            _snap, _canon, _corpus, m = _build_stage(
                stage, 0, kernels, w, args.seed, bridge
            )
            hashes.add(m["index_hash"])
            m["workers"] = w
            m["seed"] = args.seed
            runs.append(m)
            wall_total += m["wall_s"]
            print(
                f"[ecosystem] workers={w} done in {m['wall_s']}s "
                f"hash={m['index_hash'][:16]} calls={m['mncs_calls']}",
                flush=True,
            )
        mutation = None
        mutation_calls = 0
        if args.mutations:
            print("[ecosystem] mutation batch ...", flush=True)
            mutation = _run_mutation_campaign(stage, kernels, args.seed, bridge)
            wall_total += (
                mutation["gen0"]["wall_s"]
                + mutation["incremental"]["wall_s"]
                + mutation["rebuild"]["wall_s"]
            )
            mutation_calls = (
                mutation["gen0"]["mncs_calls"]
                + mutation["incremental"]["mncs_calls"]
                + mutation["rebuild"]["mncs_calls"]
            )
            print(
                "[ecosystem] mutation batch incremental==rebuild "
                f"rows={mutation['rows']} "
                f"roots={mutation['invalidation_roots']}",
                flush=True,
            )
    finally:
        shutil.rmtree(stage, ignore_errors=True)

    converged = len(hashes) == 1
    campaign_calls = sum(r["mncs_calls"] for r in runs) + mutation_calls
    record = {
        "tier": args.tier,
        "tiers": TIERS,
        "corpus": "multi-repo ecosystem: mncs-index + locally available mncs-* repos",
        "corpus_rule": (
            f"tier={args.tier}: up to max_files_per_repo="
            f"{args.max_files_per_repo} alphabetically-first **/*.mncs per "
            "repo (excluding .git/target/caches/build dirs and symlinks), "
            f"each at most per_file_bytes={args.per_file_bytes} bytes "
            "(0 = uncapped)"
        ),
        "census": {
            "files": census_files,
            "bytes": census_bytes,
            "note": (
                "inventoried, not executed: priced beyond an in-session "
                "build under the PRESS-010 subprocess-per-call model; see "
                "economics.projection below"
            ),
        },
        "repos": repos,
        "excluded": [
            {"repo": r, "path": p, "bytes": n, "reason": why}
            for r, p, n, why in excluded
        ],
        "total_files": total_files,
        "total_bytes": total_bytes,
        "runs": runs,
        "converged": converged,
        "mncs_calls": bridge.stats.calls,
        "max_in_flight": bridge.stats.max_in_flight,
        "economics": economics(
            campaign_calls, wall_total, total_files, total_bytes,
            census_files, census_bytes,
        ),
        "mncs_language": language_proof(
            language_dir, os.path.join(REPO, "mncs-language.lock.json")
        ),
        "note": (
            "Rich canonical-v2 snapshots: every verdict (digests, kinds, "
            "tokens, declarations, headings, links, PRESS/RFC shapes, "
            "traversal admission) is a real MNCS kernel call; the host "
            "moves bytes, bounds queues, and sorts by the specified key. "
            "One process, one shared Kernels handle (pure memoization, as "
            "in tests): later worker counts re-verify hash convergence. "
            "Per-run call counts are deltas off the shared bridge counter; "
            "hashes are canonical."
        ),
    }
    if mutation is not None:
        record["mutation"] = mutation
    with open(args.out, "w") as fh:
        json.dump(record, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(
        json.dumps(
            {
                "out": args.out,
                "converged": converged,
                "hash": next(iter(hashes)) if hashes else None,
                "calls": bridge.stats.calls,
            }
        ),
        flush=True,
    )
    return 0 if converged else 1


if __name__ == "__main__":
    raise SystemExit(main())
