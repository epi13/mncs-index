"""Scale campaign: tiered MNCS-family corpus + realistic mutation batch.

This module stages real ``*.mncs`` sources from sibling ``mncs-*``
checkouts into one corpus and applies a batched five-class mutation
(change / add / remove / rename / RFC+pressure edit), so both
``scripts/run_ecosystem.py`` (evidence) and ``tests/test_scale.py``
(bounded proof) share one implementation.

Corpus tiers (documented once, here):

- ``census``: every ``*.mncs`` file under each sibling ``mncs-*`` repo,
  excluding ``SKIP_DIRS`` entries and symlinks. Inventory only — the
  full 500+ file / ~2 MB census is priced by PRESS-010 (one subprocess
  per kernel call) beyond an in-session build, so the campaign executes
  the tiers below and projects census cost from measured rates.
- ``survey``: alphabetically-first ``*.mncs`` per repo (legacy
  evidence rule; tiny, kept for continuity).
- ``family``: up to ``max_files_per_repo`` alphabetically-first
  ``*.mncs`` per repo, each at most ``per_file_bytes`` bytes. The
  scale-convergence corpus: all MNCS-family content the session budget
  can actually execute. Every skip is recorded with its reason.
- ``mutation``: not a corpus by itself — the five-class edit batch
  applied to a staged ``family`` (or hermetic) corpus, verified as
  ``incremental == rebuild`` including rich tables and invalidation.

Pressure posture (PRESS-010): staging copies bytes only. Every verdict
— digests, kinds, tokens, change codes, declaration/heading/link/
PRESS/RFC shapes, traversal admission — is a real MNCS kernel call
through the standard ``build_snapshot_v2`` / ``incremental_snapshot_v2``
/ ``graph`` path. Call counts come from ``Bridge.stats``; caches are
the standard pure-function memoization (meaning-neutral).
"""

from __future__ import annotations

import os
import shutil

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

TIERS = {
    "census": (
        "every *.mncs under each sibling mncs-* repo "
        "(excluding SKIP_DIRS/symlinks); inventoried, not executed"
    ),
    "survey": "alphabetically-first *.mncs per repo (legacy evidence rule)",
    "family": (
        "up to max_files_per_repo alphabetically-first *.mncs per repo, "
        "each at most per_file_bytes bytes; the executed scale corpus"
    ),
    "mutation": (
        "five-class edit batch (change/add/remove/rename/RFC+pressure) "
        "on a staged corpus; incremental == rebuild incl. rich+invalidation"
    ),
}


def census(projects_dir: str) -> dict[str, list[tuple[str, str, int]]]:
    """Inventory every relevant ``*.mncs`` file per sibling repo.

    Returns ``{repo: [(relpath, fullpath, bytes), ...]}`` sorted by
    relative path. Only ``mncs-*`` directories count; ``SKIP_DIRS``
    entries, symlinks, and non-files never enter the inventory.
    """
    out: dict[str, list[tuple[str, str, int]]] = {}
    for repo in sorted(os.listdir(projects_dir)):
        root = os.path.join(projects_dir, repo)
        if not repo.startswith("mncs-") or not os.path.isdir(root):
            continue
        inventory: list[tuple[str, str, int]] = []
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
                inventory.append(
                    (os.path.relpath(full, root), full, os.path.getsize(full))
                )
        inventory.sort()
        out[repo] = inventory
    return out


def stage_tiered(
    projects_dir: str,
    stage: str,
    max_files_per_repo: int = 0,
    per_file_bytes: int = 0,
) -> tuple[dict, list]:
    """Stage the ``family`` tier: ``<stage>/<repo>/<relpath>`` copies.

    Caps of 0 mean uncapped. Per repo the census inventory is sorted by
    relative path and truncated to ``max_files_per_repo`` entries, and
    entries larger than ``per_file_bytes`` are skipped; every skip is
    returned in ``excluded`` as ``(repo, rel, bytes, reason)``.
    """
    if os.path.isdir(stage):
        shutil.rmtree(stage)
    os.makedirs(stage)
    repos: dict = {}
    excluded: list = []
    for repo, inventory in census(projects_dir).items():
        if max_files_per_repo and len(inventory) > max_files_per_repo:
            for rel, full, nbytes in inventory[max_files_per_repo:]:
                excluded.append((repo, rel, nbytes, "beyond max_files_per_repo"))
            inventory = inventory[:max_files_per_repo]
        files = 0
        size = 0
        for rel, full, nbytes in inventory:
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


def snapshot_metrics(
    snap, corpus, wall_s: float, calls: int
) -> dict:
    """Full per-run metrics: table counts, hash, cost. JSON-serializable."""
    return {
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
        "mncs_fingerprint": snap.mncs_fingerprint,
        "wall_s": round(wall_s, 2),
        "mncs_calls": calls,
    }


def tables_canon(snap) -> dict[str, list[str]]:
    """Canonical text of every snapshot table (v1 + rich)."""
    return {
        "docs": [d.canon_line() for d in snap.docs],
        "terms": [t.canon_line() for t in snap.terms],
        "syms": [r.canon_line() for r in snap.syms],
        "headings": [r.canon_line() for r in snap.headings],
        "rels": [r.canon_line() for r in snap.rels],
        "press": [r.canon_line() for r in snap.press],
    }


def assert_tables_equal(inc, ref) -> dict[str, int]:
    """Assert incremental and rebuild snapshots agree on every table.

    Returns row counts per table for the report. Raises AssertionError
    naming the first divergent table.
    """
    a, b = tables_canon(inc), tables_canon(ref)
    for table in ("docs", "terms", "syms", "headings", "rels", "press"):
        assert a[table] == b[table], (
            f"incremental != rebuild on {table}: "
            f"{len(a[table])} vs {len(b[table])} rows"
        )
    assert inc.index_hash == ref.index_hash, "index_hash diverged"
    return {table: len(rows) for table, rows in a.items()}


def _staged_mncs_files(stage: str) -> list[str]:
    """All staged ``*.mncs`` paths, sorted, relative to the stage root."""
    out: list[str] = []
    for dirpath, _dirnames, filenames in os.walk(stage):
        for name in filenames:
            if name.endswith(".mncs"):
                out.append(os.path.relpath(os.path.join(dirpath, name), stage))
    return sorted(out)


def apply_mutation_batch(stage: str) -> dict:
    """Apply one realistic edit of each of the five classes, batched.

    All edits land in a single generation so the campaign stays bounded;
    each class is independently verdict-checked by the caller:

    - ``change``: append a new ``fn`` declaration to the smallest staged
      ``.mncs`` file (developer adds a function; rich sym/rel rows grow).
    - ``add``: new ``scale/dep.mncs`` (defines ``scale.dep``) plus new
      ``scale/app.mncs`` (``use``es it) — a real depends-on edge.
    - ``remove``: delete the largest staged ``.mncs`` file (a depended-
      upon removal leaves dangling targets, which traversal treats as
      leaves — the shape real deletions produce).
    - ``rename``: byte-preserving rename of the second-smallest staged
      ``.mncs`` file (verdicts remove+add; content-identity lineage may
      report ``moved``).
    - ``rfc-press``: new ``scale/notes.md`` with a heading, an
      ``RFC 0007`` keyword+number pair, and a ``PRESS-010`` mention
      (heading + rfc-ref + press rows).

    Returns ``{"changed": ..., "added": [...], "removed": ...,
    "renamed": (old, new), "rfc_press": ..., "verdicts": {path: code}}``
    with the expected ``classify_change`` codes for the caller to assert.
    """
    staged = _staged_mncs_files(stage)
    assert len(staged) >= 3, (
        f"mutation batch needs >= 3 staged .mncs files, found {len(staged)}"
    )
    with open(os.path.join(stage, staged[0]), "ab") as fh:
        fh.write(b"\nfn scale_edited() -> (result: i64) {\n    return 1;\n}\n")
    changed = staged[0]

    os.makedirs(os.path.join(stage, "scale"), exist_ok=True)
    dep = os.path.join(stage, "scale", "dep.mncs")
    with open(dep, "wb") as fh:
        fh.write(
            b"mncs 0.8;\n\nmodule scale.dep;\n\n"
            b"fn dep_fn() -> (result: i64) {\n    return 7;\n}\n"
        )
    app = os.path.join(stage, "scale", "app.mncs")
    with open(app, "wb") as fh:
        fh.write(
            b"mncs 0.8;\n\nmodule scale.app;\n\nuse scale.dep as dep;\n\n"
            b"fn app_fn() -> (result: i64) {\n    return 0;\n}\n"
        )

    candidates = [p for p in staged if p != changed]
    removable = max(
        candidates, key=lambda p: os.path.getsize(os.path.join(stage, p))
    )
    os.remove(os.path.join(stage, removable))

    renamed_old = next(p for p in staged if p not in (changed, removable))
    renamed_new = renamed_old + ".renamed.mncs"
    os.rename(
        os.path.join(stage, renamed_old), os.path.join(stage, renamed_new)
    )

    notes = os.path.join(stage, "scale", "notes.md")
    with open(notes, "wb") as fh:
        fh.write(
            b"# Scale notes\n\nSee RFC 0007 and PRESS-010 for context.\n"
        )
    return {
        "changed": changed,
        "added": ["scale/app.mncs", "scale/dep.mncs"],
        "removed": removable,
        "renamed": (renamed_old, renamed_new),
        "rfc_press": "scale/notes.md",
        "verdicts": {
            changed: 3,
            "scale/app.mncs": 1,
            "scale/dep.mncs": 1,
            removable: 2,
            renamed_old: 2,
            renamed_new: 1,
            "scale/notes.md": 1,
        },
    }


def invalidation_roots(rels, changed: list[str], limit: int = 3) -> list[str]:
    """Changed paths plus up to ``limit`` highest fan-out files as roots."""
    from collections import Counter

    fanout = Counter(r.src for r in rels if r.rel == "depends-on")
    hubs = [src for src, _ in fanout.most_common(limit)]
    roots = list(dict.fromkeys(list(changed) + hubs))
    return roots


def verify_invalidation_agreement(inc_rels, ref_rels, roots, kernels) -> dict:
    """Pin traversal/invalidation agreement between two edge sets.

    For every root: forward closure, reverse closure, and invalidation
    sets agree between ``inc`` and ``ref`` under both the MNCS kernel
    verdict and the exact host mirror; kernel and mirror agree with
    each other on both snapshots. Returns the checked root list.
    """
    from .graph import (
        invalidation_set,
        traverse_dependencies,
        traverse_dependents,
    )

    for root in roots:
        for backend in (kernels, None):
            assert traverse_dependencies(
                inc_rels, root, backend
            ) == traverse_dependencies(ref_rels, root, backend), (
                f"dependencies diverged at {root}"
            )
            assert traverse_dependents(
                inc_rels, root, backend
            ) == traverse_dependents(ref_rels, root, backend), (
                f"dependents diverged at {root}"
            )
        assert invalidation_set(inc_rels, [root], kernels) == invalidation_set(
            ref_rels, [root], kernels
        ), f"invalidation diverged at {root}"
        assert invalidation_set(inc_rels, [root], None) == invalidation_set(
            inc_rels, [root], kernels
        ), f"kernel/mirror diverged at {root}"
    multi = roots[:2]
    assert invalidation_set(inc_rels, multi, kernels) == invalidation_set(
        ref_rels, multi, kernels
    ), "multi-root invalidation diverged"
    return {"roots": roots}


def economics(
    total_calls: int,
    total_wall_s: float,
    files: int,
    bytes_indexed: int,
    census_files: int = 0,
    census_bytes: int = 0,
) -> dict:
    """PRESS-010 economics from measured counters (never estimated).

    ``total_calls`` is the ``Bridge.stats.calls`` delta (one subprocess
    per call); ``total_wall_s`` is the summed build wall time. Rates
    price larger tiers; the census projection scales the measured
    calls/byte rate to the inventoried census bytes.
    """
    per_file = total_calls / files if files else 0.0
    per_kb = total_calls / (bytes_indexed / 1024) if bytes_indexed else 0.0
    out = {
        "mncs_calls": total_calls,
        "wall_s": round(total_wall_s, 2),
        "calls_per_file": round(per_file, 1),
        "calls_per_kb": round(per_kb, 1),
        "model": (
            "one mncs execute subprocess per kernel call "
            "(bridge.stats.calls); pure-function memoization only"
        ),
    }
    if census_bytes:
        out["projection"] = {
            "census_files": census_files,
            "census_bytes": census_bytes,
            "projected_calls": int(per_kb * census_bytes / 1024),
            "note": (
                "linear projection of the measured calls/KB rate to "
                "census bytes; vocabulary caches make this an upper "
                "bound, subprocess contention makes wall time "
                "superlinear — the census stays inventoried, not executed"
            ),
        }
    return out