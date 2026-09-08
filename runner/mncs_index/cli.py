"""mncs-index command line: build / query / watch over canonical snapshots."""

from __future__ import annotations

import argparse
import json
import sys

from .bridge import Bridge
from .indexer import (
    build_snapshot,
    build_snapshot_v2,
    incremental_snapshot,
    incremental_snapshot_v2,
)
from .kernels import KIND_NAMES, Kernels
from .lineage import resolve as resolve_lineage
from .model import hex16
from .pipeline import BuildConfig
from .query import QueryEngine
from .store import DURABILITY_DEFAULT, DURABILITY_LEVELS, Store
from .watch import Watcher


def _kernels(args) -> Kernels:
    bridge = Bridge(binary=args.mncs_bin, step_budget=args.step_budget)
    return Kernels(bridge)


def _config(args) -> BuildConfig:
    return BuildConfig(
        workers=args.workers,
        queue_size=args.queue_size,
        seed=args.seed,
        delay_ms=args.delay_ms,
    )


def cmd_check(args) -> int:
    """Recovery/check command (RFC 0008): report store health, optionally
    remove leftover staging files. Never guesses: only orphan temps are
    repaired; HEAD/generation corruption stays reported for an operator."""
    store = Store(args.store)
    report = store.check()
    repaired: list = []
    if args.repair:
        repaired = store.repair()
        report = store.check()
        report["repaired"] = repaired
    print(json.dumps(report, indent=1))
    return 0 if report["ok"] else 1


def cmd_reclaim(args) -> int:
    """Reclamation command (RFC 0009): delete only unpinned,
    non-HEAD generations below HEAD (stale pins reaped first).
    Never deletes HEAD, newer-than-HEAD artifacts, staging files,
    or pins. Prints the JSON report; exit 0 always (reclamation
    reports, it does not diagnose — use `check` for health)."""
    store = Store(args.store)
    report = store.reclaim(keep_recent=args.keep_recent)
    print(json.dumps(report, indent=1))
    return 0


def cmd_compact(args) -> int:
    """Compaction command (RFC 0010): deterministic in-place GC.

    Deletes only schedule-selected unpinned generations below HEAD
    (never HEAD, pinned generations, or newer-than-HEAD artifacts),
    plus orphan staging tmps and superseded sidecars. Prints the
    JSON report+metrics; exit 0 always (compaction reports, it does
    not diagnose — use `check` for health)."""
    store = Store(args.store)
    report = store.compact(schedule=args.schedule, dry_run=args.dry_run)
    print(json.dumps(report, indent=1))
    return 0


def cmd_build(args) -> int:
    kernels = _kernels(args)
    store = Store(args.store, durability=args.durability)
    head = store.head()
    rich = bool(getattr(args, "rich", False))
    if args.incremental and head is not None:
        prev, established, prev_crc = store.load_head()
        gen = head + 1
        if rich:
            snap, canon, corpus, verdicts = incremental_snapshot_v2(
                args.corpus, gen, prev, prev_crc, kernels, _config(args)
            )
        else:
            snap, canon, corpus, verdicts = incremental_snapshot(
                args.corpus, gen, prev, prev_crc, kernels, _config(args)
            )
        estat = dict(established)
        for d in snap.docs:
            estat.setdefault(d.path, gen)
        store.publish(
            snap, canon, estat, {it.path: it.crc for it in corpus.items}, head
        )
        removed = [
            (d.path, d.digest) for d in prev.docs if verdicts.get(d.path) == 2
        ]
        added = [(d.path, d.digest) for d in snap.docs if verdicts.get(d.path) == 1]
        lineage = [
            {
                "old": item.old,
                "new": item.new,
                "kind": item.kind,
                "digest": hex16(item.digest),
            }
            for item in resolve_lineage(removed, added)
        ]
        print(
            json.dumps(
                {
                    "generation": gen,
                    "mode": "incremental",
                    "verdicts": verdicts,
                    "lineage": lineage,
                    "index_hash": snap.index_hash,
                    "mncs_fingerprint": snap.mncs_fingerprint,
                }
            )
        )
    else:
        gen = 0 if head is None else head + 1
        if rich:
            snap, canon, corpus, _bridge = build_snapshot_v2(
                args.corpus, gen, kernels, _config(args)
            )
        else:
            snap, canon, corpus, _bridge = build_snapshot(
                args.corpus, gen, kernels, _config(args)
            )
        store.publish(
            snap,
            canon,
            {d.path: gen for d in snap.docs},
            {it.path: it.crc for it in corpus.items},
            head,
        )
        print(
            json.dumps(
                {
                    "generation": gen,
                    "mode": "full",
                    "files": len(corpus.items),
                    "skipped": corpus.skipped,
                    "index_hash": snap.index_hash,
                    "mncs_fingerprint": snap.mncs_fingerprint,
                }
            )
        )
    print(
        f"mncs calls={kernels.b.stats.calls} "
        f"max_in_flight={kernels.b.stats.max_in_flight}",
        file=sys.stderr,
    )
    return 0


def _rich_record_json(rec) -> dict:
    cls = type(rec).__name__
    if cls == "SymRecord":
        return {
            "record": "sym",
            "path": rec.path,
            "sym": rec.sym,
            "name": rec.name,
            "ndigest": hex16(rec.ndigest),
            "seq": rec.seq,
        }
    if cls == "HeadingRecord":
        return {
            "record": "heading",
            "path": rec.path,
            "level": rec.level,
            "seq": rec.seq,
            "tdigest": hex16(rec.tdigest),
            "title": rec.title,
        }
    if cls == "RelRecord":
        return {
            "record": "rel",
            "src": rec.src,
            "rel": rec.rel,
            "dst": rec.dst,
            "seq": rec.seq,
        }
    if cls == "PressRecord":
        return {"record": "press", "path": rec.path, "pid": rec.pid, "seq": rec.seq}
    return {
        "kind": KIND_NAMES.get(rec.kind, rec.kind),
        "path": rec.path,
        "digest": hex16(rec.digest),
        "size": rec.size,
        "words": rec.words,
        "lines": rec.lines,
    }


def cmd_query(args) -> int:
    kernels = _kernels(args)
    store = Store(args.store)
    loaded = store.load_head()
    if loaded[0] is None:
        print("empty store", file=sys.stderr)
        return 1
    snap, established, _ = loaded
    engine = QueryEngine(snap, kernels, workers=args.workers, established=established)
    if getattr(args, "defines", None):
        res = engine.defines(args.defines, args.limit)
    elif getattr(args, "references", None):
        res = engine.references(args.references, args.limit)
    elif getattr(args, "depends_on", None):
        res = engine.depends_on(args.depends_on, args.limit)
    elif getattr(args, "dependents", None):
        res = engine.dependents(args.dependents, args.limit)
    elif getattr(args, "rfc", None):
        res = engine.rfc_refs(args.rfc, args.limit)
    elif getattr(args, "pressure", None):
        res = engine.pressure(args.pressure, args.limit)
    elif getattr(args, "symbol", None):
        res = engine.symbols(args.symbol, args.limit)
    elif getattr(args, "headings", None):
        res = engine.headings_for(args.headings, args.limit)
    elif getattr(args, "provenance", None):
        print(json.dumps(engine.provenance(args.provenance), indent=1))
        return 0
    elif args.digest:
        res = engine.by_digest_query(int(args.digest, 16), args.limit)
    elif args.path:
        res = engine.by_path(args.path, args.limit)
    elif args.kind is not None:
        res = engine.by_kind(int(args.kind), args.limit)
    elif args.term:
        res = engine.term_substring(args.term, args.limit)
    else:
        res = engine._finish(list(engine.path_index.values()), args.limit)
    out = {
        "snapshot": res.snapshot_id[:16],
        "generation": res.generation,
        "total": res.total,
        "limited": res.limited,
        "records": [_rich_record_json(d) for d in res.records],
    }
    print(json.dumps(out, indent=1))
    return 0


def cmd_watch(args) -> int:
    kernels = _kernels(args)
    store = Store(args.store)
    watcher = Watcher(
        args.corpus,
        store,
        kernels,
        _config(args),
        interval_s=args.interval,
        quiet_polls=args.quiet_polls,
        rich=bool(getattr(args, "rich", False)),
        validate_every=getattr(args, "validate_every", 4),
    )
    try:
        published = watcher.run(max_generations=args.generations)
    except KeyboardInterrupt:
        published = []
    print(json.dumps({"published": published, "events": watcher.events}, indent=1))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="mncs-index", description=__doc__)
    p.add_argument("--mncs-bin", default=None)
    p.add_argument("--step-budget", type=int, default=100000)
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build")
    b.add_argument("--corpus", required=True)
    b.add_argument("--store", required=True)
    b.add_argument("--workers", type=int, default=4)
    b.add_argument("--queue-size", type=int, default=64)
    b.add_argument("--seed", type=int, default=None)
    b.add_argument("--delay-ms", type=float, default=0.0)
    b.add_argument("--incremental", action="store_true")
    b.add_argument(
        "--durability",
        choices=list(DURABILITY_LEVELS),
        default=DURABILITY_DEFAULT,
        help="commit durability level (RFC 0008; default %(default)s)",
    )
    b.add_argument(
        "--rich",
        action="store_true",
        help="build canonical-v2 (source/heading/reference/pressure records)",
    )
    b.set_defaults(func=cmd_build)

    q = sub.add_parser("query")
    q.add_argument("--store", required=True)
    q.add_argument("--workers", type=int, default=4)
    q.add_argument("--digest", default=None)
    q.add_argument("--path", default=None)
    q.add_argument("--kind", default=None)
    q.add_argument("--term", default=None)
    q.add_argument("--defines", default=None, help="who defines symbol NAME (v2)")
    q.add_argument("--references", default=None, help="references to DST (v2)")
    q.add_argument("--depends-on", default=None, help="depends-on edges of PATH (v2)")
    q.add_argument("--dependents", default=None, help="who depends on DST (v2)")
    q.add_argument("--rfc", default=None, help="rfc-ref edges for NUMBER (v2)")
    q.add_argument("--pressure", default=None, help="mentions of PRESS-ID (v2)")
    q.add_argument("--symbol", default=None, help="declarations of NAME (v2)")
    q.add_argument("--headings", default=None, help="headings of PATH (v2)")
    q.add_argument("--provenance", default=None, help="provenance of PATH")
    q.add_argument("--limit", type=int, default=None)
    q.set_defaults(func=cmd_query)

    w = sub.add_parser("watch")
    w.add_argument("--corpus", required=True)
    w.add_argument("--store", required=True)
    w.add_argument("--workers", type=int, default=4)
    w.add_argument("--queue-size", type=int, default=64)
    w.add_argument("--seed", type=int, default=None)
    w.add_argument("--delay-ms", type=float, default=0.0)
    w.add_argument("--interval", type=float, default=0.5)
    w.add_argument("--quiet-polls", type=int, default=2)
    w.add_argument("--generations", type=int, default=None)
    w.add_argument(
        "--rich",
        action="store_true",
        help="watch a canonical-v2 store (keep rich tables)",
    )
    w.add_argument(
        "--validate-every",
        type=int,
        default=4,
        help="authoritative revalidation period in quiet windows",
    )
    w.set_defaults(func=cmd_watch)

    c = sub.add_parser("check", help="check store health / recover (RFC 0008)")
    c.add_argument("--store", required=True)
    c.add_argument(
        "--repair",
        action="store_true",
        help="remove leftover staging files only; HEAD/generation "
        "corruption is never auto-repaired",
    )
    c.set_defaults(func=cmd_check)

    r = sub.add_parser("reclaim", help="reclaim unpinned generations (RFC 0009)")
    r.add_argument("--store", required=True)
    r.add_argument(
        "--keep-recent",
        type=int,
        default=0,
        help="retain this many newest otherwise reclaimable generations",
    )
    r.set_defaults(func=cmd_reclaim)

    k = sub.add_parser("compact", help="deterministic in-place compaction (RFC 0010)")
    k.add_argument("--store", required=True)
    k.add_argument(
        "--schedule",
        default="prune",
        help="retention schedule: prune | keep-recent:N | checkpoint:N "
        "(default %(default)s)",
    )
    k.add_argument(
        "--dry-run",
        action="store_true",
        help="compute victims and projected metrics without deleting anything",
    )
    k.set_defaults(func=cmd_compact)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
