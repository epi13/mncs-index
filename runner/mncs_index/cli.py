"""mncs-index command line: build / query / watch over canonical snapshots."""

from __future__ import annotations

import argparse
import json
import sys

from .bridge import Bridge
from .indexer import build_snapshot, incremental_snapshot
from .kernels import KIND_NAMES, Kernels
from .model import hex16
from .pipeline import BuildConfig
from .query import QueryEngine
from .store import Store
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


def cmd_build(args) -> int:
    kernels = _kernels(args)
    store = Store(args.store)
    head = store.head()
    if args.incremental and head is not None:
        prev, established, prev_crc = store.load_head()
        gen = head + 1
        snap, canon, corpus, verdicts = incremental_snapshot(
            args.corpus, gen, prev, prev_crc, kernels, _config(args)
        )
        estat = dict(established)
        for d in snap.docs:
            estat.setdefault(d.path, gen)
        store.publish(snap, canon, estat, {it.path: it.crc for it in corpus.items})
        print(
            json.dumps(
                {
                    "generation": gen,
                    "mode": "incremental",
                    "verdicts": verdicts,
                    "index_hash": snap.index_hash,
                    "mncs_fingerprint": snap.mncs_fingerprint,
                }
            )
        )
    else:
        gen = 0 if head is None else head + 1
        snap, canon, corpus, _bridge = build_snapshot(
            args.corpus, gen, kernels, _config(args)
        )
        store.publish(
            snap,
            canon,
            {d.path: gen for d in snap.docs},
            {it.path: it.crc for it in corpus.items},
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


def cmd_query(args) -> int:
    kernels = _kernels(args)
    store = Store(args.store)
    loaded = store.load_head()
    if loaded[0] is None:
        print("empty store", file=sys.stderr)
        return 1
    snap, _, _ = loaded
    engine = QueryEngine(snap, kernels, workers=args.workers)
    if args.digest:
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
        "records": [
            {
                "kind": KIND_NAMES.get(d.kind, d.kind),
                "path": d.path,
                "digest": hex16(d.digest),
                "size": d.size,
                "words": d.words,
                "lines": d.lines,
            }
            for d in res.records
        ],
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
    b.set_defaults(func=cmd_build)

    q = sub.add_parser("query")
    q.add_argument("--store", required=True)
    q.add_argument("--workers", type=int, default=4)
    q.add_argument("--digest", default=None)
    q.add_argument("--path", default=None)
    q.add_argument("--kind", default=None)
    q.add_argument("--term", default=None)
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
    w.set_defaults(func=cmd_watch)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
