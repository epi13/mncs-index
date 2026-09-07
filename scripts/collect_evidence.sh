#!/usr/bin/env bash
# Record a worker-matrix evidence file for one corpus.
set -euo pipefail

CORPUS=""
OUT=""
WORKERS="4,8,16"

while [ $# -gt 0 ]; do
  case "$1" in
    --corpus) CORPUS="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[ -n "$CORPUS" ] && [ -n "$OUT" ] || { echo "usage: $0 --corpus DIR --out FILE [--workers 4,8,16]" >&2; exit 2; }

REPO="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="$REPO/runner"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

python3 - "$CORPUS" "$OUT" "$WORKERS" "$TMP" <<'EOF'
import json, sys, time
sys.path.insert(0, "runner")
from mncs_index.bridge import Bridge
from mncs_index.kernels import Kernels
from mncs_index.indexer import build_snapshot
from mncs_index.pipeline import BuildConfig

corpus_dir, out_path, workers_arg, tmp = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
bridge = Bridge()
kernels = Kernels(bridge)
record = {"corpus": corpus_dir, "runs": []}
hashes = set()
for w in [int(x) for x in workers_arg.split(",")]:
    store = f"{tmp}/store-{w}"
    t0 = time.monotonic()
    snap, canon, corpus, _ = build_snapshot(
        corpus_dir, 0, kernels, BuildConfig(workers=w, seed=7, mncs_digest=False))
    dt = time.monotonic() - t0
    hashes.add(snap.index_hash)
    record["runs"].append({
        "workers": w,
        "files": len(corpus.items),
        "docs": len(snap.docs),
        "terms": len(snap.terms),
        "bytes_indexed": sum(d.size for d in snap.docs),
        "index_hash": snap.index_hash,
        "wall_s": round(dt, 2),
    })
record["converged"] = len(hashes) == 1
record["mncs_calls"] = bridge.stats.calls
record["max_in_flight"] = bridge.stats.max_in_flight
with open(out_path, "w") as fh:
    json.dump(record, fh, indent=1)
print(json.dumps({"out": out_path, "converged": record["converged"],
                  "hash": next(iter(hashes)), "calls": bridge.stats.calls}))
EOF
