#!/usr/bin/env bash
# Language-probe runner for PRESS-001/003/006/007 (plan slice 4).
#
# Runs the smallest legal .mncs programs for each granted capability and
# checks the recorded verdicts. Requires the mncs-language executor:
#   MNCS_BIN (default: ../mncs-language/target/debug/mncs)
#   MNCS_LIBRARY_PATH (default: ../mncs-language/library)
#
# Exact commands, outputs, and costs measured 2026-09-07 are recorded in
# pressure/registry.md (PRESS-001/002/003/006/007 slice-4 notes).
set -euo pipefail

REPRO="$(cd "$(dirname "$0")" && pwd)"
MNCS_BIN="${MNCS_BIN:-$(cd "$REPRO/../../.." && pwd)/mncs-language/target/debug/mncs}"
export MNCS_LIBRARY_PATH="${MNCS_LIBRARY_PATH:-$(cd "$REPRO/../../.." && pwd)/mncs-language/library}"
BACKEND=mncs-research-bytecode

check() { # check <result.json> <case-id> <expected-status>
  python3 - "$1" "$2" "$3" <<'EOF'
import json, sys
result, want_id, want_status = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(result))
cases = [c for c in d["cases"] if c["case_id"] == want_id]
assert len(cases) == 1, f"expected one case {want_id}: {d['cases']}"
c = cases[0]
assert c["status"] == want_status, f"{want_id}: status {c['status']}"
assert c.get("expectation_met") is True, f"{want_id}: expectation unmet: {c}"
assert c.get("effects_met") is True, f"{want_id}: effects unmet: {c}"
print(f"  {want_id}: status={c['status']} steps={c.get('steps')} returned={json.dumps(c.get('returned'))[:100]}")
EOF
}

echo "== probe host-read (PRESS-003): granted blob_read =="
time "$MNCS_BIN" experiment run "$REPRO/probe-host-read.mncs" \
  --backend "$BACKEND" --corpus "$REPRO/probe-host-read-corpus.json" \
  --grant-read "probe_reader=$REPRO/probe-host-read-grant.txt" \
  > /tmp/probe-host-read.json
check /tmp/probe-host-read.json blob-len returned

echo "== probe sha256-abc (PRESS-006): granted sha256_digest, NIST vector =="
time "$MNCS_BIN" experiment run "$REPRO/probe-sha256-abc.mncs" \
  --backend "$BACKEND" --corpus "$REPRO/probe-sha256-abc-corpus.json" \
  --grant-crypto verifier \
  > /tmp/probe-sha256-abc.json
check /tmp/probe-sha256-abc.json digest-abc returned

echo "== probe clock (PRESS-007): granted clock_read, relational only =="
time "$MNCS_BIN" experiment run "$REPRO/probe-clock.mncs" \
  --backend "$BACKEND" --corpus "$REPRO/probe-clock-corpus.json" \
  --grant-time ticker \
  > /tmp/probe-clock.json
check /tmp/probe-clock.json past-expired returned

echo "== probe task-lifecycle (PRESS-001): pure execute, no grants =="
run_execute() { # run_execute <function> <args-json> <expect-json>
  req=$(mktemp /tmp/probe-task-req.XXXXXX.json)
  printf '{"schema_version":"0.1","target":{"module":"pressure.probe_task_lifecycle","function":"%s"},"arguments":%s,"step_budget":100000}' \
    "$1" "$2" > "$req"
  time "$MNCS_BIN" execute "$REPRO/probe-task-lifecycle.mncs" "$req" > /tmp/probe-task.json
  rm -f "$req"
  python3 - /tmp/probe-task.json "$3" <<'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
assert d["status"] == "returned", d
assert d["returned"] == json.loads(sys.argv[2]), d["returned"]
print(f"  {d['target']['function']}: returned={json.dumps(d['returned'])} steps={d.get('steps')}")
EOF
}
run_execute lifecycle_done '[]' '[{"boolean": {"value": true}}]'
run_execute cancel_terminal '[]' '[{"boolean": {"value": true}}]'
run_execute invalid_rejected '[]' '[{"integer": {"value": 0, "type": {"bits": 64, "signed": true}}}]'

echo "== fail-closed negatives (no grant -> no value) =="
neg() { # neg <label> <result.json>
  python3 - "$2" "$1" <<'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
bad = [c for c in d["cases"] if c["status"] == "returned" and c.get("expectation_met")]
assert not bad, f"{sys.argv[2]}: ungranted call produced a value: {bad}"
print(f"  {sys.argv[2]}: fails closed (no granted value)")
EOF
}
"$MNCS_BIN" experiment run "$REPRO/probe-host-read.mncs" \
  --backend "$BACKEND" --corpus "$REPRO/probe-host-read-corpus.json" \
  > /tmp/probe-host-read-neg.json 2>/dev/null || true
neg host-read /tmp/probe-host-read-neg.json
"$MNCS_BIN" experiment run "$REPRO/probe-sha256-abc.mncs" \
  --backend "$BACKEND" --corpus "$REPRO/probe-sha256-abc-corpus.json" \
  > /tmp/probe-sha256-neg.json 2>/dev/null || true
neg sha256-abc /tmp/probe-sha256-neg.json
"$MNCS_BIN" experiment run "$REPRO/probe-clock.mncs" \
  --backend "$BACKEND" --corpus "$REPRO/probe-clock-corpus.json" \
  > /tmp/probe-clock-neg.json 2>/dev/null || true
neg clock /tmp/probe-clock-neg.json

echo "ALL PROBES GREEN"
