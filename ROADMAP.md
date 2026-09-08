# mncs-index Roadmap

The roadmap is intentionally proof-driven. A phase advances only when its invariants are demonstrated, not merely when APIs exist.

## Phase 0 — Foundation

- [x] project charter
- [x] deterministic concurrency RFC
- [x] canonical data model RFC
- [x] query model RFC
- [x] incremental/watch RFC
- [x] language-pressure methodology
- [x] integration boundaries
- [x] executable MNCS project skeleton (kernels + thin runner + CLI)
- [x] repository-native conformance command (`pytest tests/`; `mncs-index build/query/watch`)

## Phase 1 — Deterministic corpus index

Goal: prove a local fixture corpus can be indexed concurrently with canonical output independent of worker count and scheduling.

- [x] deterministic file discovery snapshot (`discover.py`, `snapshot_id`)
- [x] bounded discovery-to-parse queue (`queue.Queue(maxsize=...)`, backpressure tested)
- [x] concurrent parsing workers (read pool + kernel pool, `max_in_flight` evidence)
- [x] canonical record normalization (MNCS kernels)
- [x] ordered deterministic merge (MNCS-specified rule + differential tests)
- [x] canonical serialization and hash (canonical bytes + SHA-256 + MNCS fold)
- [x] worker-count equivalence matrix (1/2/4/8/16/32, fixtures + stress)
- [x] randomized scheduling pressure (seed shuffle, delay injection, queue sizes)
- [x] cancellation and graceful shutdown — HOST-COOPERATIVE ONLY (drain protocol, cancel tests; `threading.Event` checked between items, in-flight `mncs execute` calls run to completion bounded by the 60 s bridge timeout; no language deadlines/preemption — PRESS-012)
- [x] language-pressure evidence (12 registry entries + 3 reproducers)

Exit criterion: the same corpus/configuration produces the same canonical hash under repeated runs and materially different concurrency configurations. **Met** (`test_determinism.py`, `test_stress.py`).

## Phase 2 — Incremental indexing

- [x] content-addressed source identity (Merkle file digests, MNCS-computed)
- [x] changed-input recomputation (hint triage + MNCS `classify_change`)
- [x] deletion/tombstone handling (verdict 2, records dropped)
- [x] durable single-host publication (validate + tmp/rename HEAD swap + file/dir fsync levels L0–L3 + `flock` cross-process CAS with `StaleGenerationError` + `check` recovery — RFC 0008; single-host POSIX, Windows dir-sync best-effort; OS-crash survival by protocol construction, suite proves barrier issuance + process death only; NO language transaction effect — PRESS-013/016 stay open)
- [x] stale-work suppression (per-run assembly; no cross-generation writes)
- [x] event coalescing (watcher quiet-period; hints never become truth)
- [x] rebuild-vs-incremental equivalence tests (add/remove/change/rename/multi/chained)
- [x] dependency/invalidation graph — HOST-SIDE bounded traversal prototype (v2 records+relationships incrementally converge to clean rebuild across add/remove/rename/disappear/fan-out/cycles — `tests/test_graph_invalidation.py`; bounded transitive sets with a GLOBAL node budget via `graph.py`, per-candidate admission verdicts in `src/graph.mncs` — `tests/test_invalidation.py`; kernel owns only scalar admit/depth checks while adjacency/visited/order/traversal are host structures, language relation values still missing — PRESS-015 stays open)
- [x] rename lineage as advisory move (verdicts stay remove+add; provable 1:1 content-identity links report `moved`, ambiguity resolves to remove+add — `runner/mncs_index/lineage.py`, `build --incremental` `lineage` output)

Exit criterion: an incrementally updated index is canonically equivalent to a clean rebuild from the resulting snapshot. **Met** (`test_incremental.py`).

## Phase 3 — Query engine

- [x] exact identity lookup (digest)
- [x] field filtering (kind, path)
- [x] provenance lookup (snapshot id + generation on every result)
- [x] deterministic ranking/order contract (canonical order, tested)
- [x] snapshot-consistent concurrent queries (one snapshot per engine; parallel MNCS predicate eval)
- [x] query result limits with explicit `limited` flag (result-count budgets only; host-enforced — time budgets and language cancellation not yet — PRESS-012)
- [x] relationship traversal (single-hop v2 edges: defines/references/depends-on/rfc-ref; HOST-SIDE bounded transitive closure + global-budget invalidation sets with kernel admit verdicts — `tests/test_invalidation.py`; no transitive queries in MNCS — PRESS-015 stays open)
- [x] diagnostic/pressure lookup (v2 press records + `pressure` query)

## Phase 4 — MNCS ecosystem ingestion

- [x] first real-corpus evidence (mncs-language library + self-corpus; see `evidence/`)
- [x] multi-repo ecosystem index run — CONVERGENCE SAMPLE, not a census (mncs-index + all locally available mncs-* repos but ONE alphabetically-first `*.mncs` file per repo, 2 worker counts converged; see `evidence/ecosystem-mncs-repos.json`, `scripts/run_ecosystem.py`)
- [x] family-tier scale campaign — BOUNDED sample with priced bottleneck (2 files per repo, 2 KB truncation, 18 files/14 KB, rich extraction, multi worker-config convergence, five-class mutation batch proving incremental == rebuild incl. rich tables + invalidation; loaded-box workers=1 BEAT workers=4 — anti-scale priced as PRESS-010, not a load proof; 554-file census inventoried but never executed at ~804k projected calls; `tests/test_scale.py`, `evidence/ecosystem-mncs-family.json`)
- [x] source/compiler records (v2 sym records: module/fn/record/use via `src/extract.mncs` — RFC 0007)
- [x] RFC/document records beyond file-level (v2 heading + reference records)
- [ ] tests and diagnostics as record kinds
- [x] language-pressure findings as records (v2 press records + PRESS-014/015)
- [ ] git/project metadata adapters
- [ ] harness/CI evidence adapters
- [ ] integration with `mncs-ingest`
- [ ] integration with `mncs-memory`

## Phase 5 — Distributed pressure

After local concurrency semantics are stable:

- [ ] partition work through `mncs-fabric`
- [ ] heterogeneous worker execution
- [ ] deterministic distributed merge
- [ ] worker loss/retry semantics
- [ ] remote cancellation
- [ ] cross-machine reproducibility

The distributed phase must not weaken the local determinism invariant.

> Slice-6 evaluation 2026-09-08 (`index/phase3-systems`): **BLOCKED —
> evaluated, not implemented.** The slice-5 scale audit found no
> practical infra to distribute over (local fan-out already anti-scales:
> loaded-box workers=1 beat workers=4, 103 s vs 308 s hermetic; campaign
> 5488 calls / 1466 s for 18 files; 396.7 calls/KB projects the 554-file
> census at ~804k calls, so it stays inventoried). Missing, with exact
> boundaries: partition/work-unit transport (`bridge.call` is a local
> subprocess driver only), distributed merge wire format (merge is
> shared-memory: `results_to_records` + `globalize` + `tree_combine`),
> and retry/duplicate/reorder/loss semantics (pipeline is fail-fast
> only). PRESS-010 must land first. No distribution code was added and
> none faked. Gate record: `evidence/fabric-scale-gate.json`;
> capability gap: PRESS-019.

## Next language work (for the `mncs-language` campaign)

Ranked by unblock value for this project (slice-7 revision 2026-09-08;
full tiered handoff with host-deletion map in
`docs/LANGUAGE_HANDOFF.md`):

1. PRESS-010 — in-process or batch kernel invocation (stress economics; blocks PRESS-019).
2. PRESS-001/002 — threads/tasks + bounded channels with close semantics.
3. PRESS-003 — effect-gated filesystem traversal/read.
4. PRESS-006 — cryptographic digest primitive (or explicit non-goal).
5. PRESS-005 — `u64` traversal domains; unbounded-text story.
6. ~~PRESS-004 — integer bitwise operators~~ RESOLVED upstream (stage-b1, slice 7).
7. ~~Nested `iterate` (PRESS-009)~~ RESOLVED for two-level nests at profile 0.11 (slice 7).
