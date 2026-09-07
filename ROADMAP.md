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
- [x] cancellation and graceful shutdown (drain protocol, cancel tests)
- [x] language-pressure evidence (12 registry entries + 3 reproducers)

Exit criterion: the same corpus/configuration produces the same canonical hash under repeated runs and materially different concurrency configurations. **Met** (`test_determinism.py`, `test_stress.py`).

## Phase 2 — Incremental indexing

- [x] content-addressed source identity (Merkle file digests, MNCS-computed)
- [x] changed-input recomputation (hint triage + MNCS `classify_change`)
- [x] deletion/tombstone handling (verdict 2, records dropped)
- [x] atomic snapshot publication (validate + tmp/rename HEAD swap)
- [x] stale-work suppression (per-run assembly; no cross-generation writes)
- [x] event coalescing (watcher quiet-period; hints never become truth)
- [x] rebuild-vs-incremental equivalence tests (add/remove/change/rename/multi/chained)
- [x] dependency/invalidation graph (v2 records+relationships incrementally converge to clean rebuild across add/remove/rename/disappear/fan-out/cycles — `tests/test_graph_invalidation.py`; no transitive sets — PRESS-015)
- [x] rename lineage as advisory move (verdicts stay remove+add; provable 1:1 content-identity links report `moved`, ambiguity resolves to remove+add — `runner/mncs_index/lineage.py`, `build --incremental` `lineage` output)

Exit criterion: an incrementally updated index is canonically equivalent to a clean rebuild from the resulting snapshot. **Met** (`test_incremental.py`).

## Phase 3 — Query engine

- [x] exact identity lookup (digest)
- [x] field filtering (kind, path)
- [x] provenance lookup (snapshot id + generation on every result)
- [x] deterministic ranking/order contract (canonical order, tested)
- [x] snapshot-consistent concurrent queries (one snapshot per engine; parallel MNCS predicate eval)
- [x] query cancellation/budgets (result limits with explicit `limited` flag; time budgets not yet)
- [x] relationship traversal (single-hop v2 edges: defines/references/depends-on/rfc-ref; transitive closure not offered — PRESS-015)
- [x] diagnostic/pressure lookup (v2 press records + `pressure` query)

## Phase 4 — MNCS ecosystem ingestion

- [x] first real-corpus evidence (mncs-language library + self-corpus; see `evidence/`)
- [x] multi-repo ecosystem index run (mncs-index + all locally available mncs-* repos, `*.mncs` source graph, 2+ worker counts converged; see `evidence/ecosystem-mncs-repos.json`, `scripts/run_ecosystem.py`)
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

## Next language work (for the `mncs-language` campaign)

Ranked by unblock value for this project:

1. PRESS-010 — in-process or batch kernel invocation (stress economics).
2. PRESS-001/002 — threads/tasks + bounded channels with close semantics.
3. PRESS-003 — effect-gated filesystem traversal/read.
4. PRESS-006 — cryptographic digest primitive (or explicit non-goal).
5. PRESS-004 — integer bitwise operators.
6. PRESS-005 — `u64` traversal domains; unbounded-text story.
