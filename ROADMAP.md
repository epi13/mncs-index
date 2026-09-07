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
- [ ] executable MNCS project skeleton
- [ ] repository-native conformance command

## Phase 1 — Deterministic corpus index

Goal: prove a local fixture corpus can be indexed concurrently with canonical output independent of worker count and scheduling.

- [ ] deterministic file discovery snapshot
- [ ] bounded discovery-to-parse queue
- [ ] concurrent parsing workers
- [ ] canonical record normalization
- [ ] ordered deterministic merge
- [ ] canonical serialization and hash
- [ ] worker-count equivalence matrix
- [ ] randomized scheduling pressure
- [ ] cancellation and graceful shutdown
- [ ] language-pressure evidence

Exit criterion: the same corpus/configuration produces the same canonical hash under repeated runs and materially different concurrency configurations.

## Phase 2 — Incremental indexing

- [ ] content-addressed source identity
- [ ] dependency/invalidation graph
- [ ] changed-input recomputation
- [ ] deletion/tombstone handling
- [ ] atomic snapshot publication
- [ ] stale-work suppression
- [ ] event coalescing
- [ ] rebuild-vs-incremental equivalence tests

Exit criterion: an incrementally updated index is canonically equivalent to a clean rebuild from the resulting snapshot.

## Phase 3 — Query engine

- [ ] exact identity lookup
- [ ] field filtering
- [ ] relationship traversal
- [ ] provenance lookup
- [ ] deterministic ranking/order contract
- [ ] snapshot-consistent concurrent queries
- [ ] query cancellation/budgets

## Phase 4 — MNCS ecosystem ingestion

- [ ] source/compiler records
- [ ] RFC/document records
- [ ] tests and diagnostics
- [ ] language-pressure findings
- [ ] git/project metadata
- [ ] harness/CI evidence
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
