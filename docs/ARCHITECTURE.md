# Architecture

## Pipeline

The conceptual local pipeline is:

```text
source snapshot
    |
    v
parallel discovery
    |
    v
bounded work queue
    |
    +----> parse/inspect worker
    +----> parse/inspect worker
    +----> parse/inspect worker
    |
    v
normalize
    |
    +----> symbol records
    +----> relationship records
    +----> provenance records
    +----> diagnostics/evidence records
    |
    v
deterministic merge / reduction
    |
    v
canonical snapshot
    |
    +----> query readers
    +----> persistence
    +----> incremental base
```

The important part is not the exact number of stages. It is that each boundary has explicit ownership, ordering, capacity, cancellation, and failure semantics.

## Core concepts

### Source snapshot

A finite declared view of inputs to be indexed. Filesystem state changing underneath a run must not silently redefine the run's identity.

### Work item

A schedulable unit derived from the source snapshot. Work items should have stable identities sufficient for retry, deduplication, and stale-work rejection.

### Normalized record

An implementation-independent logical fact ready for canonicalization. Producers may emit records in any execution order.

### Canonical snapshot

An immutable logical view produced by deterministic normalization, ordering, conflict resolution, and serialization rules.

### Published snapshot

The snapshot visible to queries. Publication should be atomic at the logical level: readers should not observe half-merged state.

## Concurrency layers

1. **Discovery concurrency** — enumerate independent roots/partitions.
2. **Extraction concurrency** — parse and inspect inputs.
3. **Normalization concurrency** — transform producer-specific records.
4. **Merge concurrency** — reduce partitions while preserving canonical semantics.
5. **Query concurrency** — many readers against immutable snapshots.
6. **Maintenance concurrency** — compaction, persistence, invalidation, watch processing.
7. **Distributed concurrency** — later, execute partitions through Fabric.

## Deterministic boundary

Execution order is intentionally unspecified before canonical merge. Observable logical order is intentionally specified after it.

This separation allows aggressive work stealing and scheduling experimentation without leaking scheduler accidents into the index format.

## Backpressure

Every producer/consumer boundary that can outpace its consumer should have a defined capacity or explicit resource budget. Unbounded accumulation is considered an architectural defect, not merely a performance bug.

## Cancellation

Cancellation must form a tree rooted in an indexing/query operation. Child work should not outlive an operation unless explicitly detached by contract. Closing a pipeline must wake blocked producers and consumers rather than relying on polling.

## Failure

One worker failure must have a defined effect: fail the operation, produce a scoped diagnostic, retry a safe/idempotent item, or quarantine a partition. Silent partial success is not a default.

## Snapshot publication

Writers build a candidate snapshot privately. Once canonicalization succeeds, publication swaps the visible snapshot atomically. Readers either see the previous complete snapshot or the new complete snapshot.

## Future storage

The initial implementation should prioritize semantic correctness over storage sophistication. Persistence, MVCC-like techniques, memory mapping, compaction, and lock-free structures are valid later pressure points, but should not obscure the first deterministic concurrency proof.

## Implementation notes (first working pass)

- Meaning lives in `src/*.mncs` (Profile 0.8, dependency-free): content
  folds and Merkle combines (`digest.mncs`), byte classes and token
  validation (`scan.mncs`), kind ranks (`kind.mncs`), ordering/matching/
  change verdicts (`order.mncs`).
- The host runner (`runner/mncs_index/`) is two thread pools (read +
  kernel) joined by a bounded `queue.Queue`, per-file ordered assembly, a
  canonical sort, and atomic publication. See `runner/README.md` for the
  per-module pressure map.
- Work item = one file; kernel item = one MNCS `execute` call (one 64 B
  window, one 8-token validation batch, one predicate). Intra-file calls
  are ordered by construction; inter-file execution is fully parallel.
- The carried word-boundary flag is threaded in canonical window order at
  assembly; the one implied overcount is repaired by rule (non-space
  continuation), differentially tested against the kernel.
- Canonical bytes hash content only (format, discovery id, records);
  generation is store metadata. Term identity is (path, token) with an
  MNCS content tag; file binding travels through the sort key and parent
  digest.
- Known simplifications: no inter-file reference edges yet (so no
  dependency invalidation graph), renames surface as remove+add,
  substring queries beyond 8 B use the differentially-tested host path,
  and large-file economics are bounded by one-subprocess-per-call
  (PRESS-010).
