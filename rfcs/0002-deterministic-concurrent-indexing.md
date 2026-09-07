# RFC 0002 — Deterministic Concurrent Indexing

- Status: Proposed

## Problem

Indexing naturally parallelizes, but parallel execution introduces nondeterministic discovery, completion, and merge ordering. If those incidental orders leak into stored state, hashes, conflict resolution, or query ranking, worker count becomes part of program meaning.

## Decision

Execution order before canonicalization is unspecified. Canonical logical order after merge is specified.

A fixed source snapshot and configuration must produce equivalent canonical state under every supported worker count and valid schedule.

## Required properties

### Stable identity

Every source/work/record used in merge must carry a stable identity derived from logical input, not allocation address, thread id, or completion time.

### Deterministic conflicts

If two records compete for one canonical slot, resolution must use an explicit deterministic rule or produce a deterministic conflict record. "Last worker wins" is forbidden.

### Deterministic reduction

Parallel partitioning may vary. The reduction operation must either be mathematically/order independent for the relevant values or impose a stable merge order.

### Bounded pipelines

Producer/consumer stages must use explicit capacity or resource budgets. Backpressure behavior is part of the contract.

### Structured cancellation

Index operations own their child work. Cancellation propagates downward and blocked tasks must be releasable without polling loops.

### Failure propagation

Worker failures are explicit results. Partial success must be intentional and provenance-visible.

## Conformance

At minimum, a fixture corpus should be indexed repeatedly with multiple worker counts and randomized delay/scheduling perturbations. Canonical hash and deterministic queries must agree.

## Why this matters to MNCS-language

This workload pressures semantics that simple thread benchmarks do not: ownership transfer, channel closure, cancellation, deterministic fan-in, failure trees, and immutable publication.
