# RFC 0001 — Project Charter and Architecture

- Status: Accepted
- Project: mncs-index

## Summary

`mncs-index` is the MNCS ecosystem's machine-native indexing and query substrate. It organizes heterogeneous project facts into deterministic, provenance-rich snapshots and intentionally stresses MNCS-language concurrency in a useful production-shaped workload.

## Goals

- deterministic incremental indexing,
- aggressive local concurrency,
- provenance-preserving canonical records,
- snapshot-consistent queries,
- clear integration contracts,
- MNCS-native implementation,
- actionable language-pressure evidence,
- later distributed execution through Fabric.

## Non-goals

The initial project is not:

- a general internet search engine,
- a vector database replacement,
- a source-code compiler,
- the semantic ingestion layer itself,
- the MNCS memory system,
- a reason to duplicate Git, Harness, Fabric, or Atlas responsibilities.

## Founding invariant

For identical logical input and configuration, canonical output is independent of concurrency level, worker scheduling, filesystem enumeration order, and other incidental execution ordering.

## Architecture

The core dataflow is snapshot -> discovery -> extraction -> normalization -> deterministic merge -> immutable canonical snapshot -> queries.

Each stage may become highly parallel, but every boundary must define ownership, ordering, capacity/backpressure, cancellation, and failure behavior.

## Project success

The project succeeds only if it becomes useful infrastructure *and* continues to expose realistic pressure on MNCS-language. A high-performance host-language implementation with decorative MNCS wrappers would violate the charter.
