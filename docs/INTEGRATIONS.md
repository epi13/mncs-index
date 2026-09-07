# MNCS Ecosystem Integration Boundaries

`mncs-index` should become shared infrastructure without becoming a dependency magnet that knows sibling internals.

## mncs-language

`mncs-index` is a consumer and pressure source.

It needs language/runtime semantics for concurrency, ownership, synchronization, deterministic reduction, cancellation, filesystem access, canonical serialization, and eventually efficient storage. Gaps discovered here should be reduced to evidence and fixed upstream rather than permanently bypassed.

## mncs-ingest

`mncs-ingest` may eventually provide normalized machine-native semantic records from heterogeneous inputs. `mncs-index` should be able to consume those records while retaining source identity and provenance.

Boundary principle: ingest interprets/normalizes input; index organizes, relates, versions, and queries records.

## mncs-memory

`mncs-memory` may consume index relationships or provide higher-level semantic/memory relationships back to the index.

Boundary principle: index should not require a memory model to answer basic deterministic queries.

## Atlas

Atlas and entering agents are natural query consumers. Index responses should be provenance-rich and snapshot-identifiable so an agent can distinguish fact, source, and freshness.

## mncs-harness

Harness can publish test/build/run evidence for indexing and can use the index to discover relevant tests, diagnostics, and prior pressure records.

## mncs-fabric

Fabric is the future execution boundary for distributed indexing pressure. Local semantics must be established first. Remote workers should receive explicit work units and return normalized evidence/records rather than gaining broad repository authority.

## Git / CI / project metadata

Adapters should translate external metadata into stable records. The canonical index must not depend on provider response order, wall-clock race timing, or incidental API pagination.
