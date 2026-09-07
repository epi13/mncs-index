# RFC 0004 — Query Model

- Status: Proposed

## Goals

Queries should be deterministic, snapshot-consistent, provenance-rich, cancellable, and useful to both humans and machine agents.

## Initial query classes

- exact identity lookup,
- record-kind filtering,
- field/value filtering,
- relationship traversal,
- provenance/source lookup,
- diagnostic/pressure lookup.

Semantic/vector ranking can be added later without weakening deterministic base-query behavior.

## Snapshot consistency

Each query executes against one published snapshot identity. A concurrent reindex may publish a newer snapshot, but a query must not silently mix records from both.

## Ordering

Any query that returns an ordered collection must define an ordering key. Provider order, hash-map iteration order, insertion order, and thread completion order are not valid contracts.

## Budgets and cancellation

Queries may accept resource/time/result budgets. Exceeding a budget should return an explicit bounded result rather than allowing unbounded traversal.

## Provenance

Queries should make provenance easy to request and preserve stable identities in results so Atlas/agents can cite or re-resolve facts.
