# RFC 0005 — Incremental and Watch Model

- Status: Proposed

## Principle

Incremental indexing is an optimization over the semantics of a clean rebuild.

Given the same resulting source snapshot, incremental update and clean rebuild must produce canonically equivalent index state.

## Event handling

Filesystem/provider events are hints that something may have changed, not canonical truth. Events may be duplicated, reordered, coalesced, or lost depending on platform/provider.

The indexer should reconcile events against source identity/state before publication.

## Stale work

A work item belongs to a source generation/snapshot. Results from an obsolete generation must not overwrite newer canonical state merely because they finished later.

## Deletion

Deletion/invalidation semantics must be explicit. Records that lose their source must not linger indefinitely due to a missed event.

## Publication

A batch of incremental changes builds a candidate successor snapshot and atomically publishes after canonical merge/validation.

## Watch pressure

Watch mode should deliberately test event bursts, duplicate events, rename/write sequences, cancellation, queue saturation, and rapid successive generations.
