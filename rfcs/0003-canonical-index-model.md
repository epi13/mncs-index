# RFC 0003 — Canonical Index Model

- Status: Proposed

## Purpose

Define the logical objects whose meaning must remain stable even if internal storage changes.

## Record envelope

Every canonical record should conceptually include:

- stable record identity,
- record kind/schema identity,
- source identity,
- source snapshot identity,
- provenance,
- normalized payload,
- relationships to other stable identities,
- optional diagnostics/evidence,
- schema/version information.

Exact MNCS types will evolve with implementation pressure.

## Provenance

Provenance is not auxiliary metadata. Consumers must be able to answer where a fact came from and which snapshot established it.

## Canonicalization

Canonicalization must specify:

- string/byte normalization where relevant,
- path normalization without erasing meaningful case/platform distinctions,
- stable field ordering for serialized forms,
- stable record ordering,
- stable relationship ordering,
- explicit handling of duplicate/conflicting records,
- versioned hashing domain.

## Hashing

A canonical snapshot hash identifies canonical logical content, not incidental in-memory layout, insertion order, timestamps of processing, or worker identity.

The hash algorithm itself must be versioned so migrations are explicit.

## Immutability

Published snapshots are logically immutable. Incremental work constructs a successor snapshot and publishes it atomically after validation/canonicalization.
