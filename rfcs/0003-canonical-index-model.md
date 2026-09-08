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

## Implementation note (first pass)

Canonical bytes cover format marker, discovery snapshot id, and records
only; the store generation is publication metadata, so identical content
hashes identically at any generation. Record identity is `(kind, path,
seq)` with an MNCS-computed content digest; per-record establishment
generation (`established`) and per-record content hints (`crc`) are
tracked in non-canonical store sidecars so they cannot break
`incremental == rebuild` — two stores may share an `index_hash` while
carrying different provenance/hints, by design. The v2 format marker
re-hashes the whole corpus versus v1 (versioned hashing domain above);
only empty-rich snapshots degrade to identical v1 bytes (RFC 0007).

Lossy horizons (deterministic but information-destroying; invisible in
the hash beyond the surviving bytes): `TERMS_PER_FILE_CAP` (256 terms),
`TOKEN_MAX` (whole-token drop past 32 B), `MAX_NAME` (64 B) /
`MAX_LINK_TARGET` (256 B), `KERNEL_WINDOW` (64 B kernel input; longer
lines raise past the guard), extension slicer (`ext > 8` → unknown),
sym dedup keep-first on `(path, sym, name)`. Identical hashes therefore
mean identical *surviving* semantics, not identical source bytes —
source truth lives in the corpus, the index is a derived view.
