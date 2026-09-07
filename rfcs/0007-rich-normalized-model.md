# RFC 0007 — Rich Normalized Model (canonical-v2)

- Status: Proposed (implemented on `index/phase2-pressure`, slice 5)

## Goal

Extend the canonical index with source declarations, document sections,
relationships, and pressure mentions (RFC 0004's relationship,
diagnostic/pressure, and provenance query classes; Phase 4 ingestion)
**without silently mutating canonical-v1**.

## Non-mutation rule

- `canonical-v1` bytes, hashes, markers, readers, and the default build
  path are untouched. The v1 test suite passes unmodified.
- `canonical-v2` is a strict additive superset: the v2 byte stream is the
  v2 marker plus the v1 doc/term section **byte-identical** to v1 (same
  `canon_line` strings, same relative order — all new kind ranks exceed
  all v1 ranks) plus the appended rich section. Proven by
  `tests/test_rich_model.py::test_v1_section_byte_identical_inside_v2`.
- Opt-in: `build --rich` / `build_snapshot_v2`; the default stays v1.

## New record families (ranks append-only, cf. `src/README.md`)

| Kind | Rank | Record | Identity (dedup key) |
|------|------|--------|----------------------|
| sym | 101 | MNCS `module`/`fn`/`record`/`use` declaration | (path, sym, name) |
| heading | 102 | markdown/RFC section heading + level | positional (path, seq) |
| rel | 103 | `defines` / `references` / `depends-on` / `rfc-ref` edge | (src, rel, dst) |
| press | 104 | `PRESS-###` mention | (path, pid) |

Semantics live in `src/extract.mncs` (`mncs.index.extract.v1`); the host
(`runner/mncs_index/extract.py`) only splits/trims/slices, scans 64 B
windows with 1 B overlap, pairs adjacent RFC tokens, and sorts/dedups at
scale (PRESS-005, PRESS-014). Name/title digests reuse the digest fold,
so symbol identity is MNCS-content-addressed. Non-goals: intra-body
symbol references (only `use`-decls and markdown links yield
`references`), nested-paren link targets, transitive graph traversal
(PRESS-015).

## Merge

Per-file extraction is order-free (path-keyed slots); `globalize`
dedups by identity and assigns canonical seqs from identity sorts, so
any worker count converges (`test_worker_counts_converge_rich`).
Incremental v2 reuses previous rich rows exactly for verdict-0
(content-identical) paths and re-extracts the rest through the same
merge, so incremental == rebuild
(`test_incremental_equals_rebuild_v2`).

## Storage migration

- Envelope `mncs-index/snapshot-v2` = v1 keys unchanged + `syms`,
  `headings`, `rels`, `press` tables. The store publishes v2 iff a
  snapshot carries rich tables, else v1 (byte-identical files to before).
- Readers accept both: a v1 envelope loads with empty extension tables
  (`test_v1_envelope_migrates_to_v2_empty`). No backfill is required;
  rebuild with `--rich` to upgrade a store generation.
- Hashing: v2 `index_hash`/`mncs_fingerprint` cover the v2 bytes with
  the same dual-digest scheme (PRESS-006).

## Queries (all snapshot-consistent, canonically ordered, budgeted)

`defines(name)`, `references(dst)`, `depends_on(path)`,
`dependents(dst)`, `rfc_refs(num)`, `pressure(pid)`, `symbols(name)`,
`headings_for(path)`, and `provenance(path)` (snapshot id + generation +
doc digest + establishment generation from the store sidecar). CLI:
`query --defines/--references/--depends-on/--dependents/--rfc/
--pressure/--symbol/--headings/--provenance`.
