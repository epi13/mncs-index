# Source

Production meaning lives in MNCS source here. The host runner
(`runner/mncs_index/`) carries only effects the language cannot yet
express; every such effect maps to a `pressure/` entry.

## Kernels

| File | Module | Decides |
|------|--------|---------|
| `digest.mncs` | `mncs.index.digest.v1` | Content folds, Merkle leaves/combines, canonical-bytes fold |
| `scan.mncs` | `mncs.index.scan.v1` | Byte classes, symbol validation, token digests, batch validation |
| `kind.mncs` | `mncs.index.kind.v1` | Extension bytes to canonical kind rank |
| `order.mncs` | `mncs.index.order.v1` | Lexicographic compare, record order, query match, change verdicts |

All four target Source Profile 0.8, are dependency-free (no `use`
imports, so `mncs execute` needs no library search path), and elaborate
with zero `MNE`/`MNB`/`MNP` errors:

```bash
mncs source-study src/digest.mncs --node-id local
```

Each kernel function is pinned by `tests/test_kernels.py` and
cross-checked against independent oracles on seeded random inputs by
`tests/test_differential.py`.

## Algorithm notes (frozen)

- Digest fold: xor-free multiply-add/shift (`mix_step`), because integer
  bitwise ops do not exist (PRESS-004). FNV offset basis and prime are
  reused as constants, but the function is **not** FNV-1a.
- File digest: canonical binary Merkle tree over ordered 64-byte leaves;
  odd tails pair with `0`; empty files digest to the basis. Levels are
  sequential, pairs within a level run concurrently with index-placed
  results (`Kernels.tree_combine`).
- Canonical fingerprint: the same Merkle construction over the canonical
  bytes (`Kernels.tree_digest_windows`), so content and canonical identity
  share one scheme.
- Canonical record order: `(kind_rank, path_bytes, seq)`, byte-lexicographic.
- Kind ranks: 0 unknown, 1 mncs, 2 md, 3 json, 4 toml, 5 yaml/yml,
  6 text, 7 log, 100 term. Ranks are frozen — additions append only.
- Token rule: `[A-Za-z_][A-Za-z0-9_]*`, length 1–32; per-file distinct
  tokens sorted, capped at 256.
