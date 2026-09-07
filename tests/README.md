# Tests

Behavioral proof, not directory shape. Run from the repository root:

```bash
python3 -m pytest tests/ -q            # bounded default suite (CI)
python3 -m pytest tests/ -q --run-slow # + megabyte-scale determinism
```

Requires an `mncs-language` executor binary: sibling checkout
`../mncs-language/target/debug/mncs` (`cargo build -p mncs-cli`) or the
`MNCS_BIN` environment variable. Tests fail fast with a clear message
when it is missing.

## Files

- `conftest.py` — binary resolution, session `kernels` fixture (MNCS call
  caches shared across the session), fixture-corpus copies, build helpers,
  `--run-slow` option.
- `oracles.py` — TEST-ONLY host mirrors of the kernel algorithms for
  differential testing. Never imported by `runner/mncs_index`.
- `test_kernels.py` — every kernel function pinned by executed cases.
- `test_differential.py` — seeded-random agreement between kernels and
  oracles (the suite that caught the window-boundary word-merge bug).
- `test_determinism.py` — founding invariant: worker counts, seeds,
  delays, queue sizes converge; overlap evidence via `max_in_flight`.
- `test_incremental.py` — `incremental(A→B) == rebuild(B)` across
  add/remove/change/rename/unchanged/multi-batch/chained.
- `test_query.py` — query classes, canonical result order, limits.
- `test_failure.py` — worker failure, starved budgets, broken binary,
  unreadable files, cancellation; previous generation always intact.
- `test_stress.py` — 140+ files, duplicates, empties, deep paths, skewed
  sizes, workers to 32, `wc -w` spot-checks.
- `test_watch.py` — polling watcher follows live mutations.
- `fixtures/corpus/` — deterministic fixture tree (8 indexed files + one
  skipped `.o`).

## Cost model

Each MNCS kernel call is one `mncs execute` subprocess (~10 ms idle,
200+ ms on a loaded box). Suites economize honestly: pure-function caches
(byte classes, kinds, token vocabulary), content-derived skips, and an
`mncs_digest` flag so config sweeps compare canonical bytes while
dedicated tests carry the MNCS-fingerprint claim. Per-call overhead is
recorded as PRESS-010; it constrains stress economics, not correctness.
