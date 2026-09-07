# Testing Strategy

## Correctness hierarchy

1. logical record correctness,
2. canonical determinism,
3. incremental/full-rebuild equivalence,
4. concurrency safety,
5. cancellation/failure behavior,
6. performance/scaling.

Performance does not compensate for nondeterministic logical output.

## Determinism matrix

For every representative corpus, compare canonical output across worker counts and repeated schedules.

Suggested minimum matrix:

```text
workers: 1, 2, 4, 8, 16/32 where available
runs per configuration: multiple
input discovery order: canonical + shuffled
worker delay injection: none + randomized
```

All successful runs over the same input/configuration must produce the same canonical index hash and equivalent query results.

## Incremental equivalence

For any mutation sequence:

```text
S0 --incremental changes--> I1
S1 --clean rebuild-------> I2
```

`I1` and `I2` should be canonically equivalent when `S1` is the same resulting source snapshot.

## Adversarial scheduling

Tests should deliberately vary timing around:

- queue full/empty transitions,
- worker startup/shutdown,
- snapshot publication,
- cancellation while blocked,
- cancellation during merge,
- duplicate/stale events,
- worker failure during fan-in.

## Stress vs conformance

Conformance tests should be deterministic and reasonably bounded for CI. Stress suites may run longer and should preserve seeds/evidence when they expose a failure.

## Backend pressure

Where MNCS backends differ in threading/runtime capability, record which backend executed each test. Backend-specific success must not be generalized into language-wide conformance without evidence.

## Current evidence (first working pass)

All execution goes through the reference executor via `mncs execute`
(one subprocess per kernel call; see PRESS-010 for the cost).

- Determinism matrix: workers 1/2/4/8 (+16/32 on stress), seeds,
  delay injection, queue sizes 1–256 — identical `index_hash` and
  `mncs_fingerprint` throughout (`test_determinism.py`, `test_stress.py`).
- Overlap proof: bridge `max_in_flight >= 2` asserted on a real build.
- Incremental equivalence: add/remove/change/rename/unchanged/multi/chained
  (`test_incremental.py`).
- Differential: kernels vs independent oracles on seeded random inputs;
  this caught a real word-boundary merge bug before review.
- Failure: injected worker failure, starved step budget, broken binary,
  unreadable file, pre-cancelled build — previous generation intact in all
  cases (`test_failure.py`).
- Backend note: kernels elaborate with zero diagnostics on the current
  toolchain and additionally pass `source-study` validation in CI
  (`index-ci.yml`). Multi-backend execution agreement (WASM/bytecode/LLVM)
  for the kernels is future work.
