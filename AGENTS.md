# Agent Contract — mncs-index

`mncs-index` is both useful infrastructure and a deliberate pressure vessel for MNCS-language concurrency.

## Non-negotiable goals

1. Prefer MNCS-language for production implementation, tests, fixtures, and tools wherever the language can express the requirement.
2. Do not hide language gaps behind a permanent implementation in Rust, Python, Go, C++, JavaScript, or another host language.
3. When MNCS cannot express a required construct cleanly, isolate the smallest faithful pressure case and record it under `pressure/`.
4. Determinism is part of correctness, not a test convenience.
5. A fixed snapshot and configuration must produce equivalent canonical index state independent of worker count or scheduling.
6. Concurrency must be real. Do not satisfy concurrency milestones with serial implementations wrapped in task syntax.
7. Keep integration boundaries explicit. `mncs-index` should consume contracts, not become tightly coupled to internal implementation details of sibling repositories.

## Required implementation posture

When adding an indexing stage, identify:

- ownership of inputs and outputs,
- concurrency unit,
- ordering contract,
- cancellation behavior,
- backpressure behavior,
- failure propagation,
- retry/idempotence semantics,
- deterministic merge/reduction rule,
- shutdown semantics,
- observability/evidence produced.

If any of these are undefined, the stage is not finished.

## Language pressure

Pressure is a first-class output.

A pressure finding should include:

- concise gap title,
- concrete workload that exposed it,
- minimal MNCS example where possible,
- current workaround,
- why the workaround is insufficient,
- desired language/runtime behavior,
- affected backend/runtime if known,
- determinism/safety/performance impact,
- reproduction command or test,
- status and upstream issue/PR when created.

Do not merely write "threads are missing". Record the semantic requirement precisely: for example, bounded multi-producer channels with cancellation-safe receive and deterministic close propagation.

## Evidence over claims

Do not describe something as implemented unless executable evidence exists in the repository or linked CI.

Preferred evidence includes:

- compile success using current `mncs-language`,
- deterministic corpus hashes at several worker counts,
- repeated stress runs,
- randomized scheduling tests,
- failure/cancellation tests,
- race/deadlock pressure cases,
- backend conformance evidence where relevant.

## Changes touching RFC behavior

If implementation changes a founding invariant, canonical data model, query semantics, incremental invalidation semantics, or pressure methodology, update the corresponding RFC in the same change.

## Cross-repository work

When a genuine language gap is found, do not silently vendor a fix here. Record it here first, then fix it in `mncs-language` in a focused language run. Keep enough evidence in this repository to prove that the upstream fix actually resolves the original workload.
