# Contributing to mncs-index

## Principles

`mncs-index` values correctness, deterministic behavior, explicit concurrency semantics, and evidence-backed language pressure over feature count.

A small indexing stage that proves deterministic behavior across aggressive scheduling is preferable to a large subsystem whose concurrency semantics are implicit.

## Before implementing

Read:

- `rfcs/0001-project-charter.md`
- `rfcs/0002-deterministic-concurrent-indexing.md`
- `rfcs/0003-canonical-index-model.md`
- `docs/ARCHITECTURE.md`
- `docs/LANGUAGE_PRESSURE.md`

## Implementation language

Production code should be MNCS-language by default.

Host-language scaffolding is acceptable only when it is clearly temporary infrastructure needed to exercise the MNCS implementation and there is no current MNCS-native path. Such scaffolding must be isolated, documented, and tracked for removal.

## Pull requests

A PR that changes executable behavior should explain:

- which pipeline stage changes,
- what may execute concurrently,
- what is deterministic and how it is canonicalized,
- how cancellation/failure is propagated,
- how backpressure is bounded,
- what tests prove the behavior,
- what new language pressure was discovered.

## Tests

Concurrency work should test more than success-path output. Prefer matrices containing:

- 1, 2, 4, 8, and larger worker counts where practical,
- empty, tiny, and large corpora,
- shuffled discovery order,
- randomized execution delays,
- cancellation during each pipeline stage,
- one worker failure among successful workers,
- queue saturation,
- duplicate and stale events,
- repeated runs that compare canonical hashes.

## Pressure records

Use `pressure/registry.md` as the index and add focused files as findings become substantial.

A workaround is not a resolution unless the language/runtime semantics are actually sufficient for the intended design.
