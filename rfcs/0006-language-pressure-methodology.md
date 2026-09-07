# RFC 0006 — Language Pressure Methodology

- Status: Proposed

## Motivation

`mncs-index` must not adapt its architecture around undocumented MNCS-language limitations. It should expose those limitations precisely so the language can improve.

## Method

For each major implementation slice:

1. design the clean workload semantics first,
2. attempt them in current MNCS-language,
3. separate language/runtime/tooling/backend gaps,
4. reduce each material gap to a reproducible case,
5. register the pressure with severity and consequence,
6. keep temporary workarounds visibly temporary,
7. fix gaps upstream in focused language work,
8. re-run the originating workload as acceptance evidence.

## Categories

- syntax/type-system expressiveness,
- ownership/lifetime semantics,
- concurrency primitives,
- atomic/memory model,
- structured cancellation/failure,
- standard library gaps,
- filesystem/platform APIs,
- diagnostics/debugging,
- compiler/backend correctness,
- runtime performance/scheduling,
- build/test/tooling.

## Anti-patterns

- replacing the hard subsystem in another language,
- weakening determinism because merge ordering is difficult,
- using global locks everywhere and declaring concurrency solved,
- unbounded queues to avoid backpressure semantics,
- polling to avoid cancellation/wakeup semantics,
- documenting a gap without a reproducer when one is practical,
- fixing mncs-language without retaining the downstream regression proof.

## Definition of resolved

A pressure item is resolved only when the intended workload can use the upstream capability and the original reproduction/conformance test passes without the architecture-distorting workaround.
