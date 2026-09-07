# MNCS Language Pressure

`mncs-index` exists partly to answer a hard question: can MNCS express and efficiently execute a serious, highly concurrent systems workload without escaping into another language?

> First-pass answer (2026-09): MNCS expresses the pure deterministic core
> well (four Profile-0.8 kernels, all semantic decisions in-language), but
> cannot express threads, channels, filesystem effects, watching, clocks,
> crypto digests, unbounded text, or economical invocation. The grounded
> ledger is `pressure/registry.md` (12 entries, 3 minimal reproducers).

## Pressure domains

The project is expected to pressure at least:

- native threads/tasks,
- structured concurrency,
- bounded channels,
- multi-producer/multi-consumer queues,
- select/wait across concurrent events,
- atomics and memory ordering,
- mutex/RW-lock primitives,
- condition/event notification,
- barriers/latches,
- thread-local/task-local state,
- cancellation and deadlines,
- panic/failure propagation,
- deterministic parallel reductions,
- concurrent maps/sets,
- safe ownership transfer across tasks,
- immutable snapshot sharing,
- resource cleanup under cancellation,
- filesystem watching,
- high-resolution timing/metrics,
- scheduler fairness/starvation behavior.

This list is a hypothesis, not a demand that the language copy another ecosystem's APIs.

## Pressure workflow

1. Attempt the clean MNCS-native design.
2. If blocked, identify the exact semantic requirement.
3. Build the smallest faithful reproducer.
4. Record the workload-level consequence.
5. Record any temporary workaround separately.
6. Continue project pressure without pretending the gap is solved.
7. In a focused `mncs-language` run, implement or clarify the upstream capability.
8. Return here and prove the original pressure case now passes.

## Severity

- **P0 blocker** — cannot preserve correctness/safety without leaving MNCS.
- **P1 major** — possible only through architecture-distorting workaround or unusable performance.
- **P2 friction** — expressible but awkward, error-prone, or lacking diagnostics/tooling.
- **P3 enhancement** — ergonomic or optimization opportunity.

## Pressure is not failure

Discovering a gap is a successful result when it is precisely characterized. The failure mode is hiding the gap, weakening the design to avoid it, or describing a workaround as language support.
