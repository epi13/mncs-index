# Pressure Registry

Implementation-derived language pressure from building `mncs-index`.
Conventions: RFC 0006, `docs/LANGUAGE_PRESSURE.md`.
Upstream reference: `mncs-language` RFC 0010 (concurrency) implementation
status `NONE`, RFC 0008 (I/O) `PARTIAL`, RFC 0023 (time) `NONE`,
RFC 0026 (persistence) `NONE` (`docs/rfc-conformance.md` in mncs-language).

| ID | Severity | Area | Description | Workaround | Reproducer |
|----|----------|------|-------------|------------|------------|
| PRESS-001 | P0 | concurrency | No task spawning, threads, pools, or structured concurrency | Host `ThreadPoolExecutor` pipeline | registry text (no syntax to attempt) |
| PRESS-002 | P0 | synchronization | No channels, bounded queues, mutexes, atomics, close semantics | Host `queue.Queue` + locks + drain protocol | registry text |
| PRESS-003 | P0 | filesystem | No traversal/read/watch effects in MNCS | Host discovery/read/polling | registry text |
| PRESS-004 | P2 | language semantics | Integer bitwise ops (`^ & \|`) missing; FNV-1a inexpressible | Xor-free MNCS fold (documented, not FNV) | `reproducers/int-bitwise-xor.mncs` (MNE103) |
| PRESS-005 | P2 | collections/stdlib | `u64` sequences rejected as `iterate` domains (MNB101); unbounded strings/sort/split absent | 64 B windows; host scale-out differentially tested | `reproducers/traversal-u64-domain.mncs` (MNB101) |
| PRESS-006 | P1 | hashing | No cryptographic digest in MNCS source | MNCS fold + host SHA-256 cross-check | registry text |
| PRESS-007 | P1 | watch/time | No filesystem watching or clocks | Polling watcher + host timing | registry text |
| PRESS-008 | P2 | error model | No typed cross-task error aggregation/cancellation distinction | Host failure tree + `MNCSError` mapping | `tests/test_failure.py` |
| PRESS-009 | P3 | language semantics | Nested `iterate` rejected (MNE147) | Factored single-loop calls | `reproducers/nested-iterate.mncs` (MNE147) |
| PRESS-010 | P2 | performance/tooling | One subprocess per kernel call (~10–250 ms); no batch/in-process API | Call batching via caching + digest flag | `tests/test_stress.py` timings |
| PRESS-011 | P3 | diagnostics | MNB101 span covers the whole file; no traversable-type list | Trial-and-error probes (documented matrix) | registry text |
| PRESS-012 | P2 | cancellation | No cancellation/deadline semantics | Host events + drain protocol | `tests/test_failure.py` |

## PRESS-001 — No concurrency primitives (P0, concurrency, runtime)

- Observed: `mncs-language` RFC 0010 implementation status is `NONE`.
  Source Profiles 0.1–0.10 offer pure bounded computation only: no
  thread, pool, scoped-task, join, yield, or scheduler control syntax
  exists to attempt (verified by profile docs and
  `spec/source-representations.md` open-requirements list, which names
  "concurrency syntax" as unresolved). Nuance: `library/std/task.mncs`
  (`mncs.std.task.v1`) defines a cooperative single-task *lifecycle
  vocabulary* (`spawn`/`begin`/`advance`/`finish`, cancellation flag,
  step budget) that is explicitly not threads/channels/preemption/fan-out.
  It is the natural foundation for future structured concurrency, but
  expresses no parallel execution today.
- Desired: task spawning with structured lifetime (join/cancel/failure
  propagation) sufficient to express discovery → bounded queue →
  workers → deterministic merge.
- Why needed: the entire pipeline fan-out/fan-in, backpressure, and
  graceful shutdown currently live in `runner/mncs_index/pipeline.py`.
- Workaround: host `ThreadPoolExecutor` read/kernel pools with a bounded
  `queue.Queue`. Cost: the concurrency architecture — the heart of this
  project — is not MNCS-expressible; all scheduling evidence is host-side.
- Correctness impact: none observed (determinism enforced at the merge),
  but language coverage of the project is capped at the kernel level.
- Classification: runtime/stdlib (missing), language semantics (missing).
- Evidence: `runner/mncs_index/pipeline.py`; determinism matrix
  `tests/test_determinism.py`.

## PRESS-002 — No synchronization primitives (P0, synchronization, runtime)

- Observed: no channels (bounded/unbounded), mutexes, RwLocks, atomics,
  CAS, barriers, semaphores, or condition variables in any profile or
  stdlib module (`library/core`, `library/std` surveyed; RFC 0010 `NONE`).
- Desired: at minimum a bounded multi-producer/multi-consumer channel
  with close semantics and cancellation-safe receive, so backpressure and
  shutdown are language-level contracts rather than host code.
- Workaround: `queue.Queue(maxsize=...)` plus a hand-rolled drain protocol
  (`producers_done` event, discard-after-stop, join guarantees) in
  `pipeline.py`. An early draft of that protocol could hang `join()` on
  failure; the fixed protocol is tested by `tests/test_failure.py`.
- Cost: shutdown/cancellation semantics — a required RFC 0002 property —
  cannot be expressed or tested in MNCS.
- Classification: runtime/stdlib.

## PRESS-003 — No filesystem effects (P0, filesystem, language/runtime)

- Observed: RFC 0008 (I/O) is `DRAFT`/`PARTIAL`; effects-and-capabilities
  prototypes treat `read filesystem:/config` as string-typed doc text, not
  an executable capability. No traversal, read, metadata, or watch API is
  callable from MNCS source.
- Desired: effect-gated directory traversal and file reads with explicit
  authority, so discovery snapshots are MNCS values.
- Workaround: host `discover.py` (walk/read/normalize) + polling watcher.
  Cost: provenance roots in host code; path normalization policy is
  host-written (differentially untestable against MNCS — there is no MNCS
  string type to compare with).
- Newer evidence (not yet integrated): the engine has grant-gated host
  effects behind the *experiment* invocation path — `blob_read`
  discharging `host_read` (`crates/mncs-model/src/body.rs`), exposed via
  `experiment` `--grant-read capability=path`
  (`crates/mncs-cli/src/main.rs`). Unverified: whether `.mncs` source
  profiles can name these intrinsics, and whether directory traversal
  exists at all. Unusable for this workload today: the experiment path
  compiles through backends per invocation (~seconds), versus ~10 ms per
  `execute` call. Recommended next probe: one `.mncs` source function
  calling a granted read via `experiment run`, to pin the source-level
  surface and cost.
- Classification: language semantics (effects), runtime, stdlib.

## PRESS-004 — Integer bitwise operators missing (P2, semantics, compiler)

- Observed: `byte` supports `& | ^`; the same operators on `u64` fail
  elaboration (`MNE103`, return-type mismatch). Reproducer:
  `pressure/reproducers/int-bitwise-xor.mncs`.
- Desired: integer `& | ^` (wrapping/total, like shifts) or a documented
  reason for their absence.
- Why needed: standard content folds (FNV-1a xor-fold, splitmix-style
  mixers, hash combining) are inexpressible; `src/digest.mncs` defines an
  explicitly xor-free multiply-add/shift fold instead. The algorithm is
  deterministic and order-sensitive (pinned by
  `tests/test_differential.py`), but it is not the standard, well-studied
  function and cannot cite its analysis.
- Cost: algorithm substitution in a hashing core; mild avalanche loss.
- Classification: compiler/frontend (operator typing).

## PRESS-005 — Traversal-domain and unbounded-data gaps (P2, stdlib)

- Observed: `u64` (and view-of-`u64`) sequences are rejected as `iterate`
  domains (`MNB101`); `i64`/`byte` are accepted. Reproducer:
  `pressure/reproducers/traversal-u64-domain.mncs`. Separately, there are
  no unbounded strings, string splitting, corpus-scale sorting, or
  substring search in MNCS (MAX_SEQUENCE_BOUND is 64).
- Desired: documented traversable element types (or `u64` support), plus a
  story for unbounded text (streaming views, chunked cursors) that keeps
  semantic decisions in-language.
- Workaround: 64-byte windows; host applies MNCS-verdict tables
  (`classify_byte` cache) and the MNCS-specified lexicographic rule at
  corpus scale. Every mirrored rule is differentially tested against its
  kernel on seeded random inputs (`tests/test_differential.py`); production
  meaning still comes from MNCS calls.
- Cost: ~8x call amplification vs word-oriented kernels; host scale-out
  code must be maintained alongside kernel specs.
- Classification: compiler (domain rule), stdlib (missing collections).

## PRESS-006 — No cryptographic digests (P1, hashing, stdlib)

- Observed: no SHA-256 or other crypto digest is callable from MNCS
  source; hashing in-language stops at stdlib folds.
- Desired: either a verified digest primitive or an explicit non-goal with
  a recommended bridging contract.
- Workaround: dual digests — MNCS Merkle digest over the canonical
  bytes (`mncs_fingerprint_of`) plus host SHA-256 (`index_hash`).
  Both are asserted across worker counts; they cross-check each other.
- Newer evidence (not yet integrated): the engines carry
  `crypto:sha256`/`crypto:ed25519` verification primitives with
  `--grant-crypto` (`crates/mncs-model/src/execution.rs`,
  `ssa_execution.rs`, CLI experiment options). Unverified from `.mncs`
  source and unreachable at per-call cost (experiment path only).
  Recommended next probe: verify a known SHA-256 through a granted
  kernel call and compare envelope cost against `execute`.
- Cost: the headline canonical hash is half-outsourced; long-term
  content-addressing story depends on host crypto.
- Classification: stdlib/runtime.

## PRESS-007 — No watching or clocks (P1, watch/time, runtime)

- Observed: RFC 0023 (time) `NONE`; no watch effect exists. Performance
  evidence (wall time, throughput) can only be collected host-side.
- Desired: filesystem-event effects with coalescing-tolerant semantics, or
  an explicit statement that watching stays host-side with a normalized
  event-hint contract.
- Workaround: polling watcher with quiet-period coalescing (`watch.py`);
  hints select transactions, content re-derives truth (RFC 0005 pattern).
  Proven by `tests/test_watch.py`. Host `time.monotonic` supplies
  intervals and performance evidence.
- Newer evidence (not yet integrated): `library/std/clock.mncs`
  (`mncs.std.clock.v1`) offers relational time comparisons over u64
  epochs, fed by a `clock_read()` intrinsic realized under explicit
  `--grant-time` (`crates/mncs-cli/src/main.rs`). Unverified from the fast
  `execute` path. Recommended next probe: read granted time through a
  kernel call to pin availability and cost.
- Cost: event-burst/duplication pressure cannot be exercised against real
  watcher semantics; latency is poll-bound.
- Classification: runtime/stdlib.

## PRESS-008 — No concurrent error aggregation (P2, error model)

- Observed: MNCS has failure modes (`fail`, `Result`-shaped stdlib) but no
  way to aggregate failures across tasks, distinguish cancellation from
  error, or propagate scoped diagnostics through a task tree.
- Desired: typed multi-error results and cancellation-distinct failure
  propagation for pipelines.
- Workaround: host failure tree (first-error capture, stop broadcast,
  `BuildFailed`/`BuildCancelled`, `MNCSError` kernel mapping). Tested in
  `tests/test_failure.py`, including "failed builds never publish".
- Cost: failure semantics — an RFC 0002 requirement — are host-defined.
- Classification: language semantics, runtime.

## PRESS-009 — Nested iteration rejected (P3, semantics, compiler)

- Observed: nested `iterate` fails with `MNE147` (Profile 0.4 rule).
  Reproducer: `pressure/reproducers/nested-iterate.mncs`.
- Desired: either nested bounded iteration with a multiplied cost bound or
  a clearer diagnostic pointing at the factored-call pattern.
- Workaround: factor the inner scan into a called function
  (`window_equal8` called per outer position in `contains8`). Clean,
  bounded (64x8 comparisons), no capability loss for this workload.
- Cost: ergonomic only.
- Classification: compiler/frontend.

## PRESS-010 — Per-call process invocation cost (P2, performance/tooling)

- Observed: each MNCS kernel call is a subprocess (`mncs execute`,
  ~10 ms idle, 200+ ms under load) including full source elaboration.
  Measured: fixture build = 231 calls; stress build ≈ 1.6k calls;
  megabyte file = 16k calls (16k subprocesses for 1 MB at 64 B/window).
- Desired: in-process invocation, persistent sessions, or a batch-call API
  (many requests per process) so systems workloads are economical.
- Workaround: pure-function caches (byte classes, kinds, token
  vocabularies), term-identity redesign (content tag instead of
  per-term combine), `mncs_digest` flag so sweeps compare bytes while
  dedicated tests carry the fingerprint claim, and parallel tree
  reduction (`tree_combine`/`tree_digest_windows`: levels sequential,
  pairs concurrent) so large files and canonical bytes no longer reduce
  serially in the host thread.
- Cost: stress economics; CI minutes; temptation to shrink coverage.
- Classification: tooling/runtime. Evidence: `tests/test_stress.py`
  timing print; `bridge.py` instrumentation (`calls`, `max_in_flight`).

## PRESS-011 — Coarse traversal diagnostics (P3, diagnostics, compiler)

- Observed: `MNB101` span covers the entire file (start 0, end N) rather
  than the offending `iterate`; no documented list of traversable element
  types exists, so discovery was trial-and-error (`i64` ok, `u64` no).
- Desired: precise spans and a documented domain-eligibility rule.
- Cost: ergonomic; slowed kernel development by ~30 minutes.
- Classification: compiler diagnostics.

## PRESS-012 — No cancellation semantics (P2, runtime, language)

- Observed: no deadlines, cancellation tokens, or scoped shutdown in any
  profile; RFC 0011 (nondeterminism/failure) is `PARTIAL` without these.
- Desired: structured cancellation propagation into blocked kernel calls.
- Workaround: host cancel events checked between items plus the drain
  protocol; in-flight subprocess calls run to completion (bounded by the
  60 s timeout). Tested by cancellation tests.
- Cost: shutdown latency is host-bounded, not language-guaranteed.
- Classification: runtime, language semantics.

## Not pressure (deliberate non-findings)

- Source Profile 0.8 was sufficient for every kernel (records, `select`,
  wrapping arithmetic, views, `u64` shifts, calls across functions).
  No parser, type-system, or elaboration blockers were hit beyond the
  entries above.
- `execute` accepts `.mncs` source directly with precise, coded
  diagnostics (`source-study`); the JSON request/response ABI is stable
  and documented by example. Unknown functions surface as typed
  `invalid_request` statuses, not crashes.
