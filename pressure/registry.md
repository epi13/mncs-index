# Pressure Registry

Implementation-derived language pressure from building `mncs-index`.
Conventions: RFC 0006, `docs/LANGUAGE_PRESSURE.md`.
Upstream reference: `mncs-language` RFC 0010 (concurrency) implementation
status `NONE`, RFC 0008 (I/O) `PARTIAL`, RFC 0023 (time) `NONE`,
RFC 0026 (persistence) `NONE` (`docs/rfc-conformance.md` in mncs-language).

| ID | Severity | Area | Description | Workaround | Reproducer |
|----|----------|------|-------------|------------|------------|
| PRESS-001 | P0 | concurrency | No task spawning, threads, pools, or structured concurrency (lifecycle vocabulary confirmed usable, execution parallelism still absent) | Host `ThreadPoolExecutor` pipeline | `reproducers/probe-task-lifecycle.mncs` + `tests/test_language_probes.py` |
| PRESS-002 | P0 | synchronization | No channels, bounded queues, mutexes, atomics, close semantics (confirmed; task.v1 gives cooperative hand-off vocabulary only) | Host `queue.Queue` + locks + drain protocol | `tests/test_concurrent_read.py` (bounded queues, saturation, blocked-reader cancel) + `tests/test_failure.py` + `tests/test_language_probes.py` |
| PRESS-003 | P0 | filesystem | No traversal/watch effects in MNCS; bounded single-blob `host_read` partially available via grant-gated experiment path | Host discovery/read/polling | `reproducers/probe-host-read.mncs` + corpus/grant + `tests/test_language_probes.py` |
| PRESS-004 | P2 | language semantics | Integer bitwise ops (`^ & \|`) missing; FNV-1a inexpressible | Xor-free MNCS fold (documented, not FNV) | `reproducers/int-bitwise-xor.mncs` (MNE103) |
| PRESS-005 | P2 | collections/stdlib | `u64` sequences rejected as `iterate` domains (MNB101); unbounded strings/sort/split absent | 64 B windows; host scale-out differentially tested | `reproducers/traversal-u64-domain.mncs` (MNB101) |
| PRESS-006 | P1 | hashing | Verify-only `sha256_digest` partially available via grant-gated experiment path (views ≤64 B, per-invocation compile); no in-kernel digest | MNCS fold + host SHA-256 cross-check | `reproducers/probe-sha256-abc.mncs` (NIST "abc" vector) + `tests/test_language_probes.py` |
| PRESS-007 | P1 | watch/time | `clock_read` partially available via grant-gated experiment path (relational use only); still no filesystem watching | Polling watcher + host timing | `reproducers/probe-clock.mncs` + corpus + `tests/test_language_probes.py` |
| PRESS-008 | P2 | error model | No typed cross-task error aggregation/cancellation distinction | Host failure tree + `MNCSError` mapping | `tests/test_failure.py` |
| PRESS-009 | P3 | language semantics | Nested `iterate` rejected (MNE147) | Factored single-loop calls | `reproducers/nested-iterate.mncs` (MNE147) |
| PRESS-010 | P2 | performance/tooling | One subprocess per kernel call (~10–250 ms); no batch/in-process API | Call batching via caching + digest flag | `tests/test_stress.py` timings |
| PRESS-011 | P3 | diagnostics | MNB101 span covers the whole file; no traversable-type list | Trial-and-error probes (documented matrix) | `reproducers/traversal-u64-domain.mncs` (MNB101; shared with PRESS-005) |
| PRESS-012 | P2 | cancellation | No cancellation/deadline semantics | Host events + drain protocol | `tests/test_failure.py` |
| PRESS-013 | P1 | synchronization/persistence | No durable compare-and-swap / transaction effects for cross-process publication | Host `flock`-guarded HEAD check-and-swap in `Store.publish` | `tests/test_publish.py` |
| PRESS-014 | P2 | collections/stdlib | No unbounded lines, line splitting, substring slicing, or corpus-scale sort/dedup for rich extraction | Host line/span plumbing + 64 B kernel windows; sort/dedup at merge | `tests/test_rich_model.py` |
| PRESS-015 | P3 | language semantics | No relation/table values or transitive graph traversal over extracted edges | Single-hop host indexes; no transitive queries | `tests/test_rich_model.py` |

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
- Slice-4 verdict 2026-09-07 (`index/phase2-pressure`): **confirmed
  still missing; lifecycle vocabulary refined as available.**
  `pressure/reproducers/probe-task-lifecycle.mncs` (`mncs 0.10`,
  `use mncs.std.task.v1 as task`) drives `candidate_lifecycle(8)` to a
  terminal DONE task (`true`, 321 steps), `candidate_cancel(8)` to a
  terminal CANCELLED task (`was_cancelled == true`, 142 steps), and
  `candidate_invalid()` to packed flags `0` (all three invalid
  transitions rejected, 64 steps) — all through the fast `execute`
  path with zero `MNE`/`MNB`/`MNP` errors and no grants. This is a
  cooperative single-task state machine only: no spawnable threads,
  pools, join, preemption, or fan-out exists to express discovery →
  queue → workers → merge. Note: `import` aliases require profile
  ≥0.9 (`MNP181` under 0.8), and a stdlib-importing `execute` costs
  ~0.43 s/call versus ~10 ms for the dependency-free `src/*.mncs`
  kernels — one reason kernels stay import-free.
  Regression: `tests/test_language_probes.py` (task execute + elaboration tests).
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
- Slice-4 verdict 2026-09-07 (`index/phase2-pressure`): **confirmed
  still missing.** No channel, mutex, RwLock, atomic, CAS, barrier, or
  close-semantics syntax exists to attempt. The closest in-language
  shape — `task.request_cancel` arming a flag observed at the next
  `advance` boundary (`candidate_cancel`, pinned by
  `tests/test_language_probes.py::test_task_cancel_is_terminal`) — is a
  cooperative single-task hand-off, not an MPMC bounded queue: it cannot
  express backpressure, multi-producer close, or shutdown broadcast, so
  both queue stages stay host-side.
- Workaround: `queue.Queue(maxsize=...)` plus a hand-rolled drain protocol
  (`producers_done` event, discard-after-stop, join guarantees) in
  `pipeline.py`. An early draft of that protocol could hang `join()` on
  failure; the fixed protocol is tested by `tests/test_failure.py`.
- Update 2026-09-07 (`index/phase2-pressure`, slice 3): content
  acquisition is now a second bounded-queue stage — single-threaded
  enumeration feeding a bounded read queue drained by parallel reader
  threads (`runner/mncs_index/discover.py::discover_concurrent`, sharing
  the caller's worker/queue budget). Completion order never escapes
  (results keyed by path, sorted; identity from sorted entries), the
  drain protocol mirrors `pipeline.py` (every dequeue is `task_done`'d,
  threads joined on all paths), and the bound is backpressure, not
  meaning (`queue_size=1` builds identically). Tested by
  `tests/test_concurrent_read.py` (slow/fast-failing/saturated reads,
  cancellation with blocked readers, skewed and opposite layouts).
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
- Update 2026-09-07 (`index/phase2-pressure`, slice 3): the read half is
  now genuinely concurrent (bounded read queue + parallel `mncs-reader`
  threads; enumeration stays single-threaded so admission/skipped
  accounting is unchanged). Reader overlap is measured, not assumed
  (`ReadStats.max_in_flight >= 2` under a 4-way barrier in
  `tests/test_concurrent_read.py::test_slow_readers_overlap`).
- Slice-4 verdict 2026-09-07 (`index/phase2-pressure`): **partially
  available — bounded single-blob read confirmed at source level.**
  Smallest legal program `pressure/reproducers/probe-host-read.mncs`
  (`mncs 0.10`, `capability probe_reader`,
  `effect host_read authorized_by probe_reader`,
  `let blob: [byte; up_to 64] = host_read(); return blob.len;`)
  elaborates with zero `MNE`/`MNB`/`MNP` errors and, under
  `experiment run --backend mncs-research-bytecode
  --grant-read probe_reader=probe-host-read-grant.txt`, returns 17 for
  the 17-byte grant with effect
  `{kind: host_read, target: blob_read, capability: probe_reader,
  provenance: grant:<path> sha256:5311ca79…}` (`steps: 3`,
  `expectation_met` + `effects_met`). Fail-closed: no grant ->
  `unsupported`; wrong capability -> `invalid_request`; 65-byte grant
  file -> refused before execution (exit 2). Cost: ~0.13 s per minimal
  invocation (per-invocation backend compile) versus ~10 ms per
  dependency-free `execute` kernel call — unusable per file/kernel
  call, and there is still no directory traversal, metadata, or watch
  surface at all, so enumeration/admission stays host-side.
  Runner: `pressure/reproducers/run-language-probes.sh`;
  regression: `tests/test_language_probes.py` (granted + fail-closed).
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
- Slice-4 verdict 2026-09-07 (`index/phase2-pressure`): **partially
  available — verify-only SHA-256 confirmed at source level on a known
  vector.** Smallest legal program
  `pressure/reproducers/probe-sha256-abc.mncs` (`mncs 0.10`,
  `capability verifier`, `effect sha256_digest authorized_by verifier`,
  `return sha256_digest(msg);`) elaborates with zero `MNE`/`MNB`/`MNP`
  errors and, under `--grant-crypto verifier`, digests `abc` to exactly
  `ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad`
  (NIST vector, independently checkable via `printf abc | sha256sum`;
  `steps: 2`, `expectation_met` + `effects_met`, effect provenance
  `crypto:sha256`). Fail-closed without grant (`unsupported`). Limits:
  verify-only (no keygen in-language), operands are public views
  ≤64 B, experiment path only at ~0.13 s per minimal invocation —
  cannot back per-window kernel hashing (16k calls/MB). The dual-digest
  workaround (MNCS fold + host SHA-256 cross-check) stays.
  Corpus: `pressure/reproducers/probe-sha256-abc-corpus.json`;
  regression: `tests/test_language_probes.py::test_sha256_nist_abc_vector`.
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
- Slice-4 verdict 2026-09-07 (`index/phase2-pressure`): **partially
  available — clock half confirmed, watch half still missing.**
  Smallest legal program `pressure/reproducers/probe-clock.mncs`
  (`mncs 0.10`, `use mncs.std.clock.v1 as clock`,
  `capability ticker`, `effect clock_read authorized_by ticker`,
  `clock.expired(0, clock_read())`) elaborates with zero
  `MNE`/`MNB`/`MNP` errors and, under `--grant-time ticker`, returns
  `true` (`steps: 6`, `expectation_met` + `effects_met`, effect
  `{kind: clock_read, target: clock_read, capability: ticker,
  provenance: host-clock:wall}`). Only relational booleans are pinned —
  absolute instants are wall time and must never be asserted.
  Fail-closed without grant (`unsupported`). Cost: ~0.5 s per minimal
  invocation (stdlib compile). No filesystem-event/watch effect exists
  at source level, so the polling watcher + quiet-period coalescing
  workaround stays.
  Corpus: `pressure/reproducers/probe-clock-corpus.json`;
  regression: `tests/test_language_probes.py` (granted + fail-closed).
- Update (gap review): the watcher no longer trusts hints alone. A
  same-size/same-CRC32 content change leaves `snapshot_id` identical, so
  hint-gated triggering would keep HEAD stale forever; every
  `validate_every`-th quiet window (default 4) now runs the full
  incremental transaction regardless, and a validation that finds every
  verdict 0 publishes nothing (`tests/test_watch.py::
  test_watch_detects_hint_collision` simulates the collision by blinding
  hints). Bound: hint-invisible changes surface within `validate_every`
  quiet windows. The watcher also follows the store's richness
  (`--rich` runs the v2 transaction over the same concurrent acquisition
  stage as builds; `test_watch_rich_keeps_tables`) instead of silently
  degrading v2 stores to v1.
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
- Update 2026-09-07 (`index/phase2-pressure`, slice 1): hint-hit paths in
  `incremental_snapshot` (`runner/mncs_index/indexer.py`) are now
  authoritatively validated — size/CRC32 no longer reuse records without
  a recomputed MNCS digest — which prices a no-op incremental at ~1 full
  build on the fixture corpus: 150 vs 142 kernel calls, 28.6 s vs
  25.6 s wall (`BuildConfig(workers=4, mncs_digest=False)`, fresh kernel
  caches; the +8 calls are the per-file `classify_change` verdicts on
  the fresh digests). Correctness demands it — a hint cannot prove
  sameness — but the incremental fast path for unchanged files is gone;
  a content-defined strong hint (stored digest/byte-identity) is future
  work. Regression tests:
  `tests/test_incremental.py::test_same_size_change_detected`,
  `test_simulated_crc32_collision_detected` (simulated same-size CRC32
  collision -> verdict 3 + fresh digest, never stale reuse).
- Update (gap review): two more prices measured. (1) Watch-level
  authoritative validation costs one full incremental transaction per
  validation window even when nothing changed — correctness over
  benchmark numbers. (2) Multi-repo ecosystem evidence
  (`scripts/run_ecosystem.py`, `evidence/ecosystem-mncs-repos.json`):
  real sibling-repository corpora indexed at multiple worker counts with
  converging canonical hashes; call counts and wall time scale with the
  subprocess-per-call model, which is now the binding constraint on
  corpus size, not kernel expressiveness.

## PRESS-011 — Coarse traversal diagnostics (P3, diagnostics, compiler)

- Observed: `MNB101` span covers the entire file (start 0, end N) rather
  than the offending `iterate`; no documented list of traversable element
  types exists, so discovery was trial-and-error (`i64` ok, `u64` no).
- Desired: precise spans and a documented domain-eligibility rule.
- Reproducer: `pressure/reproducers/traversal-u64-domain.mncs` (also the
  PRESS-005 reproducer) — the `[u64; 4]` variants fail MNB101 with a
  whole-file span while `[i64; 4]`/`[byte; 4]` elaborate cleanly.
- Cost: ergonomic; slowed kernel development by ~30 minutes.
- Classification: compiler diagnostics.

## PRESS-012 — No cancellation semantics (P2, runtime, language)

- Observed: no deadlines, cancellation tokens, or scoped shutdown in any
  profile; RFC 0011 (nondeterminism/failure) is `PARTIAL` without these.
- Desired: structured cancellation propagation into blocked kernel calls.
- Workaround: host cancel events checked between items plus the drain
  protocol; in-flight subprocess calls run to completion (bounded by the
  60 s timeout). Tested by cancellation tests.
- Update 2026-09-07 (`index/phase2-pressure`, slice 3): the same pattern
  now covers blocked readers — readers check cancel between queue items,
  wake via sentinels/timeout-get, and in-flight file reads run to
  completion (bounded by MAX_FILE_BYTES). Tested by
  `tests/test_concurrent_read.py::test_cancellation_releases_blocked_readers`
  (cancel lands mid-blocked-read -> prompt `BuildCancelled`, no hang).
- Cost: shutdown latency is host-bounded, not language-guaranteed.
- Classification: runtime, language semantics.

## PRESS-013 — No durable compare-and-swap for publication (P1, synchronization/persistence, runtime)

- Observed: generational HEAD publication must be atomic across
  overlapping writers, including separate processes (two indexers racing
  on one store directory). `mncs-language` RFC 0010 implementation
  status is `NONE` (no threads, locks, atomics, or CAS even in-memory —
  PRESS-001/PRESS-002) and RFC 0026 (persistence) is `NONE`, so there is
  no language-level durable transaction, check-and-set, or file-lock
  effect to express "publish generation N iff HEAD is still M".
- Desired: an effect-gated durable compare-and-swap (or minimal
  transaction: compare HEAD, materialize generation file, swap HEAD as
  one atomic step) callable from MNCS, so publication safety is a
  language-level contract rather than host filesystem code.
- Workaround: host-side CAS in `Store.publish`
  (`runner/mncs_index/store.py`): the base re-read, the
  `expected_base` comparison (`StaleGenerationError` on mismatch), the
  generation-file materialization, and the HEAD swap run as one critical
  section under an exclusive `flock` on `.HEAD.lock`. Every writer must
  pass its build base explicitly — there is no blind-publish path — and
  candidate generation numbers must advance by exactly one. Readers need
  no lock: the generation file is `os.replace`d before the HEAD swap,
  and the HEAD swap itself is atomic, so `load_head` only ever observes
  complete, hash-validated snapshots.
- Cost (measured 2026-09-07, `index/phase2-pressure`, slice 2):
  uncontended CAS publish of a small snapshot costs p50 0.78 ms /
  p95 1.14 ms / max 2.66 ms (n=100 sequential publishes) — the lock and
  the extra HEAD re-read are noise next to one MNCS kernel subprocess
  call (~10 ms idle). Contended outcome is exact, not statistical:
  3-thread overlap yields 1 winner + 2 `StaleGenerationError`, and a
  fork-based two-process race yields 1 winner + 1 stale rejection, with
  HEAD always holding the winner's hash-validated content.
- Why the workaround is insufficient: mutual exclusion across processes
  lives in a host `fcntl` lock MNCS cannot name, audit, or test; a stale
  loser's only signal is a host exception type, not a language-level
  transaction verdict; lock-holding a filesystem path cannot compose
  with future MNCS task structure.
- Evidence: `tests/test_publish.py` (reversed completion, three-way
  thread overlap, cross-process fork race, failure/cancellation overlap,
  readers-during-publish); mutation probe (CAS check disabled ->
  reversed/overlap/contract tests fail: stale generation becomes HEAD).
- Classification: runtime/stdlib (missing durable sync), language
  semantics (no transaction effects).

## PRESS-014 — Unbounded text plumbing for rich extraction (P2, stdlib)

- Observed: the canonical-v2 extractor (`src/extract.mncs`,
  `runner/mncs_index/extract.py`, RFC 0007) decides per line, but MNCS
  has no unbounded strings, no line splitting, no substring slicing, and
  no corpus-scale sort (PRESS-005). Kernel windows are `[byte; up_to
  64]`; names/targets/titles are unbounded file text.
- Desired: chunked line cursors, bounded span slicing, and a specified
  sort/dedup story so the whole extraction pipeline is in-language.
- Workaround (all in `extract.py`, every verdict in MNCS): the host
  splits `\n`, strips one `\r`, trims leading space/tab, and feeds the
  kernel the ≤64 B prefix for `classify_decl`/`heading_level`; link
  seams past the first window are decided per overlapping 64 B window
  (stride 63, verdicts OR-ed); names are sliced dotted runs (≤64 B,
  every segment an `is_symbol_token` verdict); link targets are sliced
  `](...)` spans (no nesting, ≤256 B, UTF-8-strict); PRESS candidates
  are `PRESS-` memcmp windows decided by `is_press_id`; RFC edges pair
  adjacent kernel-shaped tokens in line-token order. Overlong, invalid,
  or undecodable spans are skipped, never truncated into a wrong
  identity. Global sort/dedup/seq assignment is host code over kernel
  verdicts in `globalize`. Differentially pinned on seeded random inputs
  (`tests/test_rich_model.py::test_extract_differential_random`); the
  64 B-boundary seam is pinned (`test_overlong_and_foreign_bytes_skipped`).
- Cost: the extractor's control plane is host code; only the verdicts
  are MNCS meaning.
- Update (gap review): two further meaning decisions live outside MNCS
  verdicts and are pinned only by host-side differential tests, not by an
  in-language specification — the cross-window word-overcount repair in
  `pipeline.py` (host-coded space policy over `classify` verdicts) and
  long-needle (`>8 B`) term substring in `query.py` (pure host scan; the
  differential suite pins only the short-needle kernel path). Likewise
  the merge contract is now explicit: `globalize` dedups headings on the
  full row and sorts full tuples, so output depends on the row multiset,
  never on worker arrival order (pinned by
  `tests/test_rich_model.py::test_globalize_permuted_rows_converge`).
  Truncation-vs-skip and span-boundary choices remain meaning decisions
  awaiting chunked-span semantics upstream.
- Classification: stdlib (missing collections/text), compiler (domain rule).

## PRESS-015 — No relation values or graph traversal (P3, semantics)

- Observed: relationships (`defines`/`references`/`depends-on`/`rfc-ref`)
  are host-side index entries over kernel-extracted rows. MNCS has no
  relation/table values, joins, or transitive-closure construct to query
  in-language, so even single-hop lookups (`query.py` v2 methods) and
  the merge dedup live in the host.
- Desired: first-class edge sets with deterministic dedup/order plus a
  bounded transitive-traversal construct (for real dependency
  invalidation, not just edges).
- Workaround: per-file rows merged by identity sort in `globalize`;
  single-hop dictionary indexes in `QueryEngine`. Transitive queries
  (dependents-of-dependents, invalidation sets) are not offered.
- Cost: the dependency/invalidation graph exists as edges only; Phase 2
  "dependency/invalidation graph" stays open (full revalidation, no
  invalidation sets).
- Update (gap review): rename lineage over those edges is a host-advisory
  layer (`runner/mncs_index/lineage.py`) precisely because digest-keyed
  verdict maps cannot be expressed in MNCS: `moved` requires exactly one
  removed and exactly one added path sharing an authoritative MNCS
  content digest, every ambiguous shape degrades to remove+add, and
  emission is sorted so caller order cannot leak. Verdicts are untouched
  (a `moved` pair still carries (2, 1)). Tested by
  `tests/test_graph_invalidation.py` (fan-out, cycles, duplicates,
  deterministic ambiguity).
- Classification: language semantics, stdlib.

## Not pressure (deliberate non-findings)

- Source Profile 0.8 was sufficient for every kernel (records, `select`,
  wrapping arithmetic, views, `u64` shifts, calls across functions).
  No parser, type-system, or elaboration blockers were hit beyond the
  entries above.
- `execute` accepts `.mncs` source directly with precise, coded
  diagnostics (`source-study`); the JSON request/response ABI is stable
  and documented by example. Unknown functions surface as typed
  `invalid_request` statuses, not crashes.
