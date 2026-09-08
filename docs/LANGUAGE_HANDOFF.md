# Language Handoff (mncs-index → mncs-language)

Consolidation slice 7 (`index/phase3-systems`, 2026-09-08). Every
pressure reproducer was rerun against the current `mncs-language`
binary (`mncs 0.1.0`); per-item verdicts and executable acceptance
tests live in `pressure/registry.md`. This document organizes the same
material **by unblock value for mncs-index**: what to build first, what
host code each item deletes, and which items depend on which.

Ground rules (unchanged): `mncs-index` was not and will not be weakened
to avoid a gap; workarounds are host-side and labeled; a gap closes
only when the original pressure case passes (acceptance test per
significant item, all recorded in the registry).

## Resolved upstream (no action needed)

| ID | Fix | Evidence |
|----|-----|----------|
| PRESS-004 (P2) integer bitwise ops | stage-b1: `^ & \|` total over all 8 widths | `xor_u64(12,10)=6`, `and_u64=8`, `or_u64=14`; `test_int_bitwise_u64_fixed_slice7` |
| PRESS-009 (P3) nested `iterate` | profile 0.11: two-level nests, distinct identities | 0.8 still MNE147 (pinned); 0.11 executes; `test_nested_two_level_profile011_slice7` |

Kernels stay as-is (valid on every profile); FNV-style folds and
two-dimensional scans are now available to future kernels.

## Tier A — architectural (a whole subsystem stays host-side until this lands)

**A1. PRESS-010 — batch/in-process kernel invocation.** The binding
constraint on everything: one subprocess per call (~10 ms idle,
200+ ms loaded; 396.7 calls/KB → ~804k calls projected for the census).
Unblocks corpus scale directly and is the ordering dependency for all
of distribution (PRESS-019). Deletes: per-call `subprocess.run` in
`runner/mncs_index/bridge.py`. Gap-review note: call counts are now
reproducible measurements, not samples (producer memo reads take the
kernel lock — `test_kernel_call_counts_reproducible`), so the
396.7 calls/KB census projection is a stable basis for sizing this
work.

**A2. PRESS-001/002 — tasks + bounded channels with close semantics.**
The entire pipeline fan-out/fan-in, backpressure, and shutdown
(`pipeline.py`, `discover.py` read stage) stay host-side. Threads
without channels (or vice versa) do not unblock this: the requirement
is spawn + join/cancel propagation + an MPMC bounded channel with
close. Deletes: `ThreadPoolExecutor` pools, `queue.Queue` stages, the
hand-rolled drain protocol.

**A3. PRESS-003 — effect-gated traversal/read.** Provenance roots in
host code (`discover.py` walk/read/normalize). The 64 B grant-gated
`host_read` is a blob primitive, not traversal: directory enumeration,
metadata, and watch admission are all still missing. Deletes: host
walk + path-normalization policy (differentially untestable today —
there is no MNCS string to compare with).

**A4. PRESS-013/016/017 — durable transaction, commit, and snapshot
handles.** Publication safety (`flock` CAS + `StaleGenerationError`),
crash durability (`os.fsync`/dir-sync levels L0–L3), and reader
protection (`.pins/` + PID probing with a known PID-reuse hole) are
three faces of one missing capability: named, effect-gated durable
state transitions. RFC 0026 is `NONE`. Deletes: `.HEAD.lock`
protocol, `DURABILITY_LEVELS` barrier code, `.pins/` + liveness
probing in `runner/mncs_index/store.py`. Gap-review notes: (a) the
reclaim-race retry keys off the `GenerationMissingError` TYPE, not a
message substring; (b) the reclamation journal is checksummed and
fail-closed (`GEN_GAP` on forgery); (c) OS-crash survival is
protocol-construction, suite-proven only for barrier issuance +
process death — stated, not hidden; (d) Windows no-op/lock branches
are stub-tested. Index-vs-store split is specified in RFC 0008
§mncs-store boundary (needs list) — port `test_durability.py`,
`test_mvcc.py`, `test_compact.py` as the integration gate.

**A5. PRESS-019 — fabric transport, merge wire format, loss semantics.**
Gated BLOCKED behind A1 (per-item remote dispatch is uneconomical at
64 B-window subprocess cost regardless of transport) plus A2
(retry/duplicate/reorder needs task + channel semantics). The three
missing pieces have exact boundaries in the registry; the reopen bar
is a three-part acceptance test. Deletes: nothing yet — distribution
was not faked, so there is no host code to remove, only local-only
builds to extend.

## Tier B — performance (correct but uneconomical)

**B1. PRESS-006 — in-kernel digest.** Dual digests (MNCS fold + host
SHA-256) cross-check each other, but the headline hash is
half-outsourced and the verify-only grant path (~0.13 s/call, ≤64 B
views) cannot back per-window hashing (16k calls/MB). Either a fast
digest primitive or an explicit non-goal with a bridging contract.
Deletes: host SHA-256 cross-check in the canonical-hash path.

**B2. PRESS-005 — `u64` traversal domains.** Still MNB101 (whole-file
span). Forces ~8x call amplification (byte windows vs word kernels)
and a host scale-out layer maintained beside every kernel spec.
Deletes: window-shredding + per-window verdict-table application in
the pipeline (every mirrored rule stays differentially tested until
then — `tests/test_differential.py`).

**B3. PRESS-014 — unbounded text plumbing.** `text_scan`/`text_view`
are still 64 B-window bounded, so line splitting, span slicing,
sort/dedup, and the cross-window word-overcount repair live in
`extract.py`/`pipeline.py`/`query.py` as host control planes over
kernel verdicts. Deletes: host split/slice/sort/dedup in extraction
and merge (`globalize`).

## Tier C — semantic (expressiveness gaps with working host cover)

**C1. PRESS-008 — typed multi-error + cancellation-distinct
propagation.** Host failure tree (`Pipeline._fail`, first-error
capture, stop broadcast). RFC 0011 is `PARTIAL`. Depends on A2:
without tasks there is nothing to propagate through.

**C2. PRESS-012 — deadlines/scoped cancellation.** Host `cancel_event`
checked between items; in-flight calls run to completion (60 s
bound). Depends on A2 (and C1 for the typed distinction).

**C3. PRESS-015 — relation/table values + bounded traversal.**
Single-hop host indexes + `graph.py` BFS over host adjacency; only
per-candidate admission verdicts (`should_visit`, `depth_next`) are
in-language. The index-side traversal is implemented and tested
(`tests/test_invalidation.py`); what is missing is expressing edge
sets, joins, and closure in MNCS. Depends on B3 (rows are text
plumbing products). Gap-review notes: `max_nodes` is a GLOBAL
multi-root cap (shrinking sub-budgets); the kernel/host split is now
contractual — call sites pre-check `visited` (kernel branch pinned
by direct unit test), admission precedes every `_step` (u64 wrap
characterized, unreachable in walks).

**C4. PRESS-018 — compaction/GC effects.** Retention schedules + byte
metrics + `os.unlink` reclamation in `Store.compact`. Depends on A4
(reader-protection contract: reclaiming exactly the unpinned needs
language-grade pins, PRESS-017) and host-level durability (PRESS-016).

**C5. PRESS-007 — watch/clock.** Clock half partially available
(relational booleans only — absolute instants must never be
asserted); watch half entirely missing (RFC 0023 `NONE`), so the
polling watcher + `validate_every` full-validation fallback stays.
Depends on A3 (event effects are filesystem effects).

## Tier D — ergonomics/diagnostics

**D1. PRESS-011 — coarse MNB101 span + undocumented domain rule.**
Still whole-file (0–615) with no traversable-type list; slowed kernel
work by ~30 min. Note the contrast: MNE147 is now precise (inner
`iterate`, 12:9) — domain diagnostics should match that bar. Pairs
with B2 (fix the rule and the span together).

**D2 (resolved). PRESS-009** — see above; third-level nests and
sub-0.11 profiles still refuse by design.

**D3 (resolved). PRESS-004** — see above.

## Host-deletion map (what each fix removes)

| Pressure | Host code deleted on fix | File |
|----------|--------------------------|------|
| 001/002 | thread/kernel pools, bounded queues, drain protocol | `pipeline.py`, `discover.py` |
| 003 | walk/read/normalize, polling admission | `discover.py` |
| 006 | host SHA-256 cross-check | canonical-hash path |
| 005 | window shredding, verdict-table scale-out | pipeline + `kernels.py` caches |
| 008 | first-error capture, stop broadcast, `MNCSError` map | `pipeline.py`, `errors.py` |
| 010 | per-call `subprocess.run` driver | `bridge.py` |
| 012 | `cancel_event` + sentinel/timeout drain | `pipeline.py`, `discover.py` |
| 013 | `flock` CAS section, `StaleGenerationError` | `store.py` |
| 014 | host split/slice/sort/dedup, seam repair | `extract.py`, `pipeline.py`, `query.py` |
| 015 | dictionary indexes, BFS/ordering sets | `query.py`, `graph.py` |
| 016 | `DURABILITY_LEVELS` barriers, `check` fsync audit | `store.py` |
| 017 | `.pins/` files, PID-liveness reaping | `store.py` |
| 018 | retention strings, `os.unlink` reclamation | `store.py` (`compact`) |
| 007 | polling loop, quiet-period coalescing, `validate_every` | `watch.py` |
| 019 | (none — not faked) local-only assumption | pipeline/merge |
| 004/009 | (none — workaround retained deliberately) | `digest.mncs` fold, factored loops |

## Pressure dependencies

- 010 → 019 (batch API before any remote dispatch).
- 001/002 → 008, 012, 013, 017 (tasks/channels before propagation, deadlines, transactions, leases).
- 003 → 007 (traversal/mutation effects before watch events).
- 017 → 018 (pins before reclamation policy).
- 017 → 013 (lease/epoch semantics strengthen CAS composition).
- 005 ↔ 011 (domain rule + diagnostic span ship together).
- 014 → 015 (rows before relations over rows).
- 006 → census scale (digest cost gates full-corpus execution alongside 010).

## Suggested language-campaign order

1. A1 (010) — unlocks measurement of everything else.
2. A2 (001/002) — unlocks C1, C2, and the A4/A5 designs.
3. A3 (003) + C5-watch (007) — one effects family.
4. A4 (013/016/017) — one RFC-0026 family; then C4 (018).
5. B1 (006) or explicit non-goal — decides the content-addressing story.
6. B2+D1 (005/011), B3 (014), C3 (015) — the stdlib/semantics tail.
7. A5 (019) — only after 1+2 hold; the gate record
   (`evidence/fabric-scale-gate.json`) is the entry ticket.
