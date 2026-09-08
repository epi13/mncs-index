# RFC 0009 — Multiprocess MVCC: Snapshot Handles, Pins, Reclamation

- Status: Accepted (implemented on `index/phase3-systems`, slice 2)
- Project: mncs-index
- Builds on: RFC 0008 (durable generational store)

## Summary

RFC 0008 made publication crash-safe but left two questions open
(the audit-2 open Qs): readers had no handle — they read HEAD then
loaded the generation (HEAD-chasing, with a TOCTOU window once old
generations can be deleted) — and there was no generation
reclamation policy at all (stores grew without bound, and any future
deletion could strand a concurrent reader). This RFC adds explicit
snapshot handles (`PinnedSnapshot`), cross-process generation pins
(`.pins/`), and reclamation of unpinned generations only
(`Store.reclaim`), with a `.reclaimed` record so `check()` tells
reclaimed-absent apart from lost-corrupt.

## Handles (never chase HEAD)

`Store.open(generation)` / `Store.open_head()` returns a
`PinnedSnapshot` binding exactly one complete, hash-validated
generation (snap + established + crc + pin path). The pin file is
created *before* the load, so a concurrent `reclaim` in any process
cannot delete the generation between bind and read. With an explicit
generation the load failure propagates (and unpins again); with
`None` (HEAD), a `file missing` load — the generation was reclaimed
after the HEAD read because HEAD moved on — releases the pin and
retries on the newer HEAD (bounded, `_OPEN_RETRIES`). Readers
therefore never re-read HEAD themselves and never observe torn
state; publishing newer generations never mutates a bound handle.
Handles close explicitly (`close()`, idempotent, or context
manager); a crashed holder's pin is reaped later by PID liveness.

## Pins (simplest correct host model)

Pin files live in `.pins/`, named
`gen-NNNNNN.<pid>.<unique>.pin` (pid names the pinner for reaping;
unique suffix keeps concurrent handles in one process distinct),
containing generation/pid/timestamp. Creation fsyncs the file; the
directory sync is best-effort (a lost pin only loses protection the
pinner re-establishes by re-opening). `list_pins()` reports live
pins only (dead pids excluded, non-mutating);
`reap_stale_pins()` deletes dead-pid pins and returns them.
Liveness is an `os.kill(pid, 0)` probe, conservative toward *alive*
on any unexpected error. Known hole, stated not hidden: PID reuse
can transiently resurrect a dead pin — the consequence is only
delayed reclamation, never a deleted live generation, because a pin
only ever *protects*. The missing lease/epoch primitives that would
close this hole are recorded as PRESS-017.

## Reclamation (only the unpinned)

`Store.reclaim(keep_recent=0)` runs under the exclusive `.HEAD.lock`
(serializing with publishers and fellow reclaimers) and deletes ONLY
generations that are present on disk, strictly below HEAD, and
covered by no live pin — after reaping stale pins first, so a
crashed reader cannot block reclamation forever. It never deletes
HEAD itself, never deletes a newer-than-HEAD crash artifact (stays
for an operator), and never touches staging files (see `repair`) or
pins. `keep_recent=K` retains the K newest otherwise reclaimable
generations as a bounded tail cache. The report
`{head, removed, reaped_stale, pinned}` is JSON-able and printed by
`mncs-index reclaim --store DIR [--keep-recent K]`.

Every deletion is unioned into the `.reclaimed` record (atomic
tmp-write + rename under the same lock), and `check()` exempts
recorded generations from `GEN_GAP`: reclaimed-absent is
expected, while a manually deleted (or rotted) file still reports
`GEN_GAP` — fail closed. `check()` gains an additive `reclaimed`
key; all prior keys and codes are unchanged. `repair()` still
removes only staging files; pins, generations, and `.reclaimed`
are never auto-touched.

Slice-3 note (RFC 0010): the record update is now *record-first
and per-deletion* (entry lands before its unlink, plus a directory
fsync each step) in both `reclaim` and `compact`, closing the
crash window between bulk unlinks and the end-of-run record write.
Report shape and `check()` semantics are unchanged.

## Crash / recovery interplay

- Reader open across a mid-commit death: the pinned pre-crash
  handle stays valid; post-crash state is old-complete or
  new-complete per RFC 0008; `check`/`repair` never disturb pins.
- Orphan pins (reader `os._exit`/SIGKILL without close) do not
  wedge the store: the next `reclaim` reaps them by liveness probe.
- Pin durability across OS crash is explicitly *not* required:
  dead processes' pins are reaped, live processes re-open.

## Portability

POSIX `flock` (Windows: `msvcrt.locking`) serializes
publish/reclaim as before; PID liveness uses `os.kill(pid, 0)`
(PRESS-017 notes the absence of a portable lease primitive).
Directory fsync keeps the RFC 0008 posture (POSIX strict,
Windows best-effort).

## Verification

- `tests/test_mvcc.py` (17 tests): handle binding across publish;
  explicit-generation open; empty/missing open fails closed with no
  leaked pins; bad pin/reclaim args rejected; reclaim keeps HEAD +
  pinned only; `keep_recent` tail; newer-than-HEAD preserved;
  empty-store reclaim; manual delete still `GEN_GAP`; `check` ok
  with pins + `repair` preserves them; orphan-pin reap unblocks
  reclamation (real fork + `os._exit` death); pinned reader across
  real-death crash + recovery (child exit 99, `NEWER_THAN_HEAD`,
  old-complete reads, clean republish); 1-writer + 4-reader
  processes with per-generation consistency; 3-writer + 2-reader
  processes with exactly-1-winner CAS; threaded readers during
  publish; `reclaim` CLI.
- Prior suites unmodified and green (durability barrier counts,
  crash matrix, publish races, snapshot isolation).
