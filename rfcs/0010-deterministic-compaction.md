# RFC 0010 — Deterministic Compaction: In-Place Generational GC

- Status: Accepted (implemented on `index/phase3-systems`, slice 3)
- Project: mncs-index
- Builds on: RFC 0008 (durable generational store), RFC 0009
  (multiprocess MVCC: handles, pins, reclamation)

## Summary

RFC 0009 added reclamation of unpinned generations but left the
storage story incomplete: no retention *policy* (only
`keep_recent`), no cleanup of staging/sidecar files, no account of
what compaction costs or saves, and a crash window in `reclaim`
itself (the `.reclaimed` record was written once at the end, so a
death between the unlinks and the record left `GEN_GAP`). This RFC
adds `Store.compact(schedule)` — deterministic, in-place garbage
collection over the live store directory. There is no second
database and no copy phase: victims are unlinked where they live,
under the exclusive `.HEAD.lock`, and the run reports storage
metrics plus a `.compact.json` manifest.

## Schedules (pure functions of store state)

`COMPACT_SCHEDULES` advertises three schedules. Each maps
`(HEAD, on-disk generations, live pins)` to a victim set, so the
same store state always yields the same victims:

- `prune` — every unpinned generation below HEAD is a victim.
- `keep-recent:N` — as `prune`, except the N newest below-HEAD
  generations are retained (bounded tail cache, same semantics as
  `reclaim(keep_recent=N)`).
- `checkpoint:N` (`N >= 1`) — as `prune`, except every N-th
  generation below HEAD (`gen % N == 0`) is retained, so a bounded
  ladder of history survives.

Malformed schedules (`"bogus"`, `"keep-recent:"`,
`"checkpoint:0"`, non-strings, …) raise `StoreError` before
anything is touched. `dry_run=True` returns the exact victim set
and projected metrics without deleting, reaping, or writing
anything.

## What compaction deletes (and never deletes)

Deletes, in lock order: schedule victims (recorded in `.reclaimed`
*before* each unlink — see below), then orphan staging tmps, then
writes the manifest. Sidecar classes:

- Stale pins (dead-process owners) are reaped first, as in
  `reclaim`, and reported (`reaped_stale`).
- `.HEAD.tmp` present under the lock is orphan *by construction*:
  HEAD staging happens inside the same lock, so no live writer can
  own it — safe to remove.
- `.gen-NNNNNN.<pid>.<tid>.tmp` carries its writer's pid: a dead
  owner means a crashed writer's orphan (remove); a live owner
  means in-flight staging (skip and report in
  `skipped_live_tmps` — deleting it would break that writer's
  rename). Unattributable names are left for the operator `repair`
  (`skipped_unknown_tmps`).
- `.compact.tmp` (interrupted manifest staging) is removed.
- The previous `.compact.json` manifest is superseded in place by
  the new one; exactly one manifest file ever exists.

Never deleted: HEAD itself, any newer-than-HEAD crash artifact
(stays for an operator), any generation covered by a live pin, any
live pin, or any live writer's staging.

## Record-first ordering (interrupted-run safety)

Slice 3 also hardened `reclaim` to the same rule: the `.reclaimed`
entry for a generation is written (file-fsynced) *before* its
unlink, followed by a directory fsync. A crash in between leaves a
recorded-but-present generation — which `check()` exempts and a
resumed run re-victims — never an unrecorded absence. An
interrupted compaction therefore resumes to exactly the
uninterrupted end state, and `check()` never reports `GEN_GAP` for
compaction-deleted files. The manifest is written last, atomically
(tmp + fsync + rename + dir sync); a crash before it leaves
deletions recorded but no manifest, a crash during staging leaves
`.compact.tmp` (`ORPHAN_TMP`, removed by resume/repair).

Crash injection covers three points (`COMPACT_CRASH_POINTS`,
armed like `CRASH_POINTS` at every durability level):
`after-compact-delete` (after each recorded unlink),
`before-compact-manifest`, and `after-compact-manifest-write`.

## Equivalence and metrics

Queries against HEAD are byte-identical before and after: the test
battery fingerprints `index_hash` plus every query class
(path/kind/digest, short-term kernel path, long-term host path,
limits, provenance hit/miss) and asserts equality. The report is
JSON-able and printed by `mncs-index compact --store DIR
[--schedule S] [--dry-run]` (exit 0 always, like `reclaim`):

- `removed_generations`, `removed_tmps`, `reaped_stale`, `pinned`,
  `retained` (below-HEAD survivors), `newer_than_head`,
  `skipped_live_tmps`, `skipped_unknown_tmps`.
- `generations_before/after` (on-disk generation counts),
  `bytes_before/after` (generation-file bytes, re-summed
  post-state, not subtracted), `tmp_bytes_reclaimed`,
  `reclaimed_bytes` (`bytes_before - bytes_after`).
- `amplification_before/after` — generation bytes per HEAD byte
  (None when HEAD has no file); a full `prune` converges to 1.0.
- `duration_s` (wall time including lock wait — a measurement, not
  a deterministic value; determinism claims exclude it, as they
  exclude reaped pin basenames, which carry pid/uuid).

## Portability

Unchanged from RFCs 0008/0009: `flock`/`msvcrt` lock, POSIX-strict
/ Windows best-effort dir sync, PID-liveness reaping with the
PRESS-017 PID-reuse caveat (worst case delayed reclamation).
Compaction adds PRESS-018 (no storage-compaction effects in MNCS).

## Verification

- `tests/test_compact.py` (25 tests): per-schedule victim sets;
  bad-schedule rejection with the store untouched; empty store;
  dry-run purity (preview equals the later real run); query+hash
  equivalence across all schedules; metric truthfulness against a
  manual `getsize` survey (including manifest == report);
  pinned-never-deleted (handles + checkpoint interplay); live
  reader process holding an old pin across compaction (file
  rendezvous, then reclaim-after-exit); threaded readers racing
  repeated compactions; all three crash points via real-death
  subprocesses (exit 99, `check()` quiet, resume converges);
  staging triage (dead tmp goes, live tmp skipped byte-identical,
  unknown tmp left for `repair`); newer-than-HEAD preserved;
  orphan-pin reap; two-store determinism; manifest supersession;
  `compact` CLI (schedule, dry-run, fail-closed bad schedule).
- Prior suites unmodified and green (durability matrix,
  publish races, MVCC, snapshot isolation).
