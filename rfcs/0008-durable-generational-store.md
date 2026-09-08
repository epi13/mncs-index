# RFC 0008 — Durable Generational Store: Crash and Durability Model

- Status: Accepted (implemented on `index/phase3-systems`, slice 1)
- Project: mncs-index

## Summary

The snapshot store (`runner/mncs_index/store.py`) is the local
durability substrate: writers publish immutable, hash-validated
generations and swap a HEAD pointer under an inter-process lock.
Prior to this RFC, publication was logical tmp/rename HEAD-swap
atomicity only — no `fsync`, no directory sync, no stated crash
model, a bare POSIX-only `import fcntl`, and a `head()` that treated
a malformed HEAD as an empty store. This RFC defines explicit
durability levels (L0–L3), the `fsync` commit protocol, the crash
model, and the recovery contract. Logical semantics (RFC 0003:
canonical bytes, generation CAS, snapshot isolation) are unchanged.

## Logical semantics (unchanged)

- Generations are immutable, content-addressed snapshots numbered
  `0, 1, 2, …`; each publish advances exactly one from an explicit
  `expected_base` (compare-and-swap; losers get
  `StaleGenerationError`).
- Readers bind one complete, hash-validated snapshot; concurrent
  publication never mutates a loaded snapshot.
- All readers fail closed: a missing file, truncated/malformed JSON,
  embedded-generation mismatch, or hash mismatch raises `StoreError`.
  A torn generation is never returned. A missing HEAD is "empty" only
  when no generation files exist; data without a pointer raises
  `StoreError` instead of masquerading as an empty store.

## Durability levels

| Level | Barriers on publish | Guarantee |
|-------|---------------------|-----------|
| L0 | none (atomic renames only) | Safe across *process* crash (old-complete or new-complete); may lose or roll back across OS crash / power loss. |
| L1 | file `fsync` on generation + HEAD files before their renames | File contents durable; a rename (directory entry) may still be lost across OS crash. |
| L2 (default) | L1 + directory `fsync` after each rename | **Durable-after-ack** across OS crash / power loss on a correctly behaving POSIX filesystem. |
| L3 | L2 + post-commit verification re-read | Ack implies durable *and* verified readable (HEAD + generation re-opened from disk, hash-validated). |

`Store(directory, durability=...)` selects the level (default L2);
`build --durability` exposes it; an unknown level is a `StoreError`.

## Commit protocol

Generation temp write (per-writer unique name) → flush → file fsync
(L1+) → under `.HEAD.lock`: re-read HEAD, base check, rename
generation into place → directory fsync (L2+) → write `.HEAD.tmp` →
flush → file fsync (L1+) → rename HEAD into place → directory fsync
(L2+) → verification re-read (L3). Generation-first ordering means a
crash between the two renames leaves a valid-but-unreachable newer
generation (`NEWER_THAN_HEAD`), never a dangling HEAD.

**Durable-after-ack:** if `publish` returned at L2/L3, any later
reader — after any crash — observes that generation or a newer one,
never an older or torn one. A crash *before* acknowledgement may land
on either side (old-complete or new-complete); that ambiguity is
inherent to acknowledged-vs-committed protocols and is not torn
state. L0/L1 make the same old-or-new promise for process crashes
only.

Crash injection: `crash_at` / `MNCS_INDEX_CRASH_AT` arms `os._exit`
(no cleanup, no flush) at the eight `CRASH_POINTS` boundaries, at
every level. `os._exit` in a subprocess is a real crash; the lock is
an OS-held `flock`, released by the OS on process death, so a crashed
holder never wedges later writers.

## Recovery contract

`Store.check()` is read-only and returns
`{head, generations, issues, ok}`. `mncs-index check --store DIR`
prints it (exit 0 iff `ok`); `--repair` additionally removes orphan
staging files and re-checks. Issue codes: `HEAD_MISSING`,
`HEAD_MALFORMED`, `HEAD_DANGLING`, `GEN_UNREADABLE`,
`GEN_HASH_MISMATCH`, `GEN_NUMBER_MISMATCH`, `GEN_GAP`,
`NEWER_THAN_HEAD`, `ORPHAN_TMP`.

No silent guessing: the only automatic repair is deleting staging
files (`.HEAD.tmp`, `.gen-*.tmp`), which no read path consults.
HEAD problems, corrupt generations, and gaps are never
auto-resolved — no invented HEAD values, no deletion of generation
data, no promotion of `NEWER_THAN_HEAD` — they stay reported for an
operator. Startup performs no repair at all (offline `check
--repair` avoids racing live writers).

## mncs-store boundary

No `mncs-store` checkout exists in the ecosystem evidence
(`evidence/ecosystem-mncs-repos.json`), so this section is a forward
boundary, not an integration report. The file store is the local
durability substrate; if a shared `mncs-store` backend appears, the
integration surface is: the `Store` logical contract (generation CAS,
`expected_base`, `StaleGenerationError`, fail-closed loads), the
`check()` report schema above, and the on-disk layout
(`gen-NNNNNN.json`, `HEAD`, `.HEAD.tmp`, `.gen-*.tmp`,
`.HEAD.lock`). Logical semantics are backend-independent: any
replacement must preserve old-or-new-never-torn publication,
durable-after-ack at L2-equivalent, and the no-guessing recovery
contract — verified by porting `tests/test_durability.py`.

## Portability

`fcntl`/`msvcrt` locking is selected behind a platform guard
(`runner/mncs_index/store.py`: `_lock_file`); POSIX keeps `flock`
semantics, Windows uses `msvcrt.locking`. Directory `fsync` is
POSIX-only: on Windows L2 directory sync is a documented
best-effort no-op (`_fsync_dir` returns False), so Windows L2
carries L1 guarantees — stated here, not silently degraded on POSIX
(where a directory-sync `OSError` propagates). Durability effects
(`fsync`, dir sync, file locks) are host-OS primitives with no MNCS
expression; recorded as PRESS-016.

## Verification

- `tests/test_durability.py`: barrier counts per level (L0 0/0, L1
  2/0, L2 2/2, L3 2/2 + verified ack); real-death crash at all eight
  boundaries (child exit code 99 asserted; post-crash HEAD side and
  exact `check()` codes asserted); corruption matrix failing closed
  (loads raise, `check` reports, `repair` touches tmps only); `check`
  command exit codes and `--repair` semantics.
- Prior suites unmodified and green: `test_publish.py`,
  `test_snapshot_isolation.py`, `test_failure.py` (durable default
  changes no logical outcome).
