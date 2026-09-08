"""Snapshot store: validated, atomic, generational publication.

Writers build a candidate privately; `publish` re-derives the canonical
bytes from the stored JSON form and compares hashes before swapping HEAD.
A failed or cancelled build never reaches `publish`, so readers only ever
see the previous complete snapshot or the new complete snapshot.

Publication is compare-and-swap: every writer declares the HEAD
generation its candidate was built on (`expected_base`), and the
read-check-swap of HEAD runs under an exclusive lock on `.HEAD.lock`,
so overlapping writers — threads or separate processes — are
serialized. A writer whose base no longer matches HEAD loses with
`StaleGenerationError` and a stale generation can never become HEAD.
Candidate generation numbers are publication metadata and must advance
by exactly one (`expected_base + 1`, or `0` from an empty store).

Durability (RFC 0008) is explicit: `Store(directory, durability=...)`
selects one of `DURABILITY_LEVELS`. The commit protocol always writes
the generation temp file first, renames it into place, then writes and
renames HEAD — two atomic renames, generation-first — and the level
selects which `fsync` barriers are issued:

- ``L0`` — no `fsync` at all, on publication *or* maintenance
  (`reclaim`/`compact`/journal). Atomic renames still make a *process*
  crash safe (old-complete or new-complete), but an OS crash or power
  loss may lose or roll back the commit — or lose the reclamation
  journal, in which case `check()` reports reclaimed generations as
  `GEN_GAP` (fail closed, never silent).
- ``L1`` — file `fsync` on the generation file and the HEAD file
  before their renames. A process crash is safe; an OS crash may lose
  a rename (directory entry) even though file contents are durable.
- ``L2`` (default) — ``L1`` plus a directory `fsync` after each
  rename. Once `publish` returns, the commit survives an OS crash or
  power loss on a correctly behaving POSIX filesystem — by construction
  of the rename+fsync protocol, not by test: the suite proves barrier
  *issuance* (mock-counted `_fsync_file`/`_fsync_dir`) and survival of
  real process death (`os._exit` at every boundary), but no test pulls
  the power cord. See the portability caveat in RFC 0008.
- ``L3`` — ``L2`` plus a post-commit verification re-read: HEAD and
  its generation are re-opened from disk and hash-validated before
  `publish` acknowledges. Acknowledgement therefore implies the new
  snapshot is not only durable but verified readable.

``durable-after-ack`` means: if `publish` returned at L2/L3, a later
reader (after any crash) observes that generation or a newer one, never
an older or torn one. A crash *before* acknowledgement may land on
either side (old-complete or new-complete) but never on torn state.

Crash injection: `crash_at` (or the `MNCS_INDEX_CRASH_AT` environment
variable, comma-separated) arms deterministic process death via
`os._exit` at any of `CRASH_POINTS`. `os._exit` skips all cleanup —
no `finally`, no stdio flush — so a test that arms a point in a
*subprocess* exercises a real crash, not an exception. The file lock
is an `flock`/OS lock, released by the OS on process death, so a
crashed holder never wedges later writers.

Multiprocess MVCC (RFC 0009) is explicit pinning, not HEAD-chasing:
readers open a `PinnedSnapshot` handle (`Store.open` / `open_head`),
which records a pin file under `.pins/` for its generation *before*
loading it, so the generation cannot be reclaimed out from under the
reader. `Store.reclaim` deletes only unpinned, non-HEAD generations
below HEAD (after reaping pins of dead processes); HEAD and any
newer-than-HEAD crash artifact are never deleted. The host model is
deliberately simple — pin files named
`gen-NNNNNN.<pid>.<unique>.pin`, liveness by PID probe — with the
missing lease/epoch primitives recorded as pressure (PRESS-017).

Deterministic compaction (RFC 0010) is in-place garbage collection
over the same directory — no second database, no copy phase.
`Store.compact(schedule)` deletes, under the exclusive `.HEAD.lock`,
exactly the generations its schedule names (a pure function of HEAD,
the on-disk set, and live pins: same state, same victims), plus
orphan staging tmps and superseded sidecars (stale pins reaped, the
previous `.compact.json` manifest superseded by the new one). It
never deletes a pinned generation, HEAD itself, or a
newer-than-HEAD artifact. Every generation deletion is recorded in
`.reclaimed` *before* the unlink (record-first), so an interrupted
run resumes to the same end state and `check()` never reports a
`GEN_GAP` for compaction-deleted files. Staging tmps
(`.gen-NNNNNN.<pid>.<tid>.tmp`) are removed only when the owner pid
is dead — a live concurrent writer's staging is skipped and
reported — while `.HEAD.tmp` under the lock is orphan by
construction (HEAD staging happens inside the same lock). The run
writes an atomic `.compact.json` manifest and reports storage
metrics (generation/file counts, bytes, amplification, reclaimed
bytes, duration); queries against HEAD are byte-identical before
and after (same `index_hash`).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
import uuid

try:  # POSIX file locking; absent on Windows (see _lock_file).
    import fcntl
except ImportError:  # pragma: no cover — platform guard (RFC 0008).
    fcntl = None  # type: ignore[assignment]

try:  # Windows file locking; absent on POSIX.
    import msvcrt
except ImportError:  # pragma: no cover — platform guard (RFC 0008).
    msvcrt = None  # type: ignore[assignment]

from .model import (
    canonical_bytes,
    canonical_bytes_v2,
    index_hash_of,
    snapshot_from_json,
    snapshot_to_json,
    snapshot_to_json_v2,
)


class StoreError(Exception):
    pass


class GenerationMissingError(StoreError):
    """A generation file is absent on disk.

    Distinct type (not a message substring) so `Store.open`'s
    reclaim-race retry keys off the contract, not the wording:
    rewording `load()` diagnostics cannot silently disable the
    retry. Pinned by `tests/test_mvcc.py::test_open_retry_contract_is_typed`.
    """


class StaleGenerationError(StoreError):
    """HEAD moved under a writer: `expected_base` no longer matches.

    The candidate was built on a stale generation and must be rebuilt
    (or dropped); it was not published and HEAD is untouched.
    """


DURABILITY_LEVELS = ("L0", "L1", "L2", "L3")

DURABILITY_DEFAULT = "L2"

DURABILITY_GUARANTEES = {
    "L0": "atomic renames only, no barriers on any path (publication "
    "or GC): safe across process crash, may lose or roll back the "
    "commit across OS crash / power loss; a lost reclamation journal "
    "reports GEN_GAP (fail closed).",
    "L1": "L0 + file fsync before each rename: file contents durable, "
    "but a rename (directory entry) may still be lost across OS crash.",
    "L2": "L1 + directory fsync after each rename: durable-after-ack "
    "across OS crash / power loss (POSIX; see RFC 0008 portability).",
    "L3": "L2 + post-commit verification re-read of HEAD and its "
    "generation: ack implies durable and verified readable.",
}

#: Commit-boundary names for deterministic crash injection. Every name
#: is a point where `_maybe_crash` runs `os._exit` when armed, at every
#: durability level (hooks fire even when the level skips the adjacent
#: sync, so each logical boundary is injectable under L0–L3).
CRASH_POINTS = (
    "after-gen-tmp-write",
    "after-gen-tmp-fsync",
    "after-gen-rename",
    "after-gen-dirsync",
    "after-head-tmp-write",
    "after-head-tmp-fsync",
    "after-head-rename",
    "after-final-dirsync",
)

#: Value of `MNCS_INDEX_CRASH_AT` entries / `crash_at` items that arms
#: no crash; accepted so harnesses can pass through an empty setting.
CRASH_NONE = frozenset({"", "none"})

#: Exit status used by `_maybe_crash`: distinctive, and unreachable by
#: normal `publish` control flow (which returns a generation number or
#: raises), so a parent process can assert *real death*, not an error.
CRASH_EXIT_CODE = 99

_GEN_RE = re.compile(r"^gen-(\d{6})\.json$")


def _fsync_file(fh) -> None:
    """Flush user-space buffers and force file contents to storage."""
    fh.flush()
    os.fsync(fh.fileno())


def _fsync_dir(dirpath: str) -> bool:
    """Force directory entries to storage.

    Returns True when a directory sync was issued. On Windows
    (`os.name == "nt"`) directory fsync is unavailable and this is a
    documented best-effort no-op returning False (RFC 0008); on POSIX
    an `OSError` propagates — L2 must not silently degrade there.
    """
    if os.name == "nt":
        return False
    fd = os.open(dirpath, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return True


def _lock_file(fh) -> None:
    """Take an exclusive, OS-released inter-process lock on `fh`."""
    if fcntl is not None:
        fcntl.flock(fh, fcntl.LOCK_EX)
    elif msvcrt is not None:  # pragma: no cover — Windows-only path.
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)
    else:  # pragma: no cover — no locking primitive at all.
        raise StoreError("no inter-process file lock available on this platform")


def _unlock_file(fh) -> None:
    if fcntl is not None:
        fcntl.flock(fh, fcntl.LOCK_UN)
    elif msvcrt is not None:  # pragma: no cover — Windows-only path.
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)


_PIN_DIRNAME = ".pins"

_PIN_SUFFIX = ".pin"

#: Pin filenames: `gen-NNNNNN.<pid>.<unique>.pin`. The pid names the
#: pinner for stale-pin reaping; the unique suffix keeps concurrent
#: pins from one process (threads, or two handles) distinct.
_PIN_RE = re.compile(r"^gen-(\d{6})\.(\d+)\.([0-9a-f]{32})\.pin$")

#: Bounded retries for `Store.open()` when a pinned generation is
#: reclaimed between the HEAD read and the pin (HEAD has moved on;
#: re-read and bind the newer generation instead of chasing).
_OPEN_RETRIES = 100

#: Staging temp names written by `Store.publish` before the lock:
#: `.gen-NNNNNN.<pid>.<tid>.tmp`. The pid attributes the staging to
#: its writer, so compaction can tell a crashed writer's orphan
#: (owner dead: safe to remove) from a live writer's in-flight
#: staging (owner alive: must be skipped, never deleted).
_TMP_RE = re.compile(r"^\.gen-(\d{6})\.(\d+)\.(.+)\.tmp$")

#: Compaction manifest: one atomic JSON record per store describing
#: the latest `compact` run (schedule, victims, metrics). Each run
#: supersedes the previous file in place; an interrupted manifest
#: write leaves `.compact.tmp`, which is staging, not state.
_COMPACT_MANIFEST = ".compact.json"

_COMPACT_TMP = ".compact.tmp"

#: Staging temp names any writer may leave behind. `.reclaimed.tmp`
#: is the reclamation journal staging (written by `_write_reclaimed`
#: on reclaim/compact paths); every name here is covered by both
#: `check()` (ORPHAN_TMP) and `repair()`.
_ORPHAN_TMP_EXACT = frozenset({".HEAD.tmp", _COMPACT_TMP, ".reclaimed.tmp"})


def _is_orphan_tmp(name: str) -> bool:
    """True for leftover staging files (never live state)."""
    return name in _ORPHAN_TMP_EXACT or (
        name.startswith(".gen-") and name.endswith(".tmp")
    )

#: Deterministic compaction schedules (RFC 0010). Each schedule is a
#: pure function of (HEAD, on-disk generations, live pins):
#: - ``prune`` — every unpinned generation below HEAD is a victim.
#: - ``keep-recent:N`` — victims as in ``prune``, except the N
#:   newest below-HEAD generations are retained (bounded tail).
#: - ``checkpoint:N`` — victims as in ``prune``, except every N-th
#:   generation below HEAD (``gen % N == 0``) is retained.
COMPACT_SCHEDULES = ("prune", "keep-recent:N", "checkpoint:N")

#: Interruption points for deterministic crash injection into
#: `Store.compact`, armed via `crash_at` / `MNCS_INDEX_CRASH_AT`
#: exactly like `CRASH_POINTS` (real death via `os._exit`, at every
#: durability level). `after-compact-delete` fires after each
#: recorded-and-unlinked generation; `after-compact-manifest-write`
#: fires after the manifest staging tmp is written and fsynced but
#: before it is renamed into place.
COMPACT_CRASH_POINTS = (
    "after-compact-delete",
    "after-compact-manifest-write",
    "before-compact-manifest",
)


def _pid_alive(pid: int) -> bool:
    """Best-effort liveness probe for stale-pin reaping.

    Conservative direction: any unexpected error (or a platform
    without `os.kill`) reports *alive*, so reaping may retain a dead
    pin but never deletes a live reader's pin. PID reuse can
    transiently resurrect a dead pin as live — the known hole of this
    simplest-correct host model (PRESS-017); the consequence is only
    delayed reclamation, never a deleted live generation, because a
    pin only ever *protects* its generation.
    """
    if pid == os.getpid():
        return True
    kill = getattr(os, "kill", None)
    if kill is None:  # pragma: no cover — no signal probe on this platform.
        return True
    try:
        kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    else:
        return True


class PinnedSnapshot:
    """An explicitly pinned snapshot handle (RFC 0009).

    Binds one complete, hash-validated generation: the pin file
    (created before the load) keeps `Store.reclaim` from deleting
    the generation while the handle is open, in this process or any
    other. Publishing newer generations never mutates the bound
    data. Close explicitly (`close()` or the context manager);
    a crashed holder's pin is reaped by liveness probe in a later
    `reap_stale_pins` / `reclaim`.
    """

    def __init__(self, store, generation, snap, established, crc, pin_path):
        self._store = store
        self.generation = generation
        self.snap = snap
        self.established = established
        self.crc = crc
        self.pin_path = pin_path
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        """Release the pin; idempotent, missing-ok (never raises)."""
        if self._closed:
            return
        self._closed = True
        try:
            self._store.unpin(self.pin_path)
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def __repr__(self) -> str:  # pragma: no cover — debugging aid.
        return f"PinnedSnapshot(generation={self.generation}, closed={self._closed})"


class Store:
    def __init__(
        self,
        directory: str,
        durability: str = DURABILITY_DEFAULT,
        crash_at=None,
    ):
        if durability not in DURABILITY_LEVELS:
            raise StoreError(
                f"unknown durability {durability!r}: expected one of "
                f"{', '.join(DURABILITY_LEVELS)}"
            )
        self.dir = directory
        self.durability = durability
        if crash_at is None:
            raw = os.environ.get("MNCS_INDEX_CRASH_AT", "")
            crash_at = (
                frozenset(p.strip() for p in raw.split(",")) if raw else frozenset()
            )
        else:
            crash_at = frozenset(crash_at)
        unknown = set(crash_at) - set(CRASH_POINTS) - set(COMPACT_CRASH_POINTS) - CRASH_NONE
        if unknown:
            raise StoreError(
                f"unknown crash point(s) {sorted(unknown)}: expected subset of "
                f"{', '.join(CRASH_POINTS + COMPACT_CRASH_POINTS)}"
            )
        self._crash_at = frozenset(crash_at - CRASH_NONE)
        os.makedirs(directory, exist_ok=True)

    def _level_at_least(self, level: str) -> bool:
        return DURABILITY_LEVELS.index(self.durability) >= DURABILITY_LEVELS.index(
            level
        )

    def _maybe_crash(self, point: str) -> None:
        """Die like a crash (no cleanup) when `point` is armed."""
        if point in self._crash_at:
            os._exit(CRASH_EXIT_CODE)

    def _maybe_crash_compact(self, point: str) -> None:
        """Die like a crash inside `compact` when `point` is armed.

        Separate name from `_maybe_crash` only for readability at
        the call sites; the arming set is the same `crash_at`, and
        hooks fire at every durability level.
        """
        if point not in COMPACT_CRASH_POINTS:  # pragma: no cover — bug guard.
            raise StoreError(f"unknown compaction crash point {point!r}")
        if point in self._crash_at:
            os._exit(CRASH_EXIT_CODE)

    def _gen_path(self, generation: int) -> str:
        return os.path.join(self.dir, f"gen-{generation:06d}.json")

    def _head_path(self) -> str:
        return os.path.join(self.dir, "HEAD")

    def _reclaimed_path(self) -> str:
        return os.path.join(self.dir, ".reclaimed")

    @staticmethod
    def _reclaimed_checksum(generations: list) -> str:
        h = hashlib.sha256()
        for g in generations:
            h.update(f"{int(g)}\n".encode())
        return h.hexdigest()

    def _read_reclaimed(self) -> set:
        """Generations deleted by `reclaim` (vs lost to corruption).

        Missing, unreadable, or checksum-invalid record means "none
        reclaimed" (fail closed toward `GEN_GAP`, never toward
        silence): a forged or torn journal entry cannot hide a lost
        generation, it only forces it to be reported.
        """
        try:
            with open(self._reclaimed_path()) as fh:
                data = json.load(fh)
        except (FileNotFoundError, ValueError, OSError):
            return set()
        try:
            claimed = sorted(int(g) for g in data.get("reclaimed", []))
        except (TypeError, ValueError, AttributeError):
            return set()
        if data.get("sha256") != self._reclaimed_checksum(claimed):
            return set()
        return set(claimed)

    def _dirsync_if(self, level: str) -> None:
        """Directory barrier when this store's level requires it."""
        if self._level_at_least(level):
            _fsync_dir(self.dir)

    def _write_reclaimed(self, generations: set) -> None:
        ordered = sorted(generations)
        payload = {
            "reclaimed": ordered,
            "sha256": self._reclaimed_checksum(ordered),
        }
        tmp = os.path.join(self.dir, ".reclaimed.tmp")
        with open(tmp, "w") as fh:
            json.dump(payload, fh, sort_keys=True)
            fh.write("\n")
            fh.flush()
            if self._level_at_least("L1"):
                os.fsync(fh.fileno())
        os.replace(tmp, self._reclaimed_path())
        # The rename itself needs a directory barrier (L2+): without
        # it an OS crash between replace and the caller's journal use
        # loses the record and turns a reclaimed victim into GEN_GAP.
        # At L0 no barriers are issued anywhere; a lost journal then
        # reports GEN_GAP (fail closed, never silent).
        self._dirsync_if("L2")

    def head(self) -> int | None:
        """Return the HEAD generation, or None for an empty store.

        A missing HEAD file means "no generation published yet". A
        present-but-unparseable HEAD is corruption, not emptiness, and
        raises `StoreError` (fail closed — never silently treated as
        empty, which would invite a generation-0 overwrite).
        """
        try:
            with open(self._head_path()) as fh:
                text = fh.read().strip()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise StoreError(f"HEAD unreadable: {exc}") from exc
        if not re.fullmatch(r"\d+", text):
            raise StoreError(f"HEAD malformed: {text!r} is not a generation number")
        return int(text)

    def _validate_stored(self, generation: int, data: dict):
        """Hash-validate decoded generation payload; raise `StoreError`."""
        try:
            snap = snapshot_from_json(data)
        except (KeyError, ValueError, TypeError) as exc:
            raise StoreError(
                f"generation {generation}: malformed payload ({exc})"
            ) from exc
        if data.get("generation") != generation:
            raise StoreError(
                f"generation {generation}: payload carries generation "
                f"{data.get('generation')!r}"
            )
        if snap.has_rich:
            expect = canonical_bytes_v2(snap.sorted_all_records(), snap.snapshot_id)
        else:
            expect = canonical_bytes(snap.snapshot_id, snap.sorted_records())
        if index_hash_of(expect) != snap.index_hash:
            raise StoreError(f"generation {generation}: stored hash mismatch")
        return snap

    def load(self, generation: int):
        """Load one generation; any corruption raises `StoreError`.

        Fails closed: missing files, truncated/malformed JSON, embedded
        generation mismatches, and hash mismatches all raise — a torn
        generation is never returned.
        """
        try:
            with open(self._gen_path(generation)) as fh:
                data = json.load(fh)
        except FileNotFoundError as exc:
            raise GenerationMissingError(
                f"generation {generation}: file missing"
            ) from exc
        except OSError as exc:
            raise StoreError(f"generation {generation}: unreadable ({exc})") from exc
        except ValueError as exc:
            raise StoreError(
                f"generation {generation}: truncated or malformed JSON ({exc})"
            ) from exc
        snap = self._validate_stored(generation, data)
        established = data.get("established", {})
        crc = {d["path"]: d.get("crc", 0) for d in data.get("docs", [])}
        return snap, established, crc

    def load_head(self):
        gen = self.head()
        if gen is None:
            # A missing HEAD is only "empty" when no generation files
            # exist either; data without a pointer is corruption (fail
            # closed) rather than an empty store.
            try:
                entries = os.listdir(self.dir)
            except FileNotFoundError:
                return None, {}, {}
            if any(_GEN_RE.match(n) for n in entries):
                raise StoreError("HEAD missing but generation files exist")
            return None, {}, {}
        return self.load(gen)

    # -- snapshot handles / pins (RFC 0009) -------------------------------

    def _pins_dir(self) -> str:
        return os.path.join(self.dir, _PIN_DIRNAME)

    def pin(self, generation: int) -> str:
        """Pin `generation` for this process; returns the pin path.

        The pin file is created (and fsynced) *before* the caller
        loads the generation, so `reclaim` in any process cannot
        delete it while pinned. Pins are advisory protection only:
        the directory sync is best-effort (a lost pin merely loses
        protection the pinner re-establishes by re-opening).
        """
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 0
        ):
            raise StoreError(f"cannot pin invalid generation {generation!r}")
        pins = self._pins_dir()
        os.makedirs(pins, exist_ok=True)
        name = f"gen-{generation:06d}.{os.getpid()}.{uuid.uuid4().hex}{_PIN_SUFFIX}"
        path = os.path.join(pins, name)
        with open(path, "w") as fh:
            fh.write(f"{generation}\n{os.getpid()}\n{time.time()}\n")
            fh.flush()
            os.fsync(fh.fileno())
        try:
            _fsync_dir(pins)
        except OSError:
            pass  # advisory pin: protection is re-established on re-open.
        return path

    def unpin(self, pin_path: str) -> bool:
        """Release one pin; missing-ok. Returns True when removed."""
        try:
            os.unlink(pin_path)
        except FileNotFoundError:
            return False
        return True

    def list_pins(self, pid_alive=None) -> list:
        """List live pin records `{"generation", "pid", "path"}`.

        Non-matching files in `.pins/` are ignored; pins of dead
        processes are excluded (non-mutating — use
        `reap_stale_pins` to delete them).
        """
        probe = pid_alive or _pid_alive
        try:
            entries = os.listdir(self._pins_dir())
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise StoreError(f"pins unreadable: {exc}") from exc
        pins = []
        for name in sorted(entries):
            m = _PIN_RE.match(name)
            if not m:
                continue
            pid = int(m.group(2))
            if not probe(pid):
                continue
            pins.append(
                {
                    "generation": int(m.group(1)),
                    "pid": pid,
                    "path": os.path.join(self._pins_dir(), name),
                }
            )
        return pins

    def pinned_generations(self, pid_alive=None) -> set:
        """Generations currently protected by a live pin."""
        return {p["generation"] for p in self.list_pins(pid_alive)}

    @staticmethod
    def _stale_pin_names(entries, pid_alive=None) -> list:
        """Basenames in `entries` whose pinner is dead (read-only).

        Factored so dry-run previews can report `would_reap_stale`
        without deleting anything. `pid_alive` is an injectable
        liveness probe (defaults to `_pid_alive`) so tests can
        simulate PID reuse: a probe that reports a dead pid alive
        models reuse, and the consequence is observable — delayed
        reclamation, never a deleted live generation, because a pin
        only ever *protects*.
        """
        probe = pid_alive or _pid_alive
        return [
            name
            for name in sorted(entries)
            if _PIN_RE.match(name) and not probe(int(_PIN_RE.match(name).group(2)))
        ]

    def reap_stale_pins(self, pid_alive=None) -> list:
        """Delete pins of dead processes; returns removed basenames.

        A reader that dies (crash, `os._exit`, SIGKILL) without
        closing its handle leaves an orphan pin that would block
        reclamation forever; reaping by PID liveness is what bounds
        that leak (PRESS-017 notes the PID-reuse caveat: worst case
        is delayed reclamation, never a deleted live generation).
        """
        try:
            entries = os.listdir(self._pins_dir())
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise StoreError(f"pins unreadable: {exc}") from exc
        removed = []
        for name in self._stale_pin_names(entries, pid_alive):
            try:
                os.unlink(os.path.join(self._pins_dir(), name))
            except OSError:
                continue
            removed.append(name)
        return removed

    def open(self, generation: int | None = None) -> PinnedSnapshot:
        """Open a pinned snapshot handle (never chases HEAD).

        With an explicit generation: pin it, then hash-validated
        load it (missing/corrupt raises `StoreError`, unpinned
        again). With `None`: bind the current HEAD — pin, then
        load; if the generation was reclaimed between the HEAD read
        and the pin (HEAD has moved on), release and retry on the
        newer HEAD. The returned handle always names one complete
        generation; readers never observe torn state and never need
        to re-read HEAD themselves.
        """
        if generation is not None:
            if (
                isinstance(generation, bool)
                or not isinstance(generation, int)
                or generation < 0
            ):
                raise StoreError(f"cannot open invalid generation {generation!r}")
            pin_path = self.pin(generation)
            try:
                snap, established, crc = self.load(generation)
            except Exception:
                self.unpin(pin_path)
                raise
            return PinnedSnapshot(self, generation, snap, established, crc, pin_path)
        for _ in range(_OPEN_RETRIES):
            head = self.head()
            if head is None:
                raise StoreError("cannot open snapshot of an empty store")
            pin_path = self.pin(head)
            try:
                snap, established, crc = self.load(head)
            except GenerationMissingError:
                self.unpin(pin_path)
                continue  # reclaimed under us; HEAD moved on — retry.
            except StoreError:
                self.unpin(pin_path)
                raise
            return PinnedSnapshot(self, head, snap, established, crc, pin_path)
        raise StoreError("cannot bind a stable HEAD: generations churned under retry")

    def open_head(self) -> PinnedSnapshot:
        """Open a pinned handle on the current HEAD generation."""
        return self.open(None)

    def publish(
        self,
        snap,
        canonical: bytes,
        established: dict,
        crc: dict,
        expected_base: int | None,
    ) -> int:
        """Validate the candidate and compare-and-swap it to HEAD.

        `expected_base` is the HEAD generation the candidate was built
        on (`None` for the first publish into an empty store); it is
        required and explicit so no writer can silently clobber a newer
        HEAD. The HEAD re-read, the base comparison, and the HEAD swap
        run under an exclusive lock, so overlapping writers serialize
        and all but one lose with `StaleGenerationError`.

        `crc` carries non-canonical content hints for the incremental fast
        path; they are stored alongside records but never hashed into
        canonical meaning.

        Snapshots carrying canonical-v2 extension tables are validated
        against the v2 bytes and stored under the v2 envelope; v1
        snapshots take the untouched v1 path (RFC 0007 migration).

        Commit protocol (RFC 0008): generation temp write → file fsync
        (L1+) → rename generation into place → directory fsync (L2+) →
        HEAD temp write → file fsync (L1+) → rename HEAD into place →
        directory fsync (L2+) → verification re-read (L3). Crash
        injection hooks fire at every boundary in `CRASH_POINTS` at
        every level.
        """
        rich = bool(
            getattr(snap, "syms", None)
            or getattr(snap, "headings", None)
            or getattr(snap, "rels", None)
            or getattr(snap, "press", None)
        )
        if rich:
            expect = canonical_bytes_v2(snap.sorted_all_records(), snap.snapshot_id)
        else:
            expect = canonical_bytes(snap.snapshot_id, snap.sorted_records())
        if expect != canonical:
            raise StoreError("candidate canonical bytes do not match records")
        if index_hash_of(canonical) != snap.index_hash:
            raise StoreError("candidate index hash does not match bytes")
        if expected_base is None:
            if snap.generation != 0:
                raise StoreError(
                    f"initial publish must carry generation 0, got {snap.generation}"
                )
        elif snap.generation != expected_base + 1:
            raise StoreError(
                f"generation {snap.generation} does not follow base {expected_base}"
            )
        payload = snapshot_to_json_v2(snap) if rich else snapshot_to_json(snap)
        payload["established"] = {k: int(v) for k, v in established.items()}
        for doc in payload["docs"]:
            doc["crc"] = int(crc.get(doc["path"], 0))
        # Stage the generation file to a per-writer temp name outside the
        # lock; it is moved into place only after the base check passes,
        # so a stale loser never leaves bytes behind at the live path.
        tmp = os.path.join(
            self.dir,
            f".gen-{snap.generation:06d}.{os.getpid()}.{threading.get_ident()}.tmp",
        )
        with open(tmp, "w") as fh:
            json.dump(payload, fh, indent=1, sort_keys=True)
            fh.write("\n")
            fh.flush()
            self._maybe_crash("after-gen-tmp-write")
            if self._level_at_least("L1"):
                _fsync_file(fh)
            self._maybe_crash("after-gen-tmp-fsync")
        try:
            lock_path = os.path.join(self.dir, ".HEAD.lock")
            with open(lock_path, "w") as lock:
                _lock_file(lock)
                try:
                    # Re-read HEAD under the lock: the base check, the
                    # generation-file materialization, and the HEAD swap
                    # are one critical section, so two overlapping writers
                    # cannot both observe the same base and both swap.
                    # A malformed HEAD raises StoreError here (fail
                    # closed) rather than losing as stale.
                    current = self.head()
                    if current != expected_base:
                        raise StaleGenerationError(
                            f"stale base {expected_base}: HEAD is {current}; "
                            f"generation {snap.generation} not published"
                        )
                    os.replace(tmp, self._gen_path(snap.generation))
                    self._maybe_crash("after-gen-rename")
                    if self._level_at_least("L2"):
                        _fsync_dir(self.dir)
                    self._maybe_crash("after-gen-dirsync")
                    head_tmp = os.path.join(self.dir, ".HEAD.tmp")
                    with open(head_tmp, "w") as fh:
                        fh.write(str(snap.generation))
                        fh.flush()
                        self._maybe_crash("after-head-tmp-write")
                        if self._level_at_least("L1"):
                            _fsync_file(fh)
                        self._maybe_crash("after-head-tmp-fsync")
                    os.replace(head_tmp, self._head_path())
                    self._maybe_crash("after-head-rename")
                    if self._level_at_least("L2"):
                        _fsync_dir(self.dir)
                    self._maybe_crash("after-final-dirsync")
                    if self._level_at_least("L3"):
                        self._verify_head(snap.generation, snap.index_hash)
                finally:
                    _unlock_file(lock)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        return snap.generation

    def _verify_head(self, generation: int, index_hash: str) -> None:
        """L3 post-commit verification: re-read HEAD and its generation
        from disk and hash-validate before acknowledgement."""
        if self.head() != generation:
            raise StoreError(
                f"post-commit verification: HEAD is not {generation}"
            )
        snap, _, _ = self.load(generation)
        if snap.index_hash != index_hash:
            raise StoreError(
                f"post-commit verification: generation {generation} hash mismatch"
            )

    # -- recovery / check -------------------------------------------------

    def check(self) -> dict:
        """Inspect store health without mutating anything.

        Returns a report `{"head": ..., "generations": [...],
        "reclaimed": [...], "issues": [...], "ok": bool}`. Any
        present issue means the store must not be trusted blindly; see
        RFC 0008 for the recovery contract. `reclaimed` lists
        generations deleted by `reclaim` (recorded in `.reclaimed`);
        they are expected-absent, not gaps. Issue codes:

        - ``HEAD_MISSING`` — no HEAD file (empty or wiped store).
        - ``HEAD_MALFORMED`` — HEAD is not a generation number.
        - ``HEAD_DANGLING`` — HEAD points at a missing generation file.
        - ``GEN_UNREADABLE`` — generation file is truncated/malformed.
        - ``GEN_HASH_MISMATCH`` — generation fails hash validation.
        - ``GEN_NUMBER_MISMATCH`` — embedded generation != filename.
        - ``GEN_GAP`` — a generation in 0..HEAD has no valid file
          and was not deleted by `reclaim` (a manually deleted file
          still reports here — fail closed).
        - ``NEWER_THAN_HEAD`` — a valid generation newer than HEAD
          exists (crash between the generation rename and the HEAD
          rename); HEAD content is still complete, but the newer
          generation is unreachable until republished.
        - ``ORPHAN_TMP`` — leftover staging file (safe to remove).
          Generation staging (`.gen-*.tmp`), HEAD staging
          (`.HEAD.tmp`), compaction-manifest staging
          (`.compact.tmp`), and reclamation-journal staging
          (`.reclaimed.tmp`) all report here.
        - ``MANIFEST_CORRUPT`` — `.compact.json` present but
          unreadable or failing schema check (metrics state only;
          never auto-deleted, left for the operator).
        """
        issues: list = []
        try:
            entries = set(os.listdir(self.dir))
        except FileNotFoundError:
            return {
                "head": None,
                "generations": [],
                "reclaimed": [],
                "issues": [
                    {"code": "HEAD_MISSING", "detail": "store directory missing"}
                ],
                "ok": False,
            }
        head: int | None = None
        try:
            head = self.head()
        except StoreError as exc:
            issues.append({"code": "HEAD_MALFORMED", "detail": str(exc)})
        if head is None and not any(i["code"] == "HEAD_MALFORMED" for i in issues):
            if "HEAD" not in entries:
                issues.append(
                    {"code": "HEAD_MISSING", "detail": "no HEAD file in store"}
                )
        gens: dict[int, str] = {}
        for name in sorted(entries):
            m = _GEN_RE.match(name)
            if m:
                gens[int(m.group(1))] = name
        valid: set[int] = set()
        accounted: set[int] = set()
        for gen in sorted(gens):
            try:
                with open(os.path.join(self.dir, gens[gen])) as fh:
                    data = json.load(fh)
            except ValueError as exc:
                issues.append(
                    {
                        "code": "GEN_UNREADABLE",
                        "detail": f"generation {gen}: truncated or malformed ({exc})",
                    }
                )
                accounted.add(gen)
                continue
            except OSError as exc:
                issues.append(
                    {
                        "code": "GEN_UNREADABLE",
                        "detail": f"generation {gen}: unreadable ({exc})",
                    }
                )
                accounted.add(gen)
                continue
            try:
                self._validate_stored(gen, data)
            except StoreError as exc:
                msg = str(exc)
                if "hash mismatch" in msg:
                    issues.append({"code": "GEN_HASH_MISMATCH", "detail": msg})
                elif "payload carries generation" in msg:
                    issues.append({"code": "GEN_NUMBER_MISMATCH", "detail": msg})
                else:
                    issues.append({"code": "GEN_UNREADABLE", "detail": msg})
                accounted.add(gen)
                continue
            valid.add(gen)
        if head is not None:
            if head not in valid:
                if head in gens:
                    pass  # already reported as unreadable/mismatch above.
                else:
                    issues.append(
                        {
                            "code": "HEAD_DANGLING",
                            "detail": f"HEAD={head} has no generation file",
                        }
                    )
            reclaimed = self._read_reclaimed()
            for n in range(head + 1):
                if n in reclaimed:
                    continue  # deleted by `reclaim`, not lost: not a gap.
                if n not in valid and n not in accounted:
                    issues.append(
                        {
                            "code": "GEN_GAP",
                            "detail": f"generation {n} missing below HEAD={head}",
                        }
                    )
            for gen in sorted(valid):
                if gen > head:
                    issues.append(
                        {
                            "code": "NEWER_THAN_HEAD",
                            "detail": f"generation {gen} valid but HEAD={head}",
                        }
                    )
        for name in sorted(entries):
            if _is_orphan_tmp(name):
                issues.append(
                    {"code": "ORPHAN_TMP", "detail": f"leftover staging file {name}"}
                )
        manifest_issue = self._check_manifest(entries)
        if manifest_issue is not None:
            issues.append(manifest_issue)
        return {
            "head": head,
            "generations": sorted(valid),
            "reclaimed": sorted(self._read_reclaimed()) if head is not None else [],
            "issues": issues,
            "ok": not issues,
        }

    def _check_manifest(self, entries) -> dict | None:
        """Validate `.compact.json` when present (metrics state, not truth).

        A corrupt manifest never affects reads or reclamation, but
        unchecked state is how rot hides: report it loudly and leave
        the file for the operator (`repair` never deletes state).
        """
        if _COMPACT_MANIFEST not in entries:
            return None
        try:
            with open(os.path.join(self.dir, _COMPACT_MANIFEST)) as fh:
                data = json.load(fh)
        except (ValueError, OSError) as exc:
            return {
                "code": "MANIFEST_CORRUPT",
                "detail": f"{_COMPACT_MANIFEST} unreadable ({exc})",
            }
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("schedule"), str)
            or not isinstance(data.get("removed_generations"), list)
        ):
            return {
                "code": "MANIFEST_CORRUPT",
                "detail": f"{_COMPACT_MANIFEST} fails schema check",
            }
        return None

    def repair(self) -> list:
        """Remove leftover staging files (see `_is_orphan_tmp`).

        This is the only automatic repair: staging files are never read
        by any path, so deleting them is always safe. HEAD problems,
        corrupt generations, gaps, and a corrupt manifest are NEVER
        guessed at — they stay reported by `check()` for an operator
        to resolve. Returns the removed filenames.
        """
        removed = []
        try:
            entries = os.listdir(self.dir)
        except FileNotFoundError:
            return removed
        for name in sorted(entries):
            if _is_orphan_tmp(name):
                try:
                    os.unlink(os.path.join(self.dir, name))
                except OSError:
                    continue
                removed.append(name)
        return removed

    # -- reclamation (RFC 0009) -------------------------------------------

    def reclaim(self, keep_recent: int = 0, pid_alive=None) -> dict:
        """Delete reclaimable generations; returns a JSON-able report.

        Runs under the exclusive `.HEAD.lock` (serializing with
        concurrent publishers and reclaimers) and deletes ONLY
        generations that are all of: present on disk, strictly below
        HEAD, and protected by no live pin. Stale (dead-process)
        pins are reaped first so a crashed reader cannot block
        reclamation forever. Never deletes HEAD itself, never
        deletes a newer-than-HEAD crash artifact (`NEWER_THAN_HEAD`
        stays for an operator), and never touches staging files
        (see `repair`) or pins.

        `keep_recent=K` additionally retains the K newest otherwise
        reclaimable generations (a bounded tail cache for readers
        opening recent history by explicit generation).
        """
        if (
            isinstance(keep_recent, bool)
            or not isinstance(keep_recent, int)
            or keep_recent < 0
        ):
            raise StoreError(f"invalid keep_recent {keep_recent!r}: expected int >= 0")
        lock_path = os.path.join(self.dir, ".HEAD.lock")
        os.makedirs(self.dir, exist_ok=True)
        with open(lock_path, "w") as lock:
            _lock_file(lock)
            try:
                head = self.head()
                if head is None:
                    return {
                        "head": None,
                        "removed": [],
                        "reaped_stale": [],
                        "pinned": [],
                    }
                reaped = self.reap_stale_pins(pid_alive)
                live = self.pinned_generations(pid_alive)
                try:
                    entries = os.listdir(self.dir)
                except OSError as exc:
                    raise StoreError(f"store unreadable: {exc}") from exc
                on_disk = set()
                for name in entries:
                    m = _GEN_RE.match(name)
                    if m:
                        on_disk.add(int(m.group(1)))
                below = sorted(g for g in on_disk if g < head)
                victims = [g for g in below if g not in live]
                if keep_recent:
                    victims = victims[: max(0, len(victims) - keep_recent)]
                removed = []
                for gen in victims:
                    # Record-first (RFC 0010): the `.reclaimed` entry
                    # lands before the unlink, so a crash in between
                    # leaves a recorded-but-present generation — which
                    # `check()` exempts and a resumed run re-victims —
                    # never an unrecorded absence (`GEN_GAP`).
                    self._write_reclaimed(self._read_reclaimed() | {gen})
                    try:
                        os.unlink(self._gen_path(gen))
                    except FileNotFoundError:
                        continue
                    except OSError as exc:
                        raise StoreError(
                            f"generation {gen}: cannot reclaim ({exc})"
                        ) from exc
                    self._dirsync_if("L2")
                    removed.append(gen)
                return {
                    "head": head,
                    "removed": sorted(removed),
                    "reaped_stale": sorted(reaped),
                    "pinned": sorted(live),
                }
            finally:
                _unlock_file(lock)

    # -- deterministic compaction (RFC 0010) ------------------------------

    @staticmethod
    def _parse_compact_schedule(schedule: str):
        """Split a schedule string into `(kind, n)`; reject junk loudly."""
        valid = ", ".join(COMPACT_SCHEDULES)
        if not isinstance(schedule, str):
            raise StoreError(
                f"invalid compaction schedule {schedule!r}: expected one of {valid}"
            )
        if schedule == "prune":
            return ("prune", 0)
        m = re.fullmatch(r"keep-recent:(\d+)", schedule)
        if m:
            return ("keep-recent", int(m.group(1)))
        m = re.fullmatch(r"checkpoint:(\d+)", schedule)
        if m:
            n = int(m.group(1))
            if n >= 1:
                return ("checkpoint", n)
        raise StoreError(
            f"invalid compaction schedule {schedule!r}: expected one of {valid}"
        )

    def compact(
        self, schedule: str = "prune", dry_run: bool = False, pid_alive=None
    ) -> dict:
        """Compact the store in place; returns a JSON-able report+metrics.

        There is no second database and no copy phase: victims are
        unlinked in the live directory under the exclusive
        `.HEAD.lock` (serializing with publishers, reclaimers, and
        fellow compactors). The schedule (see `COMPACT_SCHEDULES`)
        selects below-HEAD generations; from that set only
        generations covered by no live pin are victims. Stale
        (dead-process) pins are reaped first. Never deletes HEAD
        itself, a newer-than-HEAD crash artifact, a pinned
        generation, a live writer's staging, or any pin. Also
        removes orphan staging tmps (`.HEAD.tmp`, dead-owner
        `.gen-*.tmp`, `.compact.tmp`) and supersedes the previous
        `.compact.json` manifest with the new one.

        `dry_run=True` computes the exact victim set and projected
        metrics without deleting, reaping, or writing anything.

        Report keys: `head`, `schedule`, `dry_run`,
        `removed_generations`, `removed_tmps`, `reaped_stale`,
        `would_reap_stale` (dry runs only: stale pins a real run
        would reap — victim sets already agree because dead pins
        never protect),
        `pinned`, `skipped_live_tmps` (live writers' staging, kept),
        `skipped_unknown_tmps` (unattributable staging, kept for
        `repair`), `retained` (below-HEAD survivors),
        `newer_than_head`, `generations_before/after` (on-disk
        generation counts), `bytes_before/after` (generation-file
        bytes), `tmp_bytes_reclaimed`, `reclaimed_bytes`
        (`bytes_before - bytes_after`), `amplification_before/after`
        (generation bytes per HEAD byte; None when HEAD has no
        file), `duration_s`, `manifest` (`.compact.json`, or None
        for dry runs and empty stores).
        """
        kind, n = self._parse_compact_schedule(schedule)
        if not isinstance(dry_run, bool):
            raise StoreError(f"invalid dry_run {dry_run!r}: expected bool")
        lock_path = os.path.join(self.dir, ".HEAD.lock")
        os.makedirs(self.dir, exist_ok=True)
        started = time.monotonic()
        with open(lock_path, "w") as lock:
            _lock_file(lock)
            try:
                return self._compact_locked(
                    kind, n, schedule, dry_run, started, pid_alive
                )
            finally:
                _unlock_file(lock)

    def _compact_locked(
        self,
        kind: str,
        n: int,
        schedule: str,
        dry_run: bool,
        started: float,
        pid_alive=None,
    ) -> dict:
        head = self.head()
        if head is None:
            return {
                "head": None,
                "schedule": schedule,
                "dry_run": dry_run,
                "removed_generations": [],
                "removed_tmps": [],
                "reaped_stale": [],
                "pinned": [],
                "skipped_live_tmps": [],
                "skipped_unknown_tmps": [],
                "retained": [],
                "newer_than_head": [],
                "generations_before": 0,
                "generations_after": 0,
                "bytes_before": 0,
                "bytes_after": 0,
                "tmp_bytes_reclaimed": 0,
                "reclaimed_bytes": 0,
                "amplification_before": None,
                "amplification_after": None,
                "duration_s": time.monotonic() - started,
                "manifest": None,
            }
        try:
            pin_entries = os.listdir(self._pins_dir())
        except OSError:
            pin_entries = []
        would_reap = self._stale_pin_names(pin_entries, pid_alive)
        reaped = [] if dry_run else self.reap_stale_pins(pid_alive)
        live = self.pinned_generations(pid_alive)
        try:
            entries = os.listdir(self.dir)
        except OSError as exc:
            raise StoreError(f"store unreadable: {exc}") from exc
        sizes: dict[int, int] = {}
        for name in entries:
            m = _GEN_RE.match(name)
            if m:
                try:
                    sizes[int(m.group(1))] = os.path.getsize(
                        os.path.join(self.dir, name)
                    )
                except OSError as exc:
                    raise StoreError(
                        f"generation {m.group(1)}: cannot stat ({exc})"
                    ) from exc
        below = sorted(g for g in sizes if g < head)
        newer = sorted(g for g in sizes if g > head)
        victims = [g for g in below if g not in live]
        if kind == "keep-recent":
            victims = victims[: max(0, len(victims) - n)]
        elif kind == "checkpoint":
            victims = [g for g in victims if g % n != 0]
        # Staging triage. `.HEAD.tmp` under this lock is orphan by
        # construction (HEAD staging happens inside the same lock, so
        # no live writer can own it). `.gen-*.tmp` carries its
        # writer's pid: dead owner means a crashed writer's orphan
        # (remove); a live owner means in-flight staging (skip and
        # report — deleting it would break that writer's rename).
        # Unattributable names are left for the operator `repair`.
        orphan_tmps: list[str] = []
        skipped_live: list[str] = []
        skipped_unknown: list[str] = []
        if _COMPACT_TMP in entries:
            orphan_tmps.append(_COMPACT_TMP)
        if ".HEAD.tmp" in entries:
            orphan_tmps.append(".HEAD.tmp")
        for name in sorted(entries):
            if not (name.startswith(".gen-") and name.endswith(".tmp")):
                continue
            m = _TMP_RE.match(name)
            if not m:
                skipped_unknown.append(name)
                continue
            if _pid_alive(int(m.group(2))):
                skipped_live.append(name)
            else:
                orphan_tmps.append(name)
        victim_sizes = {g: sizes[g] for g in victims}
        tmp_sizes: dict[str, int] = {}
        for name in orphan_tmps:
            try:
                tmp_sizes[name] = os.path.getsize(os.path.join(self.dir, name))
            except OSError:
                tmp_sizes[name] = 0
        bytes_before = sum(sizes.values())
        head_size = sizes.get(head)
        amplification_before = (
            bytes_before / head_size if head_size else None
        )
        if dry_run:
            projected = bytes_before - sum(victim_sizes.values())
            amplification_after = (
                projected / head_size if head_size else None
            )
            return {
                "head": head,
                "schedule": schedule,
                "dry_run": True,
                "removed_generations": sorted(victims),
                "removed_tmps": sorted(orphan_tmps),
                "reaped_stale": [],
                "would_reap_stale": sorted(would_reap),
                "pinned": sorted(live),
                "skipped_live_tmps": sorted(skipped_live),
                "skipped_unknown_tmps": sorted(skipped_unknown),
                "retained": sorted(set(below) - set(victims)),
                "newer_than_head": newer,
                "generations_before": len(sizes),
                "generations_after": len(sizes) - len(victims),
                "bytes_before": bytes_before,
                "bytes_after": projected,
                "tmp_bytes_reclaimed": sum(tmp_sizes.values()),
                "reclaimed_bytes": bytes_before - projected,
                "amplification_before": amplification_before,
                "amplification_after": amplification_after,
                "duration_s": time.monotonic() - started,
                "manifest": None,
            }
        removed = []
        for gen in victims:
            # Record-first: see `reclaim`. An interrupted run leaves
            # only recorded-absent or recorded-present files behind,
            # so resume converges and `check()` stays quiet.
            self._write_reclaimed(self._read_reclaimed() | {gen})
            try:
                os.unlink(self._gen_path(gen))
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise StoreError(
                    f"generation {gen}: cannot compact ({exc})"
                ) from exc
            else:
                removed.append(gen)
            self._dirsync_if("L2")
            self._maybe_crash_compact("after-compact-delete")
        removed_tmps = []
        for name in orphan_tmps:
            try:
                os.unlink(os.path.join(self.dir, name))
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise StoreError(f"staging {name}: cannot compact ({exc})") from exc
            removed_tmps.append(name)
        if removed_tmps:
            self._dirsync_if("L2")
        # Ground-truth post-state (no concurrent writer can exist
        # under the lock; re-sum rather than subtract).
        post_entries = os.listdir(self.dir)
        post_sizes: dict[int, int] = {}
        for name in post_entries:
            m = _GEN_RE.match(name)
            if m:
                post_sizes[int(m.group(1))] = os.path.getsize(
                    os.path.join(self.dir, name)
                )
        bytes_after = sum(post_sizes.values())
        amplification_after = (
            bytes_after / head_size if head_size else None
        )
        report = {
            "head": head,
            "schedule": schedule,
            "dry_run": False,
            "removed_generations": sorted(removed),
            "removed_tmps": sorted(removed_tmps),
            "reaped_stale": sorted(reaped),
            "pinned": sorted(live),
            "skipped_live_tmps": sorted(skipped_live),
            "skipped_unknown_tmps": sorted(skipped_unknown),
            "retained": sorted(g for g in below if g in post_sizes),
            "newer_than_head": sorted(g for g in post_sizes if g > head),
            "generations_before": len(sizes),
            "generations_after": len(post_sizes),
            "bytes_before": bytes_before,
            "bytes_after": bytes_after,
            "tmp_bytes_reclaimed": sum(tmp_sizes[name] for name in removed_tmps),
            "reclaimed_bytes": bytes_before - bytes_after,
            "amplification_before": amplification_before,
            "amplification_after": amplification_after,
            "duration_s": time.monotonic() - started,
            "manifest": _COMPACT_MANIFEST,
        }
        self._maybe_crash_compact("before-compact-manifest")
        manifest_record = dict(report)
        manifest_tmp = os.path.join(self.dir, _COMPACT_TMP)
        with open(manifest_tmp, "w") as fh:
            json.dump(manifest_record, fh, indent=1, sort_keys=True)
            fh.write("\n")
            fh.flush()
            if self._level_at_least("L1"):
                os.fsync(fh.fileno())
        self._maybe_crash_compact("after-compact-manifest-write")
        os.replace(manifest_tmp, os.path.join(self.dir, _COMPACT_MANIFEST))
        self._dirsync_if("L2")
        return report
