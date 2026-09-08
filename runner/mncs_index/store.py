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

- ``L0`` — no `fsync` at all. Atomic renames still make a *process*
  crash safe (old-complete or new-complete), but an OS crash or power
  loss may lose or roll back the commit.
- ``L1`` — file `fsync` on the generation file and the HEAD file
  before their renames. A process crash is safe; an OS crash may lose
  a rename (directory entry) even though file contents are durable.
- ``L2`` (default) — ``L1`` plus a directory `fsync` after each
  rename. Once `publish` returns, the commit survives an OS crash or
  power loss on a correctly behaving POSIX filesystem.
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
"""

from __future__ import annotations

import json
import os
import re
import threading

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


class StaleGenerationError(StoreError):
    """HEAD moved under a writer: `expected_base` no longer matches.

    The candidate was built on a stale generation and must be rebuilt
    (or dropped); it was not published and HEAD is untouched.
    """


DURABILITY_LEVELS = ("L0", "L1", "L2", "L3")

DURABILITY_DEFAULT = "L2"

DURABILITY_GUARANTEES = {
    "L0": "atomic renames only: safe across process crash, may lose "
    "or roll back the commit across OS crash / power loss.",
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
        unknown = set(crash_at) - set(CRASH_POINTS) - CRASH_NONE
        if unknown:
            raise StoreError(
                f"unknown crash point(s) {sorted(unknown)}: expected subset of "
                f"{', '.join(CRASH_POINTS)}"
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

    def _gen_path(self, generation: int) -> str:
        return os.path.join(self.dir, f"gen-{generation:06d}.json")

    def _head_path(self) -> str:
        return os.path.join(self.dir, "HEAD")

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
            raise StoreError(f"generation {generation}: file missing") from exc
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
        "issues": [{"code": ..., "detail": ...}], "ok": bool}`. Any
        present issue means the store must not be trusted blindly; see
        RFC 0008 for the recovery contract. Issue codes:

        - ``HEAD_MISSING`` — no HEAD file (empty or wiped store).
        - ``HEAD_MALFORMED`` — HEAD is not a generation number.
        - ``HEAD_DANGLING`` — HEAD points at a missing generation file.
        - ``GEN_UNREADABLE`` — generation file is truncated/malformed.
        - ``GEN_HASH_MISMATCH`` — generation fails hash validation.
        - ``GEN_NUMBER_MISMATCH`` — embedded generation != filename.
        - ``GEN_GAP`` — a generation in 0..HEAD has no valid file.
        - ``NEWER_THAN_HEAD`` — a valid generation newer than HEAD
          exists (crash between the generation rename and the HEAD
          rename); HEAD content is still complete, but the newer
          generation is unreachable until republished.
        - ``ORPHAN_TMP`` — leftover staging file (safe to remove).
        """
        issues: list = []
        try:
            entries = set(os.listdir(self.dir))
        except FileNotFoundError:
            return {
                "head": None,
                "generations": [],
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
            for n in range(head + 1):
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
            if name == ".HEAD.tmp" or (
                name.startswith(".gen-") and name.endswith(".tmp")
            ):
                issues.append(
                    {"code": "ORPHAN_TMP", "detail": f"leftover staging file {name}"}
                )
        return {
            "head": head,
            "generations": sorted(valid),
            "issues": issues,
            "ok": not issues,
        }

    def repair(self) -> list:
        """Remove leftover staging files (`.HEAD.tmp`, `.gen-*.tmp`).

        This is the only automatic repair: staging files are never read
        by any path, so deleting them is always safe. HEAD problems,
        corrupt generations, and gaps are NEVER guessed at — they stay
        reported by `check()` for an operator to resolve. Returns the
        removed filenames.
        """
        removed = []
        try:
            entries = os.listdir(self.dir)
        except FileNotFoundError:
            return removed
        for name in sorted(entries):
            if name == ".HEAD.tmp" or (
                name.startswith(".gen-") and name.endswith(".tmp")
            ):
                try:
                    os.unlink(os.path.join(self.dir, name))
                except OSError:
                    continue
                removed.append(name)
        return removed
