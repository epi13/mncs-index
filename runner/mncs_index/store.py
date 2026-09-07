"""Snapshot store: validated, atomic, generational publication.

Writers build a candidate privately; `publish` re-derives the canonical
bytes from the stored JSON form and compares hashes before swapping HEAD.
A failed or cancelled build never reaches `publish`, so readers only ever
see the previous complete snapshot or the new complete snapshot.

Publication is compare-and-swap: every writer declares the HEAD
generation its candidate was built on (`expected_base`), and the
read-check-swap of HEAD runs under an exclusive `flock` on
`.HEAD.lock`, so overlapping writers — threads or separate processes —
are serialized. A writer whose base no longer matches HEAD loses with
`StaleGenerationError` and a stale generation can never become HEAD.
Candidate generation numbers are publication metadata and must advance
by exactly one (`expected_base + 1`, or `0` from an empty store).
"""

from __future__ import annotations

import fcntl
import json
import os
import threading

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


class Store:
    def __init__(self, directory: str):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)

    def _gen_path(self, generation: int) -> str:
        return os.path.join(self.dir, f"gen-{generation:06d}.json")

    def _head_path(self) -> str:
        return os.path.join(self.dir, "HEAD")

    def head(self) -> int | None:
        try:
            with open(self._head_path()) as fh:
                return int(fh.read().strip())
        except (OSError, ValueError):
            return None

    def load(self, generation: int):
        with open(self._gen_path(generation)) as fh:
            data = json.load(fh)
        snap = snapshot_from_json(data)
        # Round-trip validation: the stored form must reproduce canonical bytes.
        if snap.has_rich:
            expect = canonical_bytes_v2(snap.sorted_all_records(), snap.snapshot_id)
        else:
            expect = canonical_bytes(snap.snapshot_id, snap.sorted_records())
        if index_hash_of(expect) != snap.index_hash:
            raise StoreError(f"generation {generation}: stored hash mismatch")
        established = data.get("established", {})
        crc = {d["path"]: d.get("crc", 0) for d in data.get("docs", [])}
        return snap, established, crc

    def load_head(self):
        gen = self.head()
        if gen is None:
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
        try:
            lock_path = os.path.join(self.dir, ".HEAD.lock")
            with open(lock_path, "w") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                try:
                    # Re-read HEAD under the lock: the base check, the
                    # generation-file materialization, and the HEAD swap
                    # are one critical section, so two overlapping writers
                    # cannot both observe the same base and both swap.
                    current = self.head()
                    if current != expected_base:
                        raise StaleGenerationError(
                            f"stale base {expected_base}: HEAD is {current}; "
                            f"generation {snap.generation} not published"
                        )
                    os.replace(tmp, self._gen_path(snap.generation))
                    head_tmp = os.path.join(self.dir, ".HEAD.tmp")
                    with open(head_tmp, "w") as fh:
                        fh.write(str(snap.generation))
                    os.replace(head_tmp, self._head_path())
                finally:
                    fcntl.flock(lock, fcntl.LOCK_UN)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        return snap.generation
