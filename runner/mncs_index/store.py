"""Snapshot store: validated, atomic, generational publication.

Writers build a candidate privately; `publish` re-derives the canonical
bytes from the stored JSON form and compares hashes before swapping HEAD.
A failed or cancelled build never reaches `publish`, so readers only ever
see the previous complete snapshot or the new complete snapshot.
"""

from __future__ import annotations

import json
import os

from .model import (
    canonical_bytes,
    index_hash_of,
    snapshot_from_json,
    snapshot_to_json,
)


class StoreError(Exception):
    pass


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

    def publish(self, snap, canonical: bytes, established: dict, crc: dict) -> int:
        """Validate the candidate and atomically make it HEAD.

        `crc` carries non-canonical content hints for the incremental fast
        path; they are stored alongside records but never hashed into
        canonical meaning.
        """
        expect = canonical_bytes(snap.snapshot_id, snap.sorted_records())
        if expect != canonical:
            raise StoreError("candidate canonical bytes do not match records")
        if index_hash_of(canonical) != snap.index_hash:
            raise StoreError("candidate index hash does not match bytes")
        payload = snapshot_to_json(snap)
        payload["established"] = {k: int(v) for k, v in established.items()}
        for doc in payload["docs"]:
            doc["crc"] = int(crc.get(doc["path"], 0))
        tmp = os.path.join(self.dir, f".gen-{snap.generation:06d}.tmp")
        with open(tmp, "w") as fh:
            json.dump(payload, fh, indent=1, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, self._gen_path(snap.generation))
        head_tmp = os.path.join(self.dir, ".HEAD.tmp")
        with open(head_tmp, "w") as fh:
            fh.write(str(snap.generation))
        os.replace(head_tmp, self._head_path())
        return snap.generation
