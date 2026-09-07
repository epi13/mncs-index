"""Canonical index model: records, serialization, hashes.

The canonical byte layout is the hashed artifact (RFC 0003). Field order,
record order, and digest presentation are fixed here; the ordering
*rule* is specified by `src/order.mncs` (`record_less`, `compare_window`)
and differentially tested in `tests/test_differential.py`. The host sort
below implements that rule at corpus scale (pressure/PRESS-005).

Two digests identify a snapshot:
- `index_hash`: SHA-256 over the canonical bytes (host-side; the language
  has no crypto digest — pressure/PRESS-006).
- `mncs_fingerprint`: the MNCS Merkle digest of the canonical bytes
  (parallel `fold_window` leaves + canonical `tree_combine` reduction —
  the same construction as per-file content digests), so the canonical
  digest is also an MNCS-computed value. Both must agree across worker
  counts/schedules.
"""

from __future__ import annotations

import hashlib
import zlib
from dataclasses import dataclass, field

CANONICAL_VERSION = "canonical-v1"
SNAPSHOT_FORMAT = "mncs-index/snapshot-v1"
TERM_KIND_RANK = 100


def hex16(v: int) -> str:
    return f"{v & ((1 << 64) - 1):016x}"


def unhex16(s: str) -> int:
    return int(s, 16)


@dataclass(frozen=True)
class DocRecord:
    kind: int
    path: str
    digest: int
    size: int
    words: int
    lines: int

    def sort_key(self) -> tuple:
        return (self.kind, self.path.encode("utf-8"), 0)

    def canon_line(self) -> str:
        return (
            f"doc {self.kind} {self.path} {hex16(self.digest)} "
            f"{self.size} {self.words} {self.lines} 0"
        )


@dataclass(frozen=True)
class TermRecord:
    path: str
    token: str
    tid: int
    seq: int

    def sort_key(self) -> tuple:
        return (TERM_KIND_RANK, self.path.encode("utf-8"), self.seq)

    def canon_line(self) -> str:
        return f"term {self.path} {self.token} {hex16(self.tid)} {self.seq}"


@dataclass
class Snapshot:
    snapshot_id: str
    generation: int
    docs: list = field(default_factory=list)
    terms: list = field(default_factory=list)
    index_hash: str = ""
    mncs_fingerprint: str = ""

    def sorted_records(self):
        recs = [("doc", d) for d in self.docs] + [("term", t) for t in self.terms]
        recs.sort(key=lambda item: item[1].sort_key())
        return recs


def check_path_safe(path: str) -> str:
    if "\n" in path or "\r" in path or "\t" in path:
        raise ValueError(
            f"path contains control characters and cannot be canonicalized: {path!r}"
        )
    path.encode("utf-8")
    return path


def canonical_bytes(snapshot_id: str, records) -> bytes:
    """Serialize already-sorted records to the canonical byte stream.

    Only content participates: format marker, discovery snapshot id, and
    records. The store generation is publication metadata (which publish
    event made this visible), not canonical meaning — the same logical
    corpus must hash identically whether it was built fresh at generation
    0 or reached incrementally at generation 9.
    """
    lines = [
        "mncs-index " + CANONICAL_VERSION,
        "snapshot " + snapshot_id,
    ]
    for _kind, rec in records:
        lines.append(rec.canon_line())
    lines.append("end")
    return ("\n".join(lines) + "\n").encode("utf-8")


def index_hash_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def mncs_fingerprint_of(kernels, data: bytes, workers: int = 8) -> int:
    """MNCS Merkle digest of the canonical bytes: parallel `fold_window`
    leaves plus canonical `tree_combine` reduction — the same construction
    as per-file content digests, so content and canonical identity share
    one scheme. Deterministic: pairing is fixed, results placed by index.
    """
    return kernels.tree_digest_windows(data, workers)


def discovery_id(entries: list[tuple[str, int, int]]) -> str:
    """Logical corpus identity from sorted (path, size, crc32-hint) lines.

    crc32 is a *hint* for incremental reuse (RFC 0005: events are hints,
    not truth). Canonical content identity always comes from MNCS digests.
    """
    h = hashlib.sha256()
    for path, size, crc in sorted(entries):
        h.update(f"{path}\n{size}\n{crc:08x}\n".encode())
    return h.hexdigest()


def snapshot_to_json(snap: Snapshot) -> dict:
    return {
        "format": SNAPSHOT_FORMAT,
        "snapshot_id": snap.snapshot_id,
        "generation": snap.generation,
        "index_hash": snap.index_hash,
        "mncs_fingerprint": snap.mncs_fingerprint,
        "docs": [
            {
                "kind": d.kind,
                "path": d.path,
                "digest": hex16(d.digest),
                "size": d.size,
                "words": d.words,
                "lines": d.lines,
            }
            for d in snap.docs
        ],
        "terms": [
            {"path": t.path, "token": t.token, "tid": hex16(t.tid), "seq": t.seq}
            for t in snap.terms
        ],
    }


def snapshot_from_json(data: dict) -> Snapshot:
    if data.get("format") != SNAPSHOT_FORMAT:
        raise ValueError(f"unsupported snapshot format: {data.get('format')}")
    return Snapshot(
        snapshot_id=data["snapshot_id"],
        generation=int(data["generation"]),
        docs=[
            DocRecord(
                kind=int(d["kind"]),
                path=d["path"],
                digest=unhex16(d["digest"]),
                size=int(d["size"]),
                words=int(d["words"]),
                lines=int(d["lines"]),
            )
            for d in data.get("docs", [])
        ],
        terms=[
            TermRecord(
                path=t["path"],
                token=t["token"],
                tid=unhex16(t["tid"]),
                seq=int(t["seq"]),
            )
            for t in data.get("terms", [])
        ],
        index_hash=data.get("index_hash", ""),
        mncs_fingerprint=data.get("mncs_fingerprint", ""),
    )


def crc32_of(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF
