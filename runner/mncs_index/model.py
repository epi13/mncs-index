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

# canonical-v2 is a strict, additive superset of canonical-v1: the v1
# doc/term section serializes byte-identically inside v2 bytes, and the
# v1 marker/reader path below is untouched. New families append new kind
# ranks only (cf. kind-rank stability in src/README.md).
CANONICAL_VERSION_V2 = "canonical-v2"
SNAPSHOT_FORMAT_V2 = "mncs-index/snapshot-v2"
SYM_KIND_RANK = 101
HEADING_KIND_RANK = 102
REL_KIND_RANK = 103
PRESS_KIND_RANK = 104

SYM_KINDS = ("module", "fn", "record", "use")
REL_KINDS = ("defines", "references", "depends-on", "rfc-ref")


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


@dataclass(frozen=True)
class SymRecord:
    """One MNCS source declaration (canonical-v2, kind 101).

    Identity is (path, sym, name); `seq` is the canonical position after
    the global (path, line) sort. `ndigest` is the MNCS fold of the name
    bytes, so symbol identity is content-addressed, not host-hashed.
    """

    path: str
    sym: str
    name: str
    ndigest: int
    seq: int = 0

    def __post_init__(self):
        if self.sym not in SYM_KINDS:
            raise ValueError(f"unknown sym kind: {self.sym!r}")

    def sort_key(self) -> tuple:
        return (SYM_KIND_RANK, self.path.encode("utf-8"), self.seq)

    def canon_line(self) -> str:
        return (
            f"sym {self.path} {self.sym} {self.name} {hex16(self.ndigest)} "
            f"{self.seq}"
        )


@dataclass(frozen=True)
class HeadingRecord:
    """One markdown/RFC section heading (canonical-v2, kind 102).

    Identity is positional (path, seq): two identical titles at different
    lines are distinct facts. `tdigest` is the MNCS fold of the title
    bytes. Titles are single lines, so they cannot break the line
    discipline of canonical bytes.
    """

    path: str
    level: int
    seq: int
    tdigest: int
    title: str

    def __post_init__(self):
        if not 1 <= self.level <= 6:
            raise ValueError(f"heading level out of range: {self.level!r}")

    def sort_key(self) -> tuple:
        return (HEADING_KIND_RANK, self.path.encode("utf-8"), self.seq)

    def canon_line(self) -> str:
        return (
            f"heading {self.path} {self.level} {self.seq} "
            f"{hex16(self.tdigest)} {self.title}"
        )


@dataclass(frozen=True)
class RelRecord:
    """One canonical relationship edge (canonical-v2, kind 103).

    `rel` is one of defines / references / depends-on / rfc-ref.
    Identity is the full triple (src, rel, dst): duplicates from repeated
    mentions collapse to one edge deterministically at merge. `seq` is
    the canonical position after the global (rel, src, dst) sort.
    """

    src: str
    rel: str
    dst: str
    seq: int = 0

    def __post_init__(self):
        if self.rel not in REL_KINDS:
            raise ValueError(f"unknown rel kind: {self.rel!r}")

    def sort_key(self) -> tuple:
        return (REL_KIND_RANK, self.src.encode("utf-8"), self.seq)

    def canon_line(self) -> str:
        return f"rel {self.src} {self.rel} {self.dst} {self.seq}"


@dataclass(frozen=True)
class PressRecord:
    """One pressure-registry mention, e.g. PRESS-001 (v2, kind 104).

    Identity is (path, pid): repeated mentions in one file collapse to
    one record. `seq` is the canonical position after the global
    (path, pid) sort.
    """

    path: str
    pid: str
    seq: int = 0

    def sort_key(self) -> tuple:
        return (PRESS_KIND_RANK, self.path.encode("utf-8"), self.seq)

    def canon_line(self) -> str:
        return f"press {self.path} {self.pid} {self.seq}"


@dataclass
class Snapshot:
    snapshot_id: str
    generation: int
    docs: list = field(default_factory=list)
    terms: list = field(default_factory=list)
    index_hash: str = ""
    mncs_fingerprint: str = ""
    # canonical-v2 extension tables. Empty on every v1 snapshot; v1
    # canonical bytes ignore them, so v1 meaning is unaffected.
    syms: list = field(default_factory=list)
    headings: list = field(default_factory=list)
    rels: list = field(default_factory=list)
    press: list = field(default_factory=list)

    def sorted_records(self):
        recs = [("doc", d) for d in self.docs] + [("term", t) for t in self.terms]
        recs.sort(key=lambda item: item[1].sort_key())
        return recs

    @property
    def has_rich(self) -> bool:
        return bool(self.syms or self.headings or self.rels or self.press)

    def sorted_rich_records(self):
        recs = (
            [("sym", r) for r in self.syms]
            + [("heading", r) for r in self.headings]
            + [("rel", r) for r in self.rels]
            + [("press", r) for r in self.press]
        )
        recs.sort(key=lambda item: item[1].sort_key())
        return recs

    def sorted_all_records(self):
        recs = self.sorted_records() + self.sorted_rich_records()
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


def canonical_bytes_v2(all_sorted_records, snapshot_id: str) -> bytes:
    """Serialize the full v2 record set (v1 docs/terms + rich tables).

    The v1 doc/term lines are produced by the same `canon_line` methods
    in the same relative order, so the v1 section is byte-identical to
    `canonical_bytes`; only the format marker differs and rich lines are
    appended after every v1 line (all rich ranks exceed v1 ranks).
    """
    lines = [
        "mncs-index " + CANONICAL_VERSION_V2,
        "snapshot " + snapshot_id,
    ]
    for _kind, rec in all_sorted_records:
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
    if data.get("format") == SNAPSHOT_FORMAT_V2:
        return snapshot_from_json_v2(data)
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


def snapshot_to_json_v2(snap: Snapshot) -> dict:
    """v2 envelope: v1 doc/term keys unchanged, rich tables appended.

    Migration note (RFC 0007): a v1 envelope loads as v2 with empty
    extension tables, and a v2 snapshot with empty tables hashes exactly
    like its v1 projection. Readers must accept both formats.
    """
    payload = snapshot_to_json(snap)
    payload["format"] = SNAPSHOT_FORMAT_V2
    payload["syms"] = [
        {
            "path": r.path,
            "sym": r.sym,
            "name": r.name,
            "ndigest": hex16(r.ndigest),
            "seq": r.seq,
        }
        for r in snap.syms
    ]
    payload["headings"] = [
        {
            "path": r.path,
            "level": r.level,
            "seq": r.seq,
            "tdigest": hex16(r.tdigest),
            "title": r.title,
        }
        for r in snap.headings
    ]
    payload["rels"] = [
        {"src": r.src, "rel": r.rel, "dst": r.dst, "seq": r.seq} for r in snap.rels
    ]
    payload["press"] = [
        {"path": r.path, "pid": r.pid, "seq": r.seq} for r in snap.press
    ]
    return payload


def snapshot_from_json_v2(data: dict) -> Snapshot:
    snap = snapshot_from_json({**data, "format": SNAPSHOT_FORMAT})
    snap.syms = [
        SymRecord(
            path=r["path"],
            sym=r["sym"],
            name=r["name"],
            ndigest=unhex16(r["ndigest"]),
            seq=int(r["seq"]),
        )
        for r in data.get("syms", [])
    ]
    snap.headings = [
        HeadingRecord(
            path=r["path"],
            level=int(r["level"]),
            seq=int(r["seq"]),
            tdigest=unhex16(r["tdigest"]),
            title=r["title"],
        )
        for r in data.get("headings", [])
    ]
    snap.rels = [
        RelRecord(src=r["src"], rel=r["rel"], dst=r["dst"], seq=int(r["seq"]))
        for r in data.get("rels", [])
    ]
    snap.press = [
        PressRecord(path=r["path"], pid=r["pid"], seq=int(r["seq"]))
        for r in data.get("press", [])
    ]
    return snap


def crc32_of(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF
