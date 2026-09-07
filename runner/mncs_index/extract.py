"""Canonical-v2 rich extraction: source/heading/reference/pressure records.

Every semantic verdict — declaration keyword (`classify_decl`), heading
level (`heading_level`), link seam (`contains_link`), PRESS-ID shape
(`is_press_id`), RFC token shape (`classify_rfc_token`), name validity
(`is_symbol_token`), name/title digests (`fold_window` thread) — is an
MNCS kernel verdict from `src/extract.mncs` (plus scan/digest kernels).
The host only splits bytes into lines, trims ASCII space/tab, slices
candidate name/target spans, scans 64 B windows (1 B overlap) for seams
past the first window, pairs adjacent RFC tokens, and sorts/dedups at
corpus scale (pressure/PRESS-005, PRESS-014). Single-hop relationship
lookup is queryable; transitive graph traversal is a documented
non-goal (PRESS-015).

Record identities (RFC 0007): sym = (path, sym, name); heading =
positional (path, seq); rel = (rel, src, dst); press = (path, pid).
Merge dedups by identity and assigns canonical seqs, so concurrent
per-file extraction converges deterministically across worker counts.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from .model import HeadingRecord, PressRecord, RelRecord, SymRecord

KERNEL_WINDOW = 64
MAX_NAME = 64
MAX_LINK_TARGET = 256

DECL_BY_CODE = {1: "module", 2: "fn", 3: "record", 4: "use"}
DECL_KEYWORDS = {1: b"module", 2: b"fn", 3: b"record", 4: b"use"}

_NAME_RE = re.compile(rb"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*")
_SYM_CONT = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.")


def path_extension(path: str) -> bytes:
    """Lowercased extension bytes without dot (mirrors pipeline planning).

    Single source of truth for the planner and the extractor: both map a
    corpus path to kind-rank input identically.
    """
    name = path.rsplit("/", 1)[-1]
    ext = (
        name.rsplit(".", 1)[-1].lower().encode()
        if "." in name and not name.startswith(".")
        else b""
    )
    if len(ext) > 8:
        ext = b""
    return ext


@dataclass
class FileRich:
    """Per-file extraction rows; seqs are assigned at global merge."""

    path: str
    syms: list = field(default_factory=list)  # (lineno, sym, name, ndigest)
    headings: list = field(default_factory=list)  # (lineno, level, title, tdigest)
    rels: list = field(default_factory=list)  # (rel, dst)
    press: list = field(default_factory=list)  # pid


@dataclass
class RichTables:
    syms: list = field(default_factory=list)
    headings: list = field(default_factory=list)
    rels: list = field(default_factory=list)
    press: list = field(default_factory=list)


def kernel_windows(line: bytes) -> list[bytes]:
    """64 B windows with 1 B overlap so a `](` seam straddling a boundary
    is still decided by the kernel on some window (PRESS-014)."""
    if len(line) <= KERNEL_WINDOW:
        return [line]
    return [line[i : i + KERNEL_WINDOW] for i in range(0, len(line), KERNEL_WINDOW - 1)]


def ordered_symbol_tokens(line: bytes, kernels) -> list[bytes]:
    """Symbol-token runs in line order (no dedup: RFC pairing is
    positional). Runs over 32 B are dropped wholly, mirroring the
    pipeline token bound in `Pipeline._split_tokens`."""
    classify = kernels.byte_class
    out: list[bytes] = []
    cur = bytearray()
    overlong = False
    for b in line:
        if classify(b) == 3:
            if overlong:
                continue
            cur.append(b)
            if len(cur) > 32:
                cur = bytearray()
                overlong = True
        else:
            if cur:
                out.append(bytes(cur))
                cur = bytearray()
            overlong = False
    if cur:
        out.append(bytes(cur))
    return out


def take_dotted_name(rest: bytes, kernels) -> bytes | None:
    """Longest dotted symbol name at the start of `rest`, or None.

    Slicing is host plumbing; validity of every segment is the MNCS
    `is_symbol_token` verdict. Overlong (>64 B total, or a symbol run
    continuing past the window) and invalid names are skipped, never
    truncated into a wrong identity (PRESS-014).
    """
    m = _NAME_RE.match(rest)
    if not m:
        return None
    name = m.group(0)
    if len(name) > MAX_NAME:
        return None
    if len(rest) > len(name) and rest[len(name)] in _SYM_CONT:
        return None
    for segment in name.split(b"."):
        if not kernels.is_symbol_token(segment):
            return None
    return name


def extract_link_targets(line: bytes) -> list[str]:
    """Raw `](target)` spans in order (no nesting support, PRESS-014).

    Targets that are empty, overlong, or undecodable are skipped; every
    kept target is single-line by construction.
    """
    out: list[str] = []
    i = 0
    while True:
        seam = line.find(b"](", i)
        if seam < 0:
            return out
        close = line.find(b")", seam + 2)
        if close < 0:
            return out
        target = line[seam + 2 : close]
        i = close + 1
        if not target or len(target) > MAX_LINK_TARGET:
            continue
        if target.startswith(b"<") and target.endswith(b">") and len(target) >= 2:
            target = target[1:-1]
        try:
            text = target.decode("utf-8", "strict")
        except UnicodeDecodeError:
            continue
        if "\n" in text or "\r" in text or "\t" in text:
            continue
        out.append(text)


def _press_ids(line: bytes, kernels) -> list[str]:
    """PRESS-ID mentions in one line; shape decided by the kernel."""
    out: list[str] = []
    i = line.find(b"PRESS-")
    while i >= 0:
        cand = line[i : i + 9]
        if len(cand) == 9 and kernels.is_press_id(cand):
            out.append(cand.decode("ascii"))
        i = line.find(b"PRESS-", i + 1)
    return out


def _rfc_refs(tokens: list[bytes], kernels) -> list[str]:
    """RFC numbers from adjacent keyword+number token pairs in line-token
    order; token shapes are kernel verdicts, pairing is host plumbing."""
    classes = [kernels.classify_rfc_token(t) for t in tokens]
    out: list[str] = []
    for cls_a, cls_b, tok_b in zip(classes, classes[1:], tokens[1:]):
        if cls_a in (1, 3) and cls_b == 2:
            out.append(tok_b.decode("ascii"))
    return out


def extract_file(path: str, kind: int, data: bytes, kernels) -> FileRich:
    """Extract one file's rich rows. Pure function of (path, kind, data):
    identical content always yields identical rows, which is what makes
    incremental reuse by content-identity sound."""
    fr = FileRich(path)
    for lineno, raw in enumerate(data.split(b"\n")):
        line = raw[:-1] if raw.endswith(b"\r") else raw
        if not line:
            continue
        trimmed = line.lstrip(b" \t")
        if not trimmed:
            continue
        prefix = trimmed[:KERNEL_WINDOW]
        if kind == 1:
            code = kernels.classify_decl(prefix)
            if code:
                rest = trimmed[len(DECL_KEYWORDS[code]) :].lstrip(b" \t")
                name = take_dotted_name(rest, kernels)
                if name is None:
                    continue
                sym = DECL_BY_CODE[code]
                ntext = name.decode("ascii")
                ndigest = kernels.fold_bytes(name)
                fr.syms.append((lineno, sym, ntext, ndigest))
                if sym == "use":
                    fr.rels.append(("references", ntext))
                    fr.rels.append(("depends-on", ntext))
                else:
                    fr.rels.append(("defines", ntext))
        elif kind == 2:
            level = kernels.heading_level(prefix)
            if level:
                title_bytes = trimmed[level + 1 :]
                try:
                    title = title_bytes.decode("utf-8", "strict")
                except UnicodeDecodeError:
                    continue
                fr.headings.append(
                    (lineno, level, title, kernels.fold_bytes(title_bytes))
                )
            if any(kernels.contains_link(w) for w in kernel_windows(trimmed)):
                for target in extract_link_targets(trimmed):
                    fr.rels.append(("references", target))
        for pid in _press_ids(line, kernels):
            fr.press.append(pid)
        tokens = ordered_symbol_tokens(line, kernels)
        if tokens:
            for num in _rfc_refs(tokens, kernels):
                fr.rels.append(("rfc-ref", num))
    return fr


def globalize(per_file: dict[str, FileRich]) -> RichTables:
    """Deterministic merge: dedup by record identity, canonical seqs.

    Input order cannot leak: every table is re-sorted from identity keys
    before seq assignment, so thread completion order never escapes.
    """
    sym_rows: dict[tuple, tuple] = {}
    for path in sorted(per_file):
        for lineno, sym, name, ndigest in sorted(per_file[path].syms):
            sym_rows.setdefault((path, sym, name), (lineno, sym, name, ndigest))
    syms = [
        SymRecord(path=p, sym=s, name=n, ndigest=h, seq=i)
        for i, ((p, s, n), (_, _, _, h)) in enumerate(
            sorted(sym_rows.items(), key=lambda kv: (kv[0][0], kv[1][0]))
        )
    ]
    # Above: sort by (path, lineno) via the kept row's lineno.
    # Headings deduplicate on the FULL row and sort on the full tuple:
    # a bare (path, lineno) key would let duplicate pairs leak arrival
    # order through the stable sort and would double-emit identical
    # rows. The output depends only on the row multiset, never on the
    # order rows arrived from workers.
    head_keys: set[tuple] = set()
    for path in sorted(per_file):
        for lineno, level, title, tdigest in per_file[path].headings:
            head_keys.add((path, lineno, level, title, tdigest))
    headings = [
        HeadingRecord(path=p, level=l, seq=i, tdigest=h, title=t)
        for i, (p, _, l, t, h) in enumerate(sorted(head_keys))
    ]
    rel_keys = set()
    for path in sorted(per_file):
        for rel, dst in per_file[path].rels:
            rel_keys.add((path, rel, dst))
    rels = [
        RelRecord(src=p, rel=r, dst=d, seq=i)
        for i, (p, r, d) in enumerate(sorted(rel_keys))
    ]
    press_keys: dict[tuple, int] = {}
    for path in sorted(per_file):
        for pid in per_file[path].press:
            press_keys.setdefault((path, pid), 0)
    press = [
        PressRecord(path=p, pid=i2, seq=i)
        for i, (p, i2) in enumerate(sorted(press_keys))
    ]
    return RichTables(syms=syms, headings=headings, rels=rels, press=press)


def extract_many(
    files: list[tuple[str, int, bytes]], kernels, workers: int = 4
) -> dict[str, FileRich]:
    """Concurrent per-file extraction into path-keyed slots.

    Threads never order or number anything; the caller merges with
    `globalize`, so any worker count converges to identical rows.
    """

    def work(item: tuple[str, int, bytes]) -> tuple[str, FileRich]:
        path, kind, data = item
        return path, extract_file(path, kind, data, kernels)

    per_file: dict[str, FileRich] = {}
    if not files:
        return per_file
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for path, fr in pool.map(work, files):
            per_file[path] = fr
    return per_file


def build_rich(
    files: list[tuple[str, int, bytes]], kernels, workers: int = 4
) -> RichTables:
    """Concurrent per-file extraction with a deterministic merge.

    Worker threads fill path-keyed slots only; all ordering, dedup, and
    seq assignment happen single-threaded in `globalize` after the join,
    so any worker count converges to identical tables.
    """
    return globalize(extract_many(files, kernels, workers))


def filerich_from_records(path: str, snap) -> FileRich:
    """Rebuild per-file rows for an unchanged path from a previous v2
    snapshot. Previous seqs stand in for line numbers: within one path
    they were assigned in line order, so the merge reproduces them."""
    fr = FileRich(path)
    for r in snap.syms:
        if r.path == path:
            fr.syms.append((r.seq, r.sym, r.name, r.ndigest))
    for r in snap.headings:
        if r.path == path:
            fr.headings.append((r.seq, r.level, r.title, r.tdigest))
    for r in snap.rels:
        if r.src == path:
            fr.rels.append((r.rel, r.dst))
    for r in snap.press:
        if r.path == path:
            fr.press.append(r.pid)
    return fr
