"""Deterministic corpus discovery.

Filesystem enumeration order must never become canonical ordering: this
module returns items in a stable-but-arbitrary order and the pipeline
explicitly shuffles (seeded) or sorts downstream. Provenance paths are
normalized to forward-slash relative form.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from .kernels import ADMITTED_EXTENSIONS
from .model import check_path_safe, crc32_of, discovery_id

MAX_FILE_BYTES = 64 * 1024 * 1024


@dataclass
class FileItem:
    path: str  # normalized relative posix path
    size: int
    crc: int  # crc32 content hint (RFC 0005: hints, not truth)
    data: bytes


@dataclass
class CorpusSnapshot:
    root: str
    items: list
    snapshot_id: str
    skipped: int


def _extension(name: str) -> str:
    base = name.rsplit("/", 1)[-1]
    if "." not in base or base.startswith(".") and base.count(".") == 1:
        return ""
    return base.rsplit(".", 1)[-1].lower()


def discover(root: str) -> CorpusSnapshot:
    """Walk `root`, read admitted files, return declared source snapshot."""
    if not os.path.isdir(root):
        raise DiscoveryError(f"corpus root is not a directory: {root}")
    items: list[FileItem] = []
    skipped = 0
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        for name in sorted(filenames):
            full = os.path.join(dirpath, name)
            try:
                if os.path.islink(full) or not os.path.isfile(full):
                    skipped += 1
                    continue
                with open(full, "rb") as fh:
                    data = fh.read(MAX_FILE_BYTES + 1)
            except OSError:
                raise DiscoveryError(f"cannot read during discovery: {full}")
            if len(data) > MAX_FILE_BYTES:
                raise DiscoveryError(f"file exceeds size bound: {full}")
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            try:
                rel.encode("utf-8")
                check_path_safe(rel)
            except (UnicodeEncodeError, ValueError) as exc:
                raise DiscoveryError(f"path cannot be canonicalized: {rel!r}: {exc}")
            if _extension(rel) not in ADMITTED_EXTENSIONS:
                skipped += 1
                continue
            items.append(
                FileItem(path=rel, size=len(data), crc=crc32_of(data), data=data)
            )
    snap_id = discovery_id([(it.path, it.size, it.crc) for it in items])
    return CorpusSnapshot(root=root, items=items, snapshot_id=snap_id, skipped=skipped)


class DiscoveryError(Exception):
    pass
