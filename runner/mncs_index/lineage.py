"""Deterministic rename lineage over remove+add verdicts.

Change verdicts never guess renames: a disappeared path is verdict 2
(removed) and a new path is verdict 1 (added), decided by the MNCS
`classify_change` kernel in `indexer.py`. Lineage is an advisory layer
over those verdicts, keyed by authoritative MNCS content digests (never
by size/CRC hints):

- exactly one removed path and exactly one added path share a digest
  -> `moved` (provable content identity: the bytes survived the move);
- every other shape — no digest match, or two or more candidates on
  either side sharing one digest (copies, duplicates, splits, merges)
  -> unlinked `removed` + `added`: ambiguity resolves to remove+add,
  never to a guessed rename.

`resolve` is a pure function of (removed, added) digest pairs: both
sides are grouped and emitted in sorted order, so any caller order
converges to identical lineage. Verdicts are untouched — a `moved`
pair still carries verdicts (2, 1), which is what the store and the
incremental equivalence tests pin.
"""

from __future__ import annotations

from dataclasses import dataclass

MOVED = "moved"
ADDED = "added"
REMOVED = "removed"


@dataclass(frozen=True)
class Lineage:
    old: str | None  # removed path; None for pure additions
    new: str | None  # added path; None for pure removals
    kind: str  # "moved" | "added" | "removed"
    digest: int  # authoritative MNCS content digest of the bytes

    def __post_init__(self):
        if self.kind not in (MOVED, ADDED, REMOVED):
            raise ValueError(f"unknown lineage kind: {self.kind!r}")
        if self.kind == MOVED and (self.old is None or self.new is None):
            raise ValueError("moved lineage needs both old and new paths")

    def sort_key(self) -> tuple:
        return (self.old or "", self.new or "", self.kind)


def resolve(
    removed: list[tuple[str, int]],
    added: list[tuple[str, int]],
) -> list[Lineage]:
    """Link removed/added paths by content identity.

    `removed`/`added` are (path, MNCS digest) pairs from one incremental
    step (verdict-2 paths with their previous digests, verdict-1 paths
    with their fresh digests). A digest group links as `moved` only
    when it holds exactly one removed and exactly one added path;
    anything else stays unlinked remove+add. Output is canonically
    ordered.
    """
    by_digest_r: dict[int, list[str]] = {}
    for path, digest in removed:
        by_digest_r.setdefault(digest, []).append(path)
    by_digest_a: dict[int, list[str]] = {}
    for path, digest in added:
        by_digest_a.setdefault(digest, []).append(path)
    out: list[Lineage] = []
    for digest in sorted(set(by_digest_r) | set(by_digest_a)):
        olds = sorted(by_digest_r.get(digest, []))
        news = sorted(by_digest_a.get(digest, []))
        if (
            len(olds) == 1
            and len(news) == 1
            and olds[0] != news[0]
        ):
            out.append(Lineage(old=olds[0], new=news[0], kind=MOVED, digest=digest))
        else:
            out.extend(
                Lineage(old=p, new=None, kind=REMOVED, digest=digest) for p in olds
            )
            out.extend(
                Lineage(old=None, new=p, kind=ADDED, digest=digest) for p in news
            )
    out.sort(key=lambda item: item.sort_key())
    return out
