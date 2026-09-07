"""Snapshot-consistent query engine (first query language, RFC 0004).

Every query executes against one loaded snapshot and returns records in
canonical order. Substring matching over stored tokens goes through the
MNCS `contains8` kernel for terms up to 8 bytes (evaluated concurrently);
longer terms use host-side substring under the same predicate, which the
differential suite pins to the kernel on short samples (PRESS-005).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from .kernels import Kernels
from .model import Snapshot


@dataclass
class QueryResult:
    snapshot_id: str
    generation: int
    records: list = field(default_factory=list)
    total: int = 0
    limited: bool = False


class QueryEngine:
    def __init__(self, snap: Snapshot, kernels: Kernels, workers: int = 4):
        self.snap = snap
        self.k = kernels
        self.workers = max(1, workers)
        self.path_index = {d.path: d for d in snap.docs}
        self.digest_index: dict[int, list] = {}
        for d in snap.docs:
            self.digest_index.setdefault(d.digest, []).append(d)
        self.kind_index: dict[int, list] = {}
        for d in snap.docs:
            self.kind_index.setdefault(d.kind, []).append(d)
        self.tokens_by_doc: dict[str, list[str]] = {}
        for t in snap.terms:
            self.tokens_by_doc.setdefault(t.path, []).append(t.token)

    def _finish(self, docs: list, limit: int | None) -> QueryResult:
        docs = sorted(docs, key=lambda d: d.sort_key())
        total = len(docs)
        limited = False
        if limit is not None and len(docs) > limit:
            docs = docs[:limit]
            limited = True
        return QueryResult(
            snapshot_id=self.snap.snapshot_id,
            generation=self.snap.generation,
            records=docs,
            total=total,
            limited=limited,
        )

    def by_id(self, digest_hex: str, limit=None) -> QueryResult:
        return self.by_digest_query(int(digest_hex, 16), limit)

    def by_digest_query(self, digest: int, limit=None) -> QueryResult:
        return self._finish(list(self.digest_index.get(digest, [])), limit)

    def by_path(self, path: str, limit=None) -> QueryResult:
        doc = self.path_index.get(path)
        return self._finish([doc] if doc is not None else [], limit)

    def by_kind(self, kind: int, limit=None) -> QueryResult:
        return self._finish(list(self.kind_index.get(kind, [])), limit)

    def term_substring(self, term: str, limit=None) -> QueryResult:
        needle = term.encode("utf-8")
        docs = list(self.path_index.values())
        if len(needle) <= 8:
            hits = self._mncs_substring(docs, needle)
        else:
            hits = [d for d in docs if self._host_substring(d, needle)]
        return self._finish(hits, limit)

    def _host_substring(self, doc, needle: bytes) -> bool:
        for tok in self.tokens_by_doc.get(doc.path, []):
            if needle.decode("utf-8", "strict") in tok:
                return True
        return False

    def _mncs_substring(self, docs: list, needle: bytes) -> list:
        """Evaluate the MNCS `contains8` predicate per document, in parallel."""

        def check(doc) -> object | None:
            for tok in self.tokens_by_doc.get(doc.path, []):
                if self.k.contains8(tok.encode("utf-8"), needle):
                    return doc
            return None

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            found = [d for d in pool.map(check, docs) if d is not None]
        # Thread completion order must not leak into results.
        return sorted(found, key=lambda d: d.sort_key())
