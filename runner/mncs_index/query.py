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
    def __init__(
        self,
        snap: Snapshot,
        kernels: Kernels,
        workers: int = 4,
        established: dict | None = None,
    ):
        self.snap = snap
        self.k = kernels
        self.workers = max(1, workers)
        # Per-path establishment generations (store sidecar, non-canonical
        # publication metadata): backs `provenance`. Empty when the caller
        # has no store handle; v1 queries never touch it.
        self.established = dict(established) if established else {}
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
        # canonical-v2 indexes (empty on v1 snapshots).
        self.sym_by_name: dict[str, list] = {}
        self.sym_by_path: dict[str, list] = {}
        for r in snap.syms:
            self.sym_by_name.setdefault(r.name, []).append(r)
            self.sym_by_path.setdefault(r.path, []).append(r)
        self.rel_by_src: dict[str, list] = {}
        self.rel_by_dst: dict[str, list] = {}
        self.rel_by_kind: dict[str, list] = {}
        for r in snap.rels:
            self.rel_by_src.setdefault(r.src, []).append(r)
            self.rel_by_dst.setdefault(r.dst, []).append(r)
            self.rel_by_kind.setdefault(r.rel, []).append(r)
        self.headings_by_path: dict[str, list] = {}
        for r in snap.headings:
            self.headings_by_path.setdefault(r.path, []).append(r)
        self.press_by_id: dict[str, list] = {}
        self.press_by_path: dict[str, list] = {}
        for r in snap.press:
            self.press_by_id.setdefault(r.pid, []).append(r)
            self.press_by_path.setdefault(r.path, []).append(r)

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

    # -- canonical-v2 queries (RFC 0004 relationship/provenance classes) --
    def _finish_rich(self, records: list, limit=None) -> QueryResult:
        ordered = sorted(records, key=lambda r: r.sort_key())
        total = len(ordered)
        limited = False
        if limit is not None and len(ordered) > limit:
            ordered = ordered[:limit]
            limited = True
        return QueryResult(
            snapshot_id=self.snap.snapshot_id,
            generation=self.snap.generation,
            records=ordered,
            total=total,
            limited=limited,
        )

    def defines(self, name: str, limit=None) -> QueryResult:
        """`defines` edges whose target is `name` (who defines it)."""
        hits = [r for r in self.rel_by_dst.get(name, []) if r.rel == "defines"]
        return self._finish_rich(hits, limit)

    def references(self, dst: str, limit=None) -> QueryResult:
        """`references` edges pointing at `dst` (symbol, target, or file)."""
        hits = [r for r in self.rel_by_dst.get(dst, []) if r.rel == "references"]
        return self._finish_rich(hits, limit)

    def depends_on(self, path: str, limit=None) -> QueryResult:
        """`depends-on` edges out of `path` (what it needs)."""
        hits = [r for r in self.rel_by_src.get(path, []) if r.rel == "depends-on"]
        return self._finish_rich(hits, limit)

    def dependents(self, dst: str, limit=None) -> QueryResult:
        """`depends-on` edges pointing at `dst` (who needs it)."""
        hits = [r for r in self.rel_by_dst.get(dst, []) if r.rel == "depends-on"]
        return self._finish_rich(hits, limit)

    # -- transitive dependency traversal (bounded, deterministic) --
    def dependencies(
        self,
        path: str,
        max_depth: int = 64,
        max_nodes: int = 4096,
        kernels=None,
    ):
        """Files transitively needed by `path` (forward closure).

        `kernels` selects the admission verdict: `None`/`True` (default)
        uses the engine's MNCS handle (`self.k`, `should_visit` kernel),
        `False` forces the exact host mirror, or pass a `Kernels`
        instance explicitly. See `mncs_index.graph` for the
        identity/visited/ordering/budget/cycle/duplicate contract.
        """
        from . import graph as _graph

        k = self.k if kernels is None or kernels is True else kernels
        if kernels is False:
            k = None
        return _graph.traverse_dependencies(
            self.snap.rels, path, k, max_depth=max_depth, max_nodes=max_nodes
        )

    def transitive_dependents(
        self,
        node: str,
        max_depth: int = 64,
        max_nodes: int = 4096,
        kernels=None,
    ):
        """Files transitively needing `node` (reverse closure).

        `node` may be a file path or a symbol. Same kernels
        convention as `dependencies`.
        """
        from . import graph as _graph

        k = self.k if kernels is None or kernels is True else kernels
        if kernels is False:
            k = None
        return _graph.traverse_dependents(
            self.snap.rels, node, k, max_depth=max_depth, max_nodes=max_nodes
        )

    def invalidation_set(
        self,
        changed: list,
        max_depth: int = 64,
        max_nodes: int = 4096,
        kernels=None,
    ):
        """Files to revalidate when `changed` files change."""
        from . import graph as _graph

        k = self.k if kernels is None or kernels is True else kernels
        if kernels is False:
            k = None
        return _graph.invalidation_set(
            self.snap.rels, list(changed), k,
            max_depth=max_depth, max_nodes=max_nodes,
        )

    def rfc_refs(self, num: str, limit=None) -> QueryResult:
        """`rfc-ref` edges for one RFC number, e.g. "0003"."""
        hits = [r for r in self.rel_by_dst.get(num, []) if r.rel == "rfc-ref"]
        return self._finish_rich(hits, limit)

    def pressure(self, pid: str, limit=None) -> QueryResult:
        """Pressure-registry mentions of one ID, e.g. "PRESS-001"."""
        return self._finish_rich(list(self.press_by_id.get(pid, [])), limit)

    def symbols(self, name: str, limit=None) -> QueryResult:
        """Source declarations of one symbol name across the corpus."""
        return self._finish_rich(list(self.sym_by_name.get(name, [])), limit)

    def headings_for(self, path: str, limit=None) -> QueryResult:
        """Section headings of one document, in document order."""
        return self._finish_rich(list(self.headings_by_path.get(path, [])), limit)

    def provenance(self, path: str) -> dict:
        """Where one path's fact came from: snapshot identity, doc digest,
        and the establishment generation from the store sidecar (which
        publish event first carried the path). Unknown paths report
        `found: False` rather than raising."""
        doc = self.path_index.get(path)
        if doc is None:
            return {
                "snapshot_id": self.snap.snapshot_id,
                "generation": self.snap.generation,
                "path": path,
                "found": False,
                "established_generation": self.established.get(path),
            }
        return {
            "snapshot_id": self.snap.snapshot_id,
            "generation": self.snap.generation,
            "path": path,
            "found": True,
            "digest": f"{doc.digest & ((1 << 64) - 1):016x}",
            "size": doc.size,
            "established_generation": self.established.get(path),
        }

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
