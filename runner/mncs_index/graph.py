"""Bounded deterministic transitive dependency traversal + invalidation sets.

Single-hop edges (`depends-on`, `defines`) are extracted per file and
merged by identity sort (`extract.globalize`); this module walks them
transitively. The split with MNCS follows PRESS-015: the per-candidate
admission verdict (visited / depth / budget) is an MNCS kernel decision
(`src/graph.mncs::should_visit`, with `depth_next` for layer stepping)
whenever a kernels handle is supplied, while graph-local structure —
adjacency, node identity, visited sets, canonical ordering — is
explicit host code, because MNCS has no relation/table values, joins,
or unbounded traversal.

Contract (applies to every traversal here):

- Identity: a file node is its exact corpus path string; a symbol node
  is its exact edge `dst` string (e.g. `graph.hub`). Comparison is
  byte-exact; no normalization, no guessing.
- Edges walked: forward follows `depends-on` (file -> symbol) then
  resolves each symbol through `defines` (symbol -> file). Reverse
  follows the same edges backwards. A `depends-on` target with no
  `defines` row (missing/renamed/deleted target) is a leaf: the walk
  stops, it never raises.
- Visited: first visit wins. Each node is admitted at most once, so
  cycles, diamonds, and duplicate edges terminate and emit once.
- Ordering: adjacency is deduplicated and sorted byte-lexicographically
  before the walk; the frontier is FIFO (BFS); the emitted node list is
  re-sorted canonically. Insertion order and worker completion order
  cannot leak into the result.
- Budget: `max_depth` counts edges from the root (root = 0, so one
  file hop costs 2 edges: file -> symbol -> file). `max_nodes` caps
  emitted file nodes. Any candidate refused on depth or budget sets
  `truncated=True`; refusal on already-visited never truncates.
- Duplicates: duplicate edges collapse in the adjacency sets — they
  are never double-walked nor double-emitted.
- Kernel/host agreement: with `kernels=None` the host mirror
  (`Kernels.should_visit_host`, `depth + 1`) decides; otherwise the
  MNCS kernel decides. Both implement the same verdict, pinned
  differentially by `tests/test_invalidation.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DEFAULT_MAX_DEPTH = 64
DEFAULT_MAX_NODES = 4096


@dataclass(frozen=True)
class Traversal:
    """One bounded transitive walk: sorted file paths plus budget state."""

    nodes: tuple = ()
    truncated: bool = False


@dataclass
class _Graph:
    """Sorted, deduplicated adjacency over one snapshot's edge rows."""

    # file path -> sorted depends-on symbols
    depends: dict = field(default_factory=dict)
    # symbol -> sorted files defining it
    defines: dict = field(default_factory=dict)
    # symbol -> sorted files depending on it
    rdepends: dict = field(default_factory=dict)
    # file path -> sorted symbols it defines
    defines_of: dict = field(default_factory=dict)


def build_graph(rels) -> _Graph:
    """Project edge rows onto sorted file<->symbol adjacency.

    Accepts a Snapshot (reads `.rels`) or any iterable of rows with
    `.src`/`.rel`/`.dst`. Only `depends-on` and `defines` rows shape
    the walk; every other kind is ignored. Input order is irrelevant:
    every neighbor list is deduplicated and sorted.
    """
    rows = getattr(rels, "rels", rels)
    depends: dict[str, set] = {}
    defines: dict[str, set] = {}
    rdepends: dict[str, set] = {}
    defines_of: dict[str, set] = {}
    for r in rows:
        rel = getattr(r, "rel", None)
        src = getattr(r, "src", None)
        dst = getattr(r, "dst", None)
        if rel == "depends-on":
            depends.setdefault(src, set()).add(dst)
            rdepends.setdefault(dst, set()).add(src)
        elif rel == "defines":
            defines.setdefault(dst, set()).add(src)
            defines_of.setdefault(src, set()).add(dst)
    return _Graph(
        depends={k: sorted(v) for k, v in depends.items()},
        defines={k: sorted(v) for k, v in defines.items()},
        rdepends={k: sorted(v) for k, v in rdepends.items()},
        defines_of={k: sorted(v) for k, v in defines_of.items()},
    )


def _admit(kernels, depth: int, max_depth: int, used: int, limit: int) -> int:
    """Admission verdict for one candidate (host calls, kernel decides).

    Contract: every call site checks `key in visited` first and passes
    `visited=False`, so the kernel's visited branch never fires through
    traversal — it is pinned directly by unit tests
    (`test_should_visit_truth_table`, visited=True row) and by the
    host-mirror differential, not by integration paths. Depth is
    likewise always admitted-clamped before `_step`, so `depth_next`
    wrapping at u64 max is unreachable in traversal (characterized by
    `test_depth_next_wrap_characterization`, never issued).
    """
    if kernels is None:
        from .kernels import Kernels as _K

        return _K.should_visit_host(False, depth, max_depth, used, limit)
    return kernels.should_visit(False, depth, max_depth, used, limit)


def _step(kernels, depth: int) -> int:
    if kernels is None:
        return depth + 1
    return kernels.depth_next(depth)


def traverse_dependencies(
    rels,
    root: str,
    kernels=None,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_nodes: int = DEFAULT_MAX_NODES,
) -> Traversal:
    """Files transitively needed by `root` (forward closure, root excluded).

    Walks file -> {depends-on symbols} -> {defining files} -> ... in
    BFS order. Unknown roots (no rows mention them) yield no nodes.
    """
    g = build_graph(rels)
    if root not in g.depends and root not in g.defines_of:
        return Traversal(nodes=(), truncated=False)
    visited = {("file", root)}
    frontier: list[tuple[str, str, int]] = [("file", root, 0)]
    emitted: list[str] = []
    truncated = False
    while frontier:
        kind, ident, depth = frontier.pop(0)
        if kind == "file":
            neighbors = [("sym", s) for s in g.depends.get(ident, ())]
        else:
            neighbors = [("file", f) for f in g.defines.get(ident, ())]
        for nkind, nid in neighbors:
            key = (nkind, nid)
            if key in visited:
                continue
            ndepth = _step(kernels, depth)
            if _admit(kernels, ndepth, max_depth, len(emitted), max_nodes) == 0:
                truncated = True
                continue
            visited.add(key)
            frontier.append((nkind, nid, ndepth))
            if nkind == "file" and nid != root:
                emitted.append(nid)
    return Traversal(nodes=tuple(sorted(emitted)), truncated=truncated)


def traverse_dependents(
    rels,
    node: str,
    kernels=None,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_nodes: int = DEFAULT_MAX_NODES,
) -> Traversal:
    """Files transitively needing `node` (reverse closure).

    `node` may be a file path (starts from the symbols it defines) or
    a bare symbol (starts from the files depending on it). The start
    file itself is never emitted; the caller adds changed paths
    explicitly via `invalidation_set`. Unknown nodes yield no nodes.
    """
    g = build_graph(rels)
    is_file = node in g.defines_of or node in g.depends
    if is_file:
        seeds = [("sym", s) for s in g.defines_of.get(node, ())]
        if not seeds and node not in g.depends:
            return Traversal(nodes=(), truncated=False)
    else:
        if node not in g.rdepends and node not in g.defines:
            return Traversal(nodes=(), truncated=False)
        seeds = []
    visited = {("file", node)} if is_file else {("sym", node)}
    frontier: list[tuple[str, str, int]] = []
    for nkind, nid in seeds:
        key = (nkind, nid)
        if key in visited:
            continue
        ndepth = _step(kernels, 0)
        if _admit(kernels, ndepth, max_depth, 0, max_nodes) == 0:
            return Traversal(nodes=(), truncated=True)
        visited.add(key)
        frontier.append((nkind, nid, ndepth))
    emitted_seed: list[str] = []
    if not is_file:
        for f in g.rdepends.get(node, ()):
            key = ("file", f)
            if key in visited:
                continue
            ndepth = _step(kernels, 0)
            if (
                _admit(kernels, ndepth, max_depth, len(emitted_seed), max_nodes)
                == 0
            ):
                return Traversal(
                    nodes=tuple(sorted(emitted_seed)), truncated=True
                )
            visited.add(key)
            frontier.append(("file", f, ndepth))
            emitted_seed.append(f)
    emitted: list[str] = list(emitted_seed)
    # Seed refusal returns early above, so the wave starts untruncated.
    truncated = False
    while frontier:
        kind, ident, depth = frontier.pop(0)
        if kind == "sym":
            neighbors = [("file", f) for f in g.rdepends.get(ident, ())]
        else:
            neighbors = [("sym", s) for s in g.defines_of.get(ident, ())]
        for nkind, nid in neighbors:
            key = (nkind, nid)
            if key in visited:
                continue
            ndepth = _step(kernels, depth)
            if _admit(kernels, ndepth, max_depth, len(emitted), max_nodes) == 0:
                truncated = True
                continue
            visited.add(key)
            frontier.append((nkind, nid, ndepth))
            if nkind == "file" and not (is_file and nid == node):
                emitted.append(nid)
    return Traversal(nodes=tuple(sorted(emitted)), truncated=truncated)


def invalidation_set(
    rels,
    changed: list[str],
    kernels=None,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_nodes: int = DEFAULT_MAX_NODES,
) -> Traversal:
    """Files to revalidate when `changed` files change.

    The union of the changed paths themselves (verbatim, deduplicated —
    even paths the graph never mentions) and every file transitively
    depending on them. `max_nodes` bounds the TOTAL dependent set
    across all roots (global cap, enforced by shrinking each sub-walk's
    budget by what earlier roots already gathered), not the changed
    list: changed paths are always present even when the wave
    truncates.
    """
    g_rows = rels  # built per walk below; kept as rows for determinism
    changed_unique = sorted(set(changed))
    affected: set[str] = set(changed_unique)
    truncated = False
    for path in changed_unique:
        gathered = len(affected) - len(changed_unique)
        walk = traverse_dependents(
            g_rows,
            path,
            kernels,
            max_depth=max_depth,
            max_nodes=max(0, max_nodes - gathered),
        )
        affected.update(walk.nodes)
        truncated = truncated or walk.truncated
    return Traversal(nodes=tuple(sorted(affected)), truncated=truncated)
