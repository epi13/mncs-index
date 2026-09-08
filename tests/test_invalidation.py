"""Bounded deterministic transitive traversal + invalidation sets.

`mncs_index.graph` walks `depends-on`/`defines` edges transitively;
per-candidate admission is the MNCS `should_visit` verdict (host
mirror when no kernels handle is given). These tests pin the contract
across the shapes that break naive invalidation — chains, diamonds,
fan-out, fan-in, cycles, duplicate edges/definers, missing targets,
renames, deletions — under varied insertion and worker order, and pin
incremental == rebuild including traversals and invalidation sets.
"""

import itertools
import os
import shutil

import pytest
from conftest import write_file

from mncs_index.extract import build_rich
from mncs_index.graph import (
    invalidation_set,
    traverse_dependencies,
    traverse_dependents,
)
from mncs_index.indexer import build_snapshot_v2, incremental_snapshot_v2
from mncs_index.kernels import Kernels
from mncs_index.pipeline import BuildConfig
from mncs_index.query import QueryEngine
from mncs_index.store import Store


def _mod(name, uses=()):
    lines = [b"mncs 0.8;", b"", f"module graph.{name};".encode(), b""]
    for u in uses:
        lines.append(f"use graph.{u} as {u}".encode())
    lines.append(b"")
    lines.append(f"fn {name}_fn() -> (result: i64) {{".encode())
    lines.append(b"    return 0;")
    lines.append(b"}")
    return b"\n".join(lines) + b"\n"


def _build(kernels, corpus_dir, workers=4, seed=7):
    snap, canon, _, _ = build_snapshot_v2(
        corpus_dir, 0, kernels,
        BuildConfig(workers=workers, seed=seed, mncs_digest=False),
    )
    return snap, canon


def _seed_chain(root):
    write_file(root, "c.mncs", _mod("c"))
    write_file(root, "b.mncs", _mod("b", uses=("c",)))
    write_file(root, "a.mncs", _mod("a", uses=("b",)))


def _seed_diamond(root):
    write_file(root, "d.mncs", _mod("d"))
    write_file(root, "b.mncs", _mod("b", uses=("d",)))
    write_file(root, "c.mncs", _mod("c", uses=("d",)))
    write_file(root, "a.mncs", _mod("a", uses=("b", "c")))


def _seed_fanout(root, n=4):
    write_file(root, "hub.mncs", _mod("hub"))
    for i in range(n):
        write_file(root, f"s{i}.mncs", _mod(f"s{i}", uses=("hub",)))


def _seed_cycle(root):
    write_file(root, "c.mncs", _mod("c", uses=("d",)))
    write_file(root, "d.mncs", _mod("d", uses=("c",)))


# -- kernel verdict table -------------------------------------------------


def test_should_visit_truth_table(kernels):
    v = kernels.should_visit
    assert v(False, 0, 5, 0, 10) == 1
    assert v(True, 0, 5, 0, 10) == 0  # visited
    assert v(False, 6, 5, 0, 10) == 0  # past depth
    assert v(False, 5, 5, 0, 10) == 1  # at depth: admitted
    assert v(False, 0, 5, 10, 10) == 0  # budget exhausted
    assert v(False, 0, 5, 11, 10) == 0  # budget overrun
    assert v(False, 0, 0, 0, 1) == 1  # root layer always fits
    assert kernels.depth_next(0) == 1
    assert kernels.depth_next(41) == 42


def test_should_visit_host_mirror_agrees(kernels):
    cases = itertools.product(
        (False, True), (0, 1, 5, 6), (0, 5), (0, 9, 10, 11), (1, 10)
    )
    for visited, depth, max_depth, used, limit in cases:
        assert kernels.should_visit(
            visited, depth, max_depth, used, limit
        ) == Kernels.should_visit_host(visited, depth, max_depth, used, limit)


# -- shapes over real extraction ------------------------------------------


def test_chain(kernels, tmp_path):
    _seed_chain(str(tmp_path))
    snap, _ = _build(kernels, str(tmp_path))
    for backend in (None, kernels):
        deps = traverse_dependencies(snap.rels, "a.mncs", backend)
        assert deps.nodes == ("b.mncs", "c.mncs") and not deps.truncated
        assert traverse_dependencies(
            snap.rels, "c.mncs", backend
        ).nodes == ()
        rdeps = traverse_dependents(snap.rels, "c.mncs", backend)
        assert rdeps.nodes == ("a.mncs", "b.mncs") and not rdeps.truncated
        assert traverse_dependents(
            snap.rels, "a.mncs", backend
        ).nodes == ()
        inv = invalidation_set(snap.rels, ["c.mncs"], backend)
        assert inv.nodes == ("a.mncs", "b.mncs", "c.mncs")
    # Symbol roots resolve like their defining file's wave.
    assert traverse_dependents(snap.rels, "graph.c", kernels).nodes == (
        "a.mncs",
        "b.mncs",
    )


def test_diamond_emits_once(kernels, tmp_path):
    _seed_diamond(str(tmp_path))
    snap, _ = _build(kernels, str(tmp_path))
    deps = traverse_dependencies(snap.rels, "a.mncs", kernels)
    assert deps.nodes == ("b.mncs", "c.mncs", "d.mncs")
    assert list(deps.nodes).count("d.mncs") == 1
    inv = invalidation_set(snap.rels, ["d.mncs"], kernels)
    assert inv.nodes == ("a.mncs", "b.mncs", "c.mncs", "d.mncs")


def test_fanout_and_fanin(kernels, tmp_path):
    _seed_fanout(str(tmp_path), n=4)
    write_file(tmp_path, "leaf.mncs", _mod("leaf",
               uses=("s0", "s1", "s2", "s3")))
    snap, _ = _build(kernels, str(tmp_path))
    spokes = tuple(f"s{i}.mncs" for i in range(4))
    inv = invalidation_set(snap.rels, ["hub.mncs"], kernels)
    assert inv.nodes == tuple(sorted(spokes + ("hub.mncs", "leaf.mncs")))
    # Fan-in: one leaf needs many; changing the leaf disturbs nothing else.
    deps = traverse_dependencies(snap.rels, "leaf.mncs", kernels)
    assert deps.nodes == tuple(sorted(spokes + ("hub.mncs",)))
    assert invalidation_set(snap.rels, ["leaf.mncs"], kernels).nodes == (
        "leaf.mncs",
    )
    # Changing one spoke disturbs the hub's other spokes not at all.
    assert invalidation_set(snap.rels, ["s0.mncs"], kernels).nodes == (
        "leaf.mncs",
        "s0.mncs",
    )


def test_cycle_terminates(kernels, tmp_path):
    _seed_cycle(str(tmp_path))
    snap, _ = _build(kernels, str(tmp_path))
    assert traverse_dependencies(snap.rels, "c.mncs", kernels).nodes == (
        "d.mncs",
    )
    assert traverse_dependents(snap.rels, "c.mncs", kernels).nodes == (
        "d.mncs",
    )
    assert invalidation_set(snap.rels, ["c.mncs"], kernels).nodes == (
        "c.mncs",
        "d.mncs",
    )


def test_duplicate_edges_and_definers(kernels, tmp_path):
    dup = (
        b"mncs 0.8;\n\nmodule graph.dup;\n\n"
        b"use graph.hub as hub\nuse graph.hub as hub\n"
        b"use graph.hub as hub_alias\n"
    )
    write_file(tmp_path, "hub.mncs", _mod("hub"))
    write_file(tmp_path, "dup.mncs", dup)
    # Two files defining one symbol: both are real definers.
    write_file(tmp_path, "twin1.mncs", _mod("twin"))
    write_file(tmp_path, "twin2.mncs", _mod("twin"))
    write_file(tmp_path, "user.mncs", _mod("user", uses=("twin",)))
    snap, _ = _build(kernels, str(tmp_path))
    kinds = [
        (r.src, r.rel, r.dst)
        for r in snap.rels
        if r.src == "dup.mncs" and r.rel == "depends-on"
    ]
    assert kinds == [("dup.mncs", "depends-on", "graph.hub")]
    assert traverse_dependencies(snap.rels, "dup.mncs", kernels).nodes == (
        "hub.mncs",
    )
    assert traverse_dependents(snap.rels, "graph.twin", kernels).nodes == (
        "user.mncs",
    )
    assert traverse_dependencies(snap.rels, "user.mncs", kernels).nodes == (
        "twin1.mncs",
        "twin2.mncs",
    )


def test_missing_target_is_leaf(kernels, tmp_path):
    write_file(tmp_path, "a.mncs", _mod("a", uses=("ghost",)))
    snap, _ = _build(kernels, str(tmp_path))
    assert traverse_dependencies(snap.rels, "a.mncs", kernels).nodes == ()
    assert traverse_dependents(snap.rels, "graph.ghost", kernels).nodes == (
        "a.mncs",
    )
    assert traverse_dependents(snap.rels, "graph.nowhere", kernels).nodes == ()


def test_renamed_target_rekeys(kernels, tmp_path):
    _seed_chain(str(tmp_path))
    os.rename(os.path.join(str(tmp_path), "c.mncs"),
              os.path.join(str(tmp_path), "c2.mncs"))
    snap, _ = _build(kernels, str(tmp_path))
    assert traverse_dependencies(snap.rels, "a.mncs", kernels).nodes == (
        "b.mncs",
        "c2.mncs",
    )
    assert invalidation_set(snap.rels, ["c2.mncs"], kernels).nodes == (
        "a.mncs",
        "b.mncs",
        "c2.mncs",
    )
    assert traverse_dependents(snap.rels, "c.mncs", kernels).nodes == ()


def test_deleted_target_dangles(kernels, tmp_path):
    _seed_chain(str(tmp_path))
    os.remove(os.path.join(str(tmp_path), "c.mncs"))
    snap, _ = _build(kernels, str(tmp_path))
    # b.mncs still depends on graph.c, but no file defines it.
    assert ("b.mncs", "depends-on", "graph.c") in {
        (r.src, r.rel, r.dst) for r in snap.rels
    }
    assert traverse_dependencies(snap.rels, "a.mncs", kernels).nodes == (
        "b.mncs",
    )
    assert traverse_dependents(snap.rels, "graph.c", kernels).nodes == (
        "a.mncs",
        "b.mncs",
    )
    assert invalidation_set(snap.rels, ["b.mncs"], kernels).nodes == (
        "a.mncs",
        "b.mncs",
    )


# -- budgets ----------------------------------------------------------------


def test_budgets_truncate_deterministically(kernels, tmp_path):
    _seed_chain(str(tmp_path))
    snap, _ = _build(kernels, str(tmp_path))
    # One file hop costs 2 edges (file -> symbol -> file).
    mid = traverse_dependencies(snap.rels, "a.mncs", kernels, max_depth=2)
    assert mid.nodes == ("b.mncs",) and mid.truncated
    tiny = traverse_dependencies(snap.rels, "a.mncs", kernels, max_nodes=1)
    assert tiny.nodes == ("b.mncs",) and tiny.truncated
    assert traverse_dependents(
        snap.rels, "c.mncs", kernels, max_nodes=1
    ).truncated
    # Same budgets, both verdict paths, every time.
    for _ in range(3):
        assert traverse_dependencies(
            snap.rels, "a.mncs", None, max_depth=2
        ) == traverse_dependencies(
            snap.rels, "a.mncs", kernels, max_depth=2
        )
        assert traverse_dependents(
            snap.rels, "c.mncs", None, max_nodes=1
        ) == traverse_dependents(
            snap.rels, "c.mncs", kernels, max_nodes=1
        )


# -- order independence -------------------------------------------------------


def test_insertion_and_worker_order_irrelevant(kernels, tmp_path):
    _seed_diamond(str(tmp_path))
    write_file(tmp_path, "hub.mncs", _mod("hub"))
    write_file(tmp_path, "extra.mncs", _mod("extra", uses=("hub", "d")))
    ref, _ = _build(kernels, str(tmp_path), workers=1, seed=7)
    alt, _ = _build(kernels, str(tmp_path), workers=5, seed=99)
    assert [r.canon_line() for r in alt.rels] == [
        r.canon_line() for r in ref.rels
    ]
    for root in ("a.mncs", "extra.mncs", "hub.mncs", "d.mncs"):
        assert traverse_dependencies(
            alt.rels, root, kernels
        ) == traverse_dependencies(ref.rels, root, kernels)
        assert traverse_dependents(
            alt.rels, root, kernels
        ) == traverse_dependents(ref.rels, root, kernels)
    assert invalidation_set(
        alt.rels, ["d.mncs", "hub.mncs"], kernels
    ) == invalidation_set(ref.rels, ["d.mncs", "hub.mncs"], kernels)


def test_permuted_extraction_converges(kernels, tmp_path):
    _seed_diamond(str(tmp_path))
    snap0, _, corpus, _ = build_snapshot_v2(
        str(tmp_path), 0, kernels,
        BuildConfig(workers=1, seed=7, mncs_digest=False),
    )
    kind_of = {}
    for it in corpus.items:
        kind_of[it.path] = kernels.classify_kind(
            it.path.rsplit(".", 1)[-1].encode()
        )
    Blobs = {it.path: it.data for it in corpus.items}
    orders = [list(Blobs), list(reversed(Blobs)), sorted(Blobs)]
    tables = [
        build_rich([(p, kind_of[p], Blobs[p]) for p in order], kernels, w)
        for order, w in zip(orders, (1, 4, 2))
    ]
    rel_lines = [[r.canon_line() for r in t.rels] for t in tables]
    assert rel_lines[1] == rel_lines[0] == rel_lines[2]
    walks = [
        (
            traverse_dependencies(t.rels, "a.mncs", kernels),
            traverse_dependents(t.rels, "d.mncs", kernels),
            invalidation_set(t.rels, ["d.mncs"], kernels),
        )
        for t in tables
    ]
    assert walks[1] == walks[0] == walks[2]
    assert walks[0][0].nodes == ("b.mncs", "c.mncs", "d.mncs")


# -- engine surface -------------------------------------------------------------


def test_query_engine_traversal(kernels, tmp_path):
    _seed_diamond(str(tmp_path))
    snap, _ = _build(kernels, str(tmp_path))
    eng = QueryEngine(snap, kernels, workers=2)
    # Default is the MNCS verdict; kernels=False forces the host mirror.
    assert eng.dependencies("a.mncs").nodes == ("b.mncs", "c.mncs", "d.mncs")
    assert eng.dependencies("a.mncs", kernels=False) == eng.dependencies(
        "a.mncs"
    )
    assert eng.transitive_dependents("d.mncs").nodes == (
        "a.mncs",
        "b.mncs",
        "c.mncs",
    )
    assert eng.transitive_dependents(
        "d.mncs", kernels=False
    ) == eng.transitive_dependents("d.mncs")
    assert eng.invalidation_set(["d.mncs"]).nodes == (
        "a.mncs",
        "b.mncs",
        "c.mncs",
        "d.mncs",
    )
    # Single-hop queries are untouched by the transitive layer.
    assert {r.src for r in eng.dependents("graph.d").records} == {
        "b.mncs",
        "c.mncs",
    }
    assert [(r.src, r.dst) for r in eng.depends_on("a.mncs").records] == [
        ("a.mncs", "graph.b"),
        ("a.mncs", "graph.c"),
    ]


# -- incremental == rebuild, traversals included ---------------------------------


def _publish_gen0(kernels, corpus_dir, store_dir):
    snap, canon, corpus, _ = build_snapshot_v2(
        corpus_dir, 0, kernels,
        BuildConfig(workers=4, seed=7, mncs_digest=False),
    )
    Store(store_dir).publish(
        snap, canon,
        {d.path: 0 for d in snap.docs},
        {it.path: it.crc for it in corpus.items},
        None,
    )


def _incremental(kernels, corpus_dir, store_dir, generation, workers=3):
    store = Store(store_dir)
    prev, established, prev_crc = store.load_head()
    base = prev.generation
    snap, canon, corpus, verdicts = incremental_snapshot_v2(
        corpus_dir, generation, prev, prev_crc, kernels,
        BuildConfig(workers=workers, seed=11, mncs_digest=False),
    )
    estat = dict(established)
    for d in snap.docs:
        estat.setdefault(d.path, generation)
    store.publish(
        snap, canon, estat,
        {it.path: it.crc for it in corpus.items}, base,
    )
    return snap, verdicts


@pytest.mark.parametrize("workers", [1, 5])
def test_incremental_rebuild_agreement_incl_invalidation(
    kernels, tmp_path, workers
):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    store = tmp_path / "store"
    store.mkdir()
    _seed_chain(str(corpus))
    write_file(str(corpus), "hub.mncs", _mod("hub"))
    write_file(str(corpus), "s0.mncs", _mod("s0", uses=("hub",)))
    _publish_gen0(kernels, str(corpus), str(store))
    # Change the hub's content, add a spoke, delete the chain tail.
    write_file(str(corpus), "hub.mncs", _mod("hub") + b"\n")
    write_file(str(corpus), "s1.mncs", _mod("s1", uses=("hub",)))
    os.remove(corpus / "c.mncs")
    inc, verdicts = _incremental(kernels, str(corpus), str(store), 1,
                                 workers=workers)
    assert verdicts["hub.mncs"] == 3
    assert verdicts["s1.mncs"] == 1
    assert verdicts["c.mncs"] == 2
    ref, _, _, _ = build_snapshot_v2(
        str(corpus), 0, kernels,
        BuildConfig(workers=2, seed=21, mncs_digest=False),
    )
    assert [r.canon_line() for r in inc.rels] == [
        r.canon_line() for r in ref.rels
    ]
    roots = ["a.mncs", "b.mncs", "hub.mncs", "s0.mncs", "s1.mncs"]
    for root in roots:
        assert traverse_dependencies(
            inc.rels, root, kernels
        ) == traverse_dependencies(ref.rels, root, kernels)
        assert traverse_dependents(
            inc.rels, root, kernels
        ) == traverse_dependents(ref.rels, root, kernels)
    for changed in (["hub.mncs"], ["b.mncs"], ["hub.mncs", "b.mncs"]):
        assert invalidation_set(
            inc.rels, changed, kernels
        ) == invalidation_set(ref.rels, changed, kernels)
    # The surviving chain still resolves through the dangling tail target.
    assert traverse_dependencies(inc.rels, "a.mncs", kernels).nodes == (
        "b.mncs",
    )
    assert invalidation_set(inc.rels, ["hub.mncs"], kernels).nodes == (
        "hub.mncs",
        "s0.mncs",
        "s1.mncs",
    )


def _edge(src, rel, dst):
    from types import SimpleNamespace

    return SimpleNamespace(src=src, rel=rel, dst=dst)


def _two_hubs():
    """Two independent hubs, each with two dependents (kernel-free rows)."""
    rows = [
        _edge("hub1.mncs", "defines", "h1"),
        _edge("hub2.mncs", "defines", "h2"),
    ]
    for i in (1, 2):
        rows.append(_edge(f"a{i}.mncs", "depends-on", "h1"))
        rows.append(_edge(f"b{i}.mncs", "depends-on", "h2"))
    return rows


def test_invalidation_set_budget_is_global():
    """`max_nodes` caps TOTAL dependents across roots, not per root.

    Two roots with two dependents each under `max_nodes=2` must yield
    at most 2 dependents (the old per-walk budget admitted 2+2).
    Changed paths are always present even when the wave truncates.
    """
    rows = _two_hubs()
    full = invalidation_set(rows, ["hub1.mncs", "hub2.mncs"], max_nodes=100)
    assert full.truncated is False
    assert full.nodes == (
        "a1.mncs",
        "a2.mncs",
        "b1.mncs",
        "b2.mncs",
        "hub1.mncs",
        "hub2.mncs",
    )
    capped = invalidation_set(rows, ["hub1.mncs", "hub2.mncs"], max_nodes=2)
    dependents = [n for n in capped.nodes if n not in ("hub1.mncs", "hub2.mncs")]
    assert len(dependents) <= 2
    assert capped.truncated is True
    assert "hub1.mncs" in capped.nodes and "hub2.mncs" in capped.nodes
    # Deterministic under root permutation.
    flipped = invalidation_set(rows, ["hub2.mncs", "hub1.mncs"], max_nodes=2)
    assert flipped.nodes == capped.nodes


def test_depth_next_wrap_characterization(kernels):
    """`depth_next` wraps at u64 max by design; traversal never issues it.

    Admission precedes every `_step` and refuses depth > max_depth, so
    traversal depths stay within max_depth+1 (default 64+1). The wrap
    is characterized here, not reachable in a walk.
    """
    assert kernels.depth_next(0) == 1
    assert kernels.depth_next(41) == 42
    assert kernels.depth_next(2**64 - 1) == 0
