"""Dependency/invalidation graph: incremental == clean rebuild for rich tables.

Records *and* relationships invalidate by content identity: a changed
file re-extracts, an unchanged file reuses its rows, a removed file
drops its rows, and the deterministic `globalize` merge converges to
exactly what a clean rebuild produces. These tests pin that contract
across the graph shapes that break naive invalidation — add, remove,
rename (edge re-keying), disappearing targets (dangling references),
fan-out (one hub, many spokes), and use-cycles — asserting verdict
codes, query agreement (`depends_on`/`dependents`/`defines`/
`references`/`pressure`), and byte-level rich-table equality against a
clean rebuild at different workers/seeds.

Rename lineage (`mncs_index.lineage`) is pinned here too: verdicts
never guess renames (remove 2 + add 1), and content-identity linkage
reports `moved` only for provable 1:1 digest matches — every ambiguous
shape resolves to unlinked remove+add.
"""

import os
import shutil

import pytest
from conftest import write_file
from mncs_index.indexer import build_snapshot_v2, incremental_snapshot_v2
from mncs_index.lineage import ADDED, MOVED, REMOVED, Lineage, resolve
from mncs_index.pipeline import BuildConfig
from mncs_index.query import QueryEngine
from mncs_index.store import Store

HUB = (
    b"mncs 0.8;\n"
    b"\n"
    b"module graph.hub;\n"
    b"\n"
    b"fn hub_fn() -> (result: i64) {\n"
    b"    return 1;\n"
    b"}\n"
)

HUB_CHANGED = (
    b"mncs 0.8;\n"
    b"\n"
    b"module graph.hub;\n"
    b"\n"
    b"fn hub_fn() -> (result: i64) {\n"
    b"    return 2;\n"
    b"}\n"
    b"\n"
    b"fn hub_extra() -> (result: i64) {\n"
    b"    return 3;\n"
    b"}\n"
)


def _spoke(name: str) -> bytes:
    return (
        b"mncs 0.8;\n"
        b"\n"
        b"module graph." + name.encode() + b";\n"
        b"\n"
        b"use graph.hub as hub\n"
        b"\n"
        b"fn " + name.encode() + b"_fn() -> (result: i64) {\n"
        b"    return 0;\n"
        b"}\n"
    )


DOC = b"# Graph\n\nSee [hub](hub.mncs).\n\nPRESS-007 anchors the queue.\n"


def _seed_corpus(root: str) -> None:
    write_file(root, "hub.mncs", HUB)
    write_file(root, "a.mncs", _spoke("a"))
    write_file(root, "b.mncs", _spoke("b"))
    write_file(root, "doc.md", DOC)


@pytest.fixture(scope="module")
def baseline(kernels, tmp_path_factory):
    """One published gen-0 graph template; tests copy it for isolation."""
    tmp = tmp_path_factory.mktemp("graph")
    corpus = tmp / "corpus"
    corpus.mkdir()
    _seed_corpus(str(corpus))
    store = tmp / "store"
    store.mkdir()
    cfg = BuildConfig(workers=4, seed=7, mncs_digest=False)
    snap, canon, corpus_snap, _ = build_snapshot_v2(str(corpus), 0, kernels, cfg)
    Store(str(store)).publish(
        snap,
        canon,
        {d.path: 0 for d in snap.docs},
        {it.path: it.crc for it in corpus_snap.items},
        None,
    )
    return str(corpus), str(store)


@pytest.fixture()
def branch(baseline, tmp_path):
    corpus_src, store_src = baseline
    corpus = tmp_path / "corpus"
    store = tmp_path / "store"
    shutil.copytree(corpus_src, corpus)
    shutil.copytree(store_src, store)
    return str(corpus), str(store)


def _incremental(kernels, corpus_dir, store_dir, generation):
    store = Store(store_dir)
    prev, established, prev_crc = store.load_head()
    base = prev.generation
    snap, canon, corpus, verdicts = incremental_snapshot_v2(
        corpus_dir,
        generation,
        prev,
        prev_crc,
        kernels,
        BuildConfig(workers=4, seed=7, mncs_digest=False),
    )
    estat = dict(established)
    for d in snap.docs:
        estat.setdefault(d.path, generation)
    store.publish(
        snap, canon, estat, {it.path: it.crc for it in corpus.items}, base
    )
    return snap, canon, verdicts


def _rebuild(kernels, corpus_dir):
    snap, canon, _, _ = build_snapshot_v2(
        corpus_dir, 0, kernels, BuildConfig(workers=2, seed=21, mncs_digest=False)
    )
    return snap, canon


def _rich_key(snap):
    return (
        snap.index_hash,
        [r.canon_line() for r in snap.syms],
        [r.canon_line() for r in snap.headings],
        [r.canon_line() for r in snap.rels],
        [r.canon_line() for r in snap.press],
        [d.canon_line() for d in snap.docs],
    )


def _query_view(snap, kernels):
    eng = QueryEngine(snap, kernels, workers=2)
    return {
        "depends_on_a": [r.canon_line() for r in eng.depends_on("a.mncs").records],
        "dependents_hub": [
            r.canon_line() for r in eng.dependents("graph.hub").records
        ],
        "defines_hub": [r.canon_line() for r in eng.defines("graph.hub").records],
        "references_hub_file": [
            r.canon_line() for r in eng.references("hub.mncs").records
        ],
        "press": [r.canon_line() for r in eng.pressure("PRESS-007").records],
        "headings": [r.canon_line() for r in eng.headings_for("doc.md").records],
    }


def _assert_converged(kernels, inc, corpus_dir):
    ref, _ = _rebuild(kernels, corpus_dir)
    assert _rich_key(inc) == _rich_key(ref)
    assert _query_view(inc, kernels) == _query_view(ref, kernels)
    return ref


def _lineage_of(prev, snap, verdicts):
    removed = [(d.path, d.digest) for d in prev.docs if verdicts.get(d.path) == 2]
    added = [(d.path, d.digest) for d in snap.docs if verdicts.get(d.path) == 1]
    return resolve(removed, added)


def test_add_file_with_edges(kernels, branch):
    corpus_dir, store_dir = branch
    write_file(corpus_dir, "c.mncs", _spoke("c"))
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["c.mncs"] == 1
    assert _lineage_of(Store(store_dir).load(0)[0], inc, verdicts) == [
        Lineage(old=None, new="c.mncs", kind=ADDED, digest=next(
            d.digest for d in inc.docs if d.path == "c.mncs")),
    ]
    ref = _assert_converged(kernels, inc, corpus_dir)
    eng = QueryEngine(ref, kernels, workers=2)
    assert {r.src for r in eng.dependents("graph.hub").records} == {
        "a.mncs",
        "b.mncs",
        "c.mncs",
    }


def test_remove_file_drops_edges(kernels, branch):
    corpus_dir, store_dir = branch
    os.remove(os.path.join(corpus_dir, "b.mncs"))
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["b.mncs"] == 2
    assert all(r.path != "b.mncs" for r in inc.syms)
    assert all(r.src != "b.mncs" for r in inc.rels)
    ref = _assert_converged(kernels, inc, corpus_dir)
    eng = QueryEngine(ref, kernels, workers=2)
    assert {r.src for r in eng.dependents("graph.hub").records} == {"a.mncs"}
    assert eng.symbols("b_fn").total == 0


def test_rename_rekeys_edges_and_links_moved(kernels, branch):
    corpus_dir, store_dir = branch
    prev, _, _ = Store(store_dir).load_head()
    os.rename(
        os.path.join(corpus_dir, "hub.mncs"), os.path.join(corpus_dir, "hub2.mncs")
    )
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    # Verdicts never guess renames: remove + add.
    assert verdicts["hub.mncs"] == 2
    assert verdicts["hub2.mncs"] == 1
    # ...but content identity proves the move.
    assert _lineage_of(prev, inc, verdicts) == [
        Lineage(
            old="hub.mncs",
            new="hub2.mncs",
            kind=MOVED,
            digest=next(d.digest for d in prev.docs if d.path == "hub.mncs"),
        )
    ]
    # Edges re-key to the new path; nothing points at the old one.
    assert all(r.src != "hub.mncs" for r in inc.rels)
    assert ("hub2.mncs", "defines", "graph.hub") in {
        (r.src, r.rel, r.dst) for r in inc.rels
    }
    _assert_converged(kernels, inc, corpus_dir)


def test_disappearing_target_leaves_dangling_reference(kernels, branch):
    corpus_dir, store_dir = branch
    os.remove(os.path.join(corpus_dir, "hub.mncs"))
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["hub.mncs"] == 2
    # Keeper rows survive untouched: a.mncs still depends on graph.hub.
    assert ("a.mncs", "depends-on", "graph.hub") in {
        (r.src, r.rel, r.dst) for r in inc.rels
    }
    ref = _assert_converged(kernels, inc, corpus_dir)
    eng = QueryEngine(ref, kernels, workers=2)
    assert eng.defines("graph.hub").total == 0
    assert {r.src for r in eng.dependents("graph.hub").records} == {
        "a.mncs",
        "b.mncs",
    }


def test_fanout_hub_change_converges(kernels, branch):
    corpus_dir, store_dir = branch
    write_file(corpus_dir, "c.mncs", _spoke("c"))
    write_file(corpus_dir, "d.mncs", _spoke("d"))
    write_file(corpus_dir, "hub.mncs", HUB_CHANGED)
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["hub.mncs"] == 3
    assert verdicts["c.mncs"] == 1
    assert verdicts["d.mncs"] == 1
    ref = _assert_converged(kernels, inc, corpus_dir)
    eng = QueryEngine(ref, kernels, workers=2)
    assert {r.src for r in eng.dependents("graph.hub").records} == {
        "a.mncs",
        "b.mncs",
        "c.mncs",
        "d.mncs",
    }
    assert eng.defines("hub_extra").total == 1


def test_use_cycle_converges(kernels, branch):
    corpus_dir, store_dir = branch
    write_file(
        corpus_dir,
        "c.mncs",
        b"mncs 0.8;\n\nmodule graph.c;\n\nuse graph.d as d\n",
    )
    write_file(
        corpus_dir,
        "d.mncs",
        b"mncs 0.8;\n\nmodule graph.d;\n\nuse graph.c as c\n",
    )
    inc, _, verdicts = _incremental(kernels, corpus_dir, store_dir, 1)
    assert verdicts["c.mncs"] == 1
    assert verdicts["d.mncs"] == 1
    ref = _assert_converged(kernels, inc, corpus_dir)
    eng = QueryEngine(ref, kernels, workers=2)
    assert [(r.src, r.dst) for r in eng.depends_on("c.mncs").records] == [
        ("c.mncs", "graph.d")
    ]
    assert [(r.src, r.dst) for r in eng.depends_on("d.mncs").records] == [
        ("d.mncs", "graph.c")
    ]
    # A change inside the cycle invalidates exactly the cycle member.
    write_file(
        corpus_dir,
        "c.mncs",
        b"mncs 0.8;\n\nmodule graph.c;\n\nuse graph.d as d\n\nfn c_fn() -> (result: i64) {\n    return 9;\n}\n",
    )
    inc2, _, verdicts2 = _incremental(kernels, corpus_dir, store_dir, 2)
    assert verdicts2["c.mncs"] == 3
    assert verdicts2["d.mncs"] == 0
    _assert_converged(kernels, inc2, corpus_dir)


# -- lineage unit contract (kernel-free, deterministic) --------------------


def test_lineage_exact_move():
    assert resolve([("old.mncs", 7)], [("new.mncs", 7)]) == [
        Lineage(old="old.mncs", new="new.mncs", kind=MOVED, digest=7)
    ]


def test_lineage_no_match_is_remove_plus_add():
    assert resolve([("gone.mncs", 1)], [("fresh.mncs", 2)]) == [
        Lineage(old=None, new="fresh.mncs", kind=ADDED, digest=2),
        Lineage(old="gone.mncs", new=None, kind=REMOVED, digest=1),
    ]


def test_lineage_ambiguous_duplicate_add_is_unlinked():
    got = resolve([("old.mncs", 7)], [("copy1.mncs", 7), ("copy2.mncs", 7)])
    # Canonical order sorts by (old, new): pure additions (old "")
    # come first; every shape here is unlinked remove+add.
    assert got == [
        Lineage(old=None, new="copy1.mncs", kind=ADDED, digest=7),
        Lineage(old=None, new="copy2.mncs", kind=ADDED, digest=7),
        Lineage(old="old.mncs", new=None, kind=REMOVED, digest=7),
    ]


def test_lineage_ambiguous_duplicate_remove_is_unlinked():
    got = resolve([("a.mncs", 7), ("b.mncs", 7)], [("merged.mncs", 7)])
    assert got == [
        Lineage(old=None, new="merged.mncs", kind=ADDED, digest=7),
        Lineage(old="a.mncs", new=None, kind=REMOVED, digest=7),
        Lineage(old="b.mncs", new=None, kind=REMOVED, digest=7),
    ]


def test_lineage_empty_is_empty():
    assert resolve([], []) == []


def test_lineage_order_is_canonical_despite_input_order():
    fwd = resolve(
        [("b.mncs", 2), ("a.mncs", 1)], [("y.mncs", 1), ("z.mncs", 3)]
    )
    rev = resolve(
        [("a.mncs", 1), ("b.mncs", 2)], [("z.mncs", 3), ("y.mncs", 1)]
    )
    assert fwd == rev
    assert fwd == [
        Lineage(old=None, new="z.mncs", kind=ADDED, digest=3),
        Lineage(old="a.mncs", new="y.mncs", kind=MOVED, digest=1),
        Lineage(old="b.mncs", new=None, kind=REMOVED, digest=2),
    ]


def test_lineage_contract_rejects_bad_kinds():
    with pytest.raises(ValueError):
        Lineage(old="a", new="b", kind="renamed", digest=1)
    with pytest.raises(ValueError):
        Lineage(old="a", new=None, kind=MOVED, digest=1)
