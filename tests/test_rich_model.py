"""Canonical-v2 rich normalized model: kernels, extraction, merge, queries.

Slice 5 proves the richer model (source declarations, headings,
relationships, pressure mentions) is additive over canonical-v1: v1 bytes
are untouched, every new semantic is an MNCS kernel verdict, the merge
dedups deterministically, worker counts converge, incremental == rebuild,
and the extended query classes are snapshot-consistent.
"""

import shutil

import oracles
import pytest
from conftest import FIXTURES, full_build_v2, write_file
from mncs_index.extract import build_rich, extract_file, globalize
from mncs_index.indexer import (
    build_snapshot,
    build_snapshot_v2,
    finalize,
    incremental_snapshot_v2,
)
from mncs_index.model import (
    HeadingRecord,
    PressRecord,
    RelRecord,
    SymRecord,
    canonical_bytes,
    snapshot_from_json,
    snapshot_to_json,
)
from mncs_index.pipeline import BuildConfig
from mncs_index.query import QueryEngine
from mncs_index.store import Store


def _v1_section(data: bytes) -> list[bytes]:
    lines = data.split(b"\n")
    assert lines[0].startswith(b"mncs-index canonical-v")
    assert lines[1].startswith(b"snapshot ")
    assert lines[-2] == b"end" and lines[-1] == b""
    return lines[2:-2]


# -- extract kernel conformance ------------------------------------------


@pytest.mark.parametrize(
    "line,expected",
    [
        (b"module fixture.alpha;", 1),
        (b"module m;", 1),
        (b"module  spaced;", 1),
        (b"fn\ttabbed", 2),
        (b"fn alpha_value(seed: i64) -> (result: i64) {", 2),
        (b"record WinDigest { state: u64 }", 3),
        (b"use mncs.std.task.v1 as task", 4),
        (b"return seed + 1;", 0),
        (b"", 0),
        (b"modules x", 0),
        (b"fns x", 0),
        (b"fnx", 0),
        (b"fn", 0),
        (b"use", 0),
        (b"# comment", 0),
        (b"Module upper;", 0),
    ],
)
def test_classify_decl(kernels, line, expected):
    assert kernels.classify_decl(line) == expected == oracles.classify_decl(line)


@pytest.mark.parametrize(
    "line,expected",
    [
        (b"# Fixture Beta", 1),
        (b"## Sub", 2),
        (b"###### six", 6),
        (b"####### seven is text", 0),
        (b"#nospace", 0),
        (b"#", 0),
        (b"##", 0),
        (b"hello", 0),
        (b"", 0),
        (b"  # indented", 0),  # host trims; raw indent is not a heading
    ],
)
def test_heading_level(kernels, line, expected):
    assert kernels.heading_level(line) == expected == oracles.heading_level(line)


@pytest.mark.parametrize(
    "line,expected",
    [
        (b"see [RFC 0003](rfcs/0003-x.md) now", True),
        (b"[a](b)", True),
        (b"x ] ( y", False),
        (b"no links (just parens)", False),
        (b"", False),
        (b"]( at start", True),
        (b"ends with ](", True),
    ],
)
def test_contains_link(kernels, line, expected):
    assert kernels.contains_link(line) is expected
    assert oracles.contains_link(line) is expected


@pytest.mark.parametrize(
    "tok,expected",
    [
        (b"PRESS-001", True),
        (b"PRESS-012", True),
        (b"PRESS-01", False),
        (b"PRESS-0001", False),
        (b"press-001", False),
        (b"PRESS-00a", False),
        (b"PRESS-", False),
        (b"", False),
        (b"XPRESS-001", False),
    ],
)
def test_is_press_id(kernels, tok, expected):
    assert kernels.is_press_id(tok) is expected
    assert oracles.is_press_id(tok) is expected


@pytest.mark.parametrize(
    "tok,expected",
    [
        (b"RFC", 1),
        (b"rfc", 1),
        (b"Rfc", 1),
        (b"0003", 2),
        (b"0000", 2),
        (b"rfcs", 3),
        (b"RFCS", 3),
        (b"index", 0),
        (b"RFC0", 0),
        (b"000", 0),
        (b"00003", 0),
        (b"", 0),
    ],
)
def test_classify_rfc_token(kernels, tok, expected):
    assert kernels.classify_rfc_token(tok) == expected
    assert oracles.classify_rfc_token(tok) == expected


def test_extract_kernel_bounds(kernels):
    import pytest as _pytest

    with _pytest.raises(ValueError):
        kernels.classify_decl(b"x" * 65)
    with _pytest.raises(ValueError):
        kernels.heading_level(b"x" * 65)
    with _pytest.raises(ValueError):
        kernels.contains_link(b"x" * 65)
    with _pytest.raises(ValueError):
        kernels.is_press_id(b"x" * 33)
    with _pytest.raises(ValueError):
        kernels.classify_rfc_token(b"x" * 33)


def test_extract_differential_random(kernels):
    import random

    r = random.Random(20260907)
    alphabet = b"abcdEFRSP #%[]().-_\t0123456789"
    keywords = [b"module ", b"fn ", b"record ", b"use ", b"# ", b"## ", b"RFC ", b"PRESS-"]
    for _ in range(24):
        n = r.randrange(0, 65)
        line = bytes(r.choice(alphabet) for _ in range(n))
        if r.random() < 0.4 and keywords:
            kw = r.choice(keywords)
            line = (kw + line)[:64]
        assert kernels.classify_decl(line) == oracles.classify_decl(line)
        assert kernels.heading_level(line) == oracles.heading_level(line)
        assert kernels.contains_link(line) == oracles.contains_link(line)
    for _ in range(16):
        n = r.randrange(0, 13)
        tok = bytes(r.choice(alphabet) for _ in range(n))
        if r.random() < 0.4:
            tok = r.choice([b"PRESS-001", b"RFC", b"rfcs", b"0007", b"nope"])
        assert kernels.is_press_id(tok) == oracles.is_press_id(tok)
        assert kernels.classify_rfc_token(tok) == oracles.classify_rfc_token(tok)


def test_record_contracts():
    import pytest as _pytest

    with _pytest.raises(ValueError):
        SymRecord(path="a", sym="bogus", name="x", ndigest=1)
    with _pytest.raises(ValueError):
        HeadingRecord(path="a", level=0, seq=0, tdigest=1, title="t")
    with _pytest.raises(ValueError):
        HeadingRecord(path="a", level=7, seq=0, tdigest=1, title="t")
    with _pytest.raises(ValueError):
        RelRecord(src="a", rel="bogus", dst="b")


# -- fixtures ------------------------------------------------------------


@pytest.fixture(scope="module")
def rich_fixture(kernels, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("rich")
    corpus = tmp / "corpus"
    shutil.copytree(FIXTURES, corpus)
    store = tmp / "store"
    store.mkdir()
    cfg = BuildConfig(workers=4, seed=7, mncs_digest=False)
    snap, canon, corpus_snap, _ = build_snapshot_v2(str(corpus), 0, kernels, cfg)
    snap_v1, canon_v1, _, _ = build_snapshot(str(corpus), 0, kernels, cfg)
    return snap, canon, snap_v1, canon_v1


def test_fixture_source_records(rich_fixture):
    snap, _, _, _ = rich_fixture
    got = {(r.path, r.sym, r.name) for r in snap.syms}
    assert ("a.mncs", "module", "fixture.alpha") in got
    assert ("a.mncs", "fn", "alpha_value") in got
    assert ("sub/g.mncs", "module", "fixture.deep") in got
    assert ("sub/g.mncs", "fn", "deep_helper") in got
    # Name digests are MNCS folds, stable and content-addressed.
    by_name = {r.name: r for r in snap.syms}
    assert by_name["alpha_value"].ndigest == oracles.fold_window(
        oracles.FNV_BASIS, b"alpha_value"
    )


def test_fixture_headings_and_edges(rich_fixture):
    snap, _, _, _ = rich_fixture
    assert [(r.path, r.level, r.title) for r in snap.headings] == [
        ("b.md", 1, "Fixture Beta")
    ]
    defines = {(r.src, r.dst) for r in snap.rels if r.rel == "defines"}
    assert ("a.mncs", "fixture.alpha") in defines
    assert ("a.mncs", "alpha_value") in defines
    assert ("sub/g.mncs", "deep_helper") in defines
    # Fixtures carry no use-decls, links, PRESS or RFC mentions.
    assert {r.rel for r in snap.rels} == {"defines"}
    assert snap.press == []


def test_v1_section_byte_identical_inside_v2(rich_fixture):
    snap, canon_v2, snap_v1, canon_v1 = rich_fixture
    assert canon_v2.startswith(b"mncs-index canonical-v2\n")
    assert canon_v1.startswith(b"mncs-index canonical-v1\n")
    v1_lines = _v1_section(canon_v1)
    v2_lines = _v1_section(canon_v2)
    assert v2_lines[: len(v1_lines)] == v1_lines
    assert len(v2_lines) > len(v1_lines)  # rich section appended
    # Same logical corpus: identical docs and terms.
    assert [d.canon_line() for d in snap.docs] == [
        d.canon_line() for d in snap_v1.docs
    ]
    assert [t.canon_line() for t in snap.terms] == [
        t.canon_line() for t in snap_v1.terms
    ]
    assert canonical_bytes(snap_v1.snapshot_id, snap_v1.sorted_records()) == canon_v1


def test_v1_snapshot_untouched_by_v2_code(rich_fixture):
    _, _, snap_v1, _ = rich_fixture
    assert not snap_v1.has_rich
    assert snap_v1.syms == snap_v1.headings == snap_v1.rels == snap_v1.press == []


# -- synthetic corpus: links, use, PRESS, RFC, dedup ----------------------


def _synthetic_corpus(root: str) -> None:
    write_file(
        root,
        "a.mncs",
        b"mncs 0.8;\n"
        b"\n"
        b"module demo.app;\n"
        b"\n"
        b"use mncs.std.task.v1 as task\n"
        b"\n"
        b"fn shared() -> (result: i64) {\n"
        b"    return 1;\n"
        b"}\n"
        b"\n"
        b"fn shared() -> (result: i64) {\n"
        b"    return 1;\n"
        b"}\n"
        b"\n"
        b"fn 9bad() -> (result: i64) {\n"
        b"    return 2;\n"
        b"}\n"
        b"\n"
        b"// PRESS-001 hosts the queue; see RFC 0002.\n",
    )
    write_file(
        root,
        "dup.mncs",
        b"mncs 0.8;\n\nmodule demo.other;\n\nfn shared() -> (result: i64) {\n    return 3;\n}\n",
    )
    write_file(
        root,
        "b.md",
        b"# Demo\n"
        b"\n"
        b"See [the spec](rfcs/0002-model.md) and [again](rfcs/0002-model.md).\n"
        b"\n"
        b"## Details\n"
        b"\n"
        b"PRESS-002 and PRESS-002 once more; also RFC 0003.\n",
    )
    write_file(root, "c.json", b'{"note": "PRESS-004 meets rfc 0005"}\n')


@pytest.fixture(scope="module")
def rich_synth(kernels, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("synth")
    corpus = tmp / "corpus"
    corpus.mkdir()
    _synthetic_corpus(str(corpus))
    store = tmp / "store"
    store.mkdir()
    cfg = BuildConfig(workers=4, seed=7, mncs_digest=False)
    snap, canon, corpus_snap, _ = build_snapshot_v2(str(corpus), 0, kernels, cfg)
    established = {d.path: 0 for d in snap.docs}
    Store(str(store)).publish(
        snap, canon, established, {it.path: it.crc for it in corpus_snap.items}, None
    )
    loaded, est, _ = Store(str(store)).load_head()
    return snap, canon, str(corpus), str(store), loaded, est


def test_synth_decls_and_invalid_names(rich_synth):
    snap, _, _, _, _, _ = rich_synth
    got = {(r.path, r.sym, r.name) for r in snap.syms}
    assert ("a.mncs", "module", "demo.app") in got
    assert ("a.mncs", "use", "mncs.std.task.v1") in got
    assert ("a.mncs", "fn", "shared") in got
    assert ("dup.mncs", "fn", "shared") in got
    assert ("dup.mncs", "module", "demo.other") in got
    # Leading-digit name is rejected by the MNCS validity verdict.
    assert not [r for r in snap.syms if r.name == "9bad"]
    # Exact-duplicate declaration dedups to one record (stable identity).
    assert len([r for r in snap.syms if (r.path, r.name) == ("a.mncs", "shared")]) == 1


def test_synth_relationships(rich_synth):
    snap, _, _, _, _, _ = rich_synth
    edges = {(r.src, r.rel, r.dst) for r in snap.rels}
    assert ("a.mncs", "defines", "demo.app") in edges
    assert ("a.mncs", "defines", "shared") in edges
    assert ("a.mncs", "references", "mncs.std.task.v1") in edges
    assert ("a.mncs", "depends-on", "mncs.std.task.v1") in edges
    # Duplicate link target collapses to one edge.
    assert ("b.md", "references", "rfcs/0002-model.md") in edges
    assert len([e for e in edges if e[2] == "rfcs/0002-model.md"]) == 1
    assert ("b.md", "rfc-ref", "0003") in edges
    assert ("a.mncs", "rfc-ref", "0002") in edges
    assert ("c.json", "rfc-ref", "0005") in edges


def test_synth_pressure_records(rich_synth):
    snap, _, _, _, _, _ = rich_synth
    got = {(r.path, r.pid) for r in snap.press}
    assert ("a.mncs", "PRESS-001") in got
    assert ("b.md", "PRESS-002") in got
    assert ("c.json", "PRESS-004") in got
    # Repeated mention in one file collapses to one record.
    assert len([r for r in snap.press if r.pid == "PRESS-002"]) == 1


def test_synth_headings_ordered(rich_synth):
    snap, _, _, _, _, _ = rich_synth
    got = [(r.path, r.level, r.title) for r in snap.headings]
    assert got == [("b.md", 1, "Demo"), ("b.md", 2, "Details")]


def test_overlong_and_foreign_bytes_skipped(kernels):
    long_name = b"fn " + b"a" * 70 + b"() {}"
    fr = extract_file("x.mncs", 1, long_name + b"\n", kernels)
    assert fr.syms == []
    fr = extract_file("x.mncs", 1, b"fn 9lives() {}\n", kernels)
    assert fr.syms == []
    fr = extract_file("x.md", 2, b"# \xff\xfe\n", kernels)
    assert fr.headings == []
    fr = extract_file("x.md", 2, b"see [t](\xff)\n", kernels)
    assert fr.rels == []
    # A `](` seam past the first 64 B window is still a kernel verdict.
    line = b"x" * 63 + b"](y)"
    fr = extract_file("x.md", 2, line + b"\n", kernels)
    assert ("references", "y") in set(fr.rels)


def test_merge_dedup_is_canonical(kernels):
    fr = extract_file("m.mncs", 1, b"module m;\nfn f() {}\nfn f() {}\n", kernels)
    tables = globalize({"m.mncs": fr})
    assert [r.seq for r in tables.syms] == [0, 1]
    assert [r.seq for r in tables.rels] == [0, 1]
    assert tables.syms == sorted(tables.syms, key=lambda r: r.sort_key())
    assert tables.rels == sorted(tables.rels, key=lambda r: r.sort_key())


# -- worker-count graph convergence ---------------------------------------


def test_worker_counts_converge_rich(kernels, tmp_path):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    _synthetic_corpus(str(corpus))
    seen = set()
    for workers in (1, 2, 4, 8):
        cfg = BuildConfig(workers=workers, seed=7, mncs_digest=True)
        snap, canon, _, _ = build_snapshot_v2(str(corpus), 0, kernels, cfg)
        lines = _v1_section(canon)
        seen.add((snap.index_hash, snap.mncs_fingerprint, tuple(lines)))
    assert len(seen) == 1


def test_seed_and_queue_converge_rich(kernels, tmp_path):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    _synthetic_corpus(str(corpus))
    ref, _, _, _ = build_snapshot_v2(
        str(corpus), 0, kernels, BuildConfig(workers=4, seed=7, mncs_digest=False)
    )
    for cfg in (
        BuildConfig(workers=4, seed=99, mncs_digest=False),
        BuildConfig(workers=2, seed=7, queue_size=1, mncs_digest=False),
    ):
        snap, _, _, _ = build_snapshot_v2(str(corpus), 0, kernels, cfg)
        assert snap.index_hash == ref.index_hash
        assert [r.canon_line() for _, r in snap.sorted_all_records()] == [
            r.canon_line() for _, r in ref.sorted_all_records()
        ]


# -- incremental == rebuild (v2) ------------------------------------------


def _publish_v2(kernels, corpus_dir, store_dir, generation, workers=4, **cfg_kw):
    from mncs_index.indexer import build_snapshot_v2 as _build

    cfg = BuildConfig(workers=workers, **cfg_kw)
    store = Store(store_dir)
    base = store.head()
    snap, canon, corpus, _ = _build(corpus_dir, generation, kernels, cfg)
    established = {}
    if base is not None:
        _, established, _ = store.load_head()
    for d in snap.docs:
        established.setdefault(d.path, generation)
    store.publish(
        snap, canon, established, {it.path: it.crc for it in corpus.items}, base
    )
    return snap, canon


def _incremental_v2(kernels, corpus_dir, store_dir, generation):
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


def test_incremental_equals_rebuild_v2(kernels, tmp_path):
    import shutil

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    _synthetic_corpus(str(corpus))
    store = tmp_path / "store"
    store.mkdir()
    _publish_v2(kernels, str(corpus), str(store), 0, mncs_digest=False)
    # Modify (new decl + new PRESS), add, delete.
    write_file(str(corpus), "a.mncs", b"mncs 0.8;\n\nmodule demo.app;\n\nfn brand_new() -> (result: i64) {\n    return 9;\n}\n\n// PRESS-007 arrives.\n")
    write_file(str(corpus), "new.md", b"# Fresh\n\nLink [up](a.mncs).\n")
    (corpus / "c.json").unlink()
    inc, _, verdicts = _incremental_v2(kernels, str(corpus), str(store), 1)
    rebuild_dir = tmp_path / "rebuild"
    shutil.copytree(corpus, rebuild_dir)
    cfg = BuildConfig(workers=4, seed=7, mncs_digest=False)
    re_snap, re_canon, _, _ = build_snapshot_v2(str(rebuild_dir), 1, kernels, cfg)
    assert inc.index_hash == re_snap.index_hash
    assert [r.canon_line() for _, r in inc.sorted_all_records()] == [
        r.canon_line() for _, r in re_snap.sorted_all_records()
    ]
    got_rels = {(r.src, r.rel, r.dst) for r in inc.rels}
    assert ("a.mncs", "defines", "brand_new") in got_rels
    assert ("a.mncs", "defines", "shared") not in got_rels
    assert ("new.md", "references", "a.mncs") in got_rels
    assert ("c.json", "PRESS-004") not in {(r.path, r.pid) for r in inc.press}
    assert ("a.mncs", "PRESS-007") in {(r.path, r.pid) for r in inc.press}


# -- store round-trip + migration ------------------------------------------


def test_store_round_trip_v2(rich_synth):
    _, _, _, _, loaded, _ = rich_synth
    snap, _, _, _, _, _ = rich_synth
    assert loaded.index_hash == snap.index_hash
    assert [r.canon_line() for _, r in loaded.sorted_all_records()] == [
        r.canon_line() for _, r in snap.sorted_all_records()
    ]


def test_v1_envelope_migrates_to_v2_empty(kernels, tmp_path):
    docs = []
    snap_v1, canon_v1 = finalize(None, "mig", 0, docs, [], with_mncs_digest=False)
    payload = snapshot_to_json(snap_v1)
    assert payload["format"] == "mncs-index/snapshot-v1"
    assert "syms" not in payload
    loaded = snapshot_from_json(payload)
    assert not loaded.has_rich
    assert loaded.syms == loaded.headings == loaded.rels == loaded.press == []
    assert loaded.index_hash == snap_v1.index_hash


# -- extended queries ------------------------------------------------------


def test_rich_queries(rich_synth, kernels):
    snap, _, _, _, _, est = rich_synth
    engine = QueryEngine(snap, kernels, workers=2, established=est)
    definers = engine.defines("shared")
    assert {(r.src, r.rel) for r in definers.records} == {
        ("a.mncs", "defines"),
        ("dup.mncs", "defines"),
    }
    assert definers.snapshot_id == snap.snapshot_id
    refs = engine.references("mncs.std.task.v1")
    assert {(r.src, r.rel) for r in refs.records} == {("a.mncs", "references")}
    deps = engine.depends_on("a.mncs")
    assert [(r.rel, r.dst) for r in deps.records] == [
        ("depends-on", "mncs.std.task.v1")
    ]
    assert engine.dependents("mncs.std.task.v1").total == 1
    assert engine.dependents("nothing").total == 0
    assert {(r.src, r.dst) for r in engine.rfc_refs("0003").records} == {
        ("b.md", "0003")
    }
    assert engine.rfc_refs("9999").total == 0
    assert {(r.path, r.pid) for r in engine.pressure("PRESS-002").records} == {
        ("b.md", "PRESS-002")
    }
    syms = engine.symbols("shared")
    assert {r.path for r in syms.records} == {"a.mncs", "dup.mncs"}
    heads = engine.headings_for("b.md")
    assert [(r.level, r.title) for r in heads.records] == [
        (1, "Demo"),
        (2, "Details"),
    ]
    assert engine.headings_for("a.mncs").total == 0
    # Canonical order + limit contract on every rich class.
    for res in (definers, refs, deps, syms, heads):
        keys = [r.sort_key() for r in res.records]
        assert keys == sorted(keys)
    limited = engine.defines("shared", limit=1)
    assert limited.total == 2 and limited.limited is True
    assert len(limited.records) == 1
    assert engine.defines("missing-symbol").total == 0


def test_provenance_query(rich_synth, kernels):
    snap, _, _, _, _, est = rich_synth
    engine = QueryEngine(snap, kernels, workers=2, established=est)
    prov = engine.provenance("a.mncs")
    assert prov["found"] is True
    assert prov["snapshot_id"] == snap.snapshot_id
    assert prov["generation"] == snap.generation
    assert prov["established_generation"] == 0
    assert "digest" in prov and "size" in prov
    missing = engine.provenance("nope.mncs")
    assert missing["found"] is False
    assert missing["snapshot_id"] == snap.snapshot_id


def test_rich_build_rich_tables_directly(kernels):
    tables = build_rich(
        [("m.mncs", 1, b"module m;\n"), ("d.md", 2, b"# T\n")], kernels, workers=2
    )
    assert [(r.sym, r.name) for r in tables.syms] == [("module", "m")]
    assert [(r.level, r.title) for r in tables.headings] == [(1, "T")]
    assert tables.rels[0].rel == "defines"


def _rich_key(tables) -> tuple:
    return (
        [r.canon_line() for r in tables.syms],
        [r.canon_line() for r in tables.headings],
        [r.canon_line() for r in tables.rels],
        [r.canon_line() for r in tables.press],
    )


def test_globalize_permuted_rows_converge():
    """`globalize` output depends on the row multiset, never arrival order.

    Identical per-file facts fed in permuted within-file row order must
    produce identical canonical tables (regression pin for the heading
    stable-sort leak: duplicate (path, lineno) pairs must not leak order
    and identical rows must not double-emit).
    """
    import random

    from mncs_index.extract import FileRich

    syms = [(3, "fn", "alpha", 11), (1, "module", "mod", 22), (2, "fn", "beta", 33)]
    heads = [(2, 1, "T2", 9), (1, 1, "T1", 7), (1, 1, "T1", 7)]
    rels = [("defines", "b"), ("references", "a"), ("defines", "a")]
    press = ["PRESS-007", "PRESS-004", "PRESS-007"]

    def build(order) -> tuple:
        fr = FileRich(path="x.mncs")
        fr.syms = [syms[i] for i in order]
        fr.headings = [heads[i % len(heads)] for i in order]
        fr.rels = [rels[i % len(rels)] for i in order]
        fr.press = [press[i % len(press)] for i in order]
        other = FileRich(path="a.md")
        other.headings = [(1, 1, "Doc", 5)]
        return _rich_key(globalize({"x.mncs": fr, "a.md": other}))

    reference = build([0, 1, 2])
    rng = random.Random(1234)
    for _ in range(8):
        order = [0, 1, 2]
        rng.shuffle(order)
        assert build(order) == reference
    # The duplicate heading row collapsed: exactly 3 distinct headings.
    assert len(reference[1]) == 3


def test_noop_incremental_v2_reuses(kernels, tmp_path):
    """Unchanged corpus: v2 incremental reuses every row, bytes converge."""
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    _synthetic_corpus(str(corpus_dir))
    corpus = str(corpus_dir)
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    store = str(store_dir)
    snap0, _canon0 = _publish_v2(kernels, corpus, store, 0, mncs_digest=False)
    inc, _canon1, verdicts = _incremental_v2(kernels, corpus, store, 1)
    assert verdicts and all(v == 0 for v in verdicts.values())
    assert inc.index_hash == snap0.index_hash
    assert [r.canon_line() for _, r in inc.sorted_all_records()] == [
        r.canon_line() for _, r in snap0.sorted_all_records()
    ]


def test_rich_build_without_rows_publishes_v1(kernels, tmp_path):
    """`--rich` over a row-less corpus degrades to v1 bytes (RFC 0007).

    Regression pin: `finalize_v2` must not emit a v2 marker the store
    classifies as v1 — the publication must succeed and hash exactly
    like a plain v1 build of the same corpus.
    """
    from conftest import rebuild_bytes

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    write_file(str(corpus), "notes.txt", b"just plain text, no structure\n")
    store = tmp_path / "store"
    store.mkdir()
    snap, canon, _corpus = full_build_v2(
        kernels, str(corpus), str(store), generation=0,
        workers=2, seed=7, mncs_digest=False,
    )
    assert canon.startswith(b"mncs-index canonical-v1\n")
    assert not snap.has_rich
    ref, _ref_canon = rebuild_bytes(
        kernels, str(corpus), workers=2, seed=7, mncs_digest=False
    )
    assert snap.index_hash == ref.index_hash


def test_dotfile_kind_agreement(kernels, tmp_path):
    """Admission and planning agree on dotfiles: unknown, never split."""
    from mncs_index.discover import _extension, enumerate_corpus
    from mncs_index.extract import path_extension

    assert _extension(".hidden.mncs") == ""
    assert _extension(".hidden") == ""
    assert path_extension(".hidden.mncs") == b""
    assert kernels.classify_kind(path_extension(".hidden.mncs")) == 0
    root = tmp_path / "corpus"
    root.mkdir()
    write_file(str(root), ".hidden.mncs", b"module sneaky;\n")
    write_file(str(root), "plain.mncs", b"module plain;\n")
    admitted, _skipped = enumerate_corpus(str(root))
    rels = [rel for rel, _full in admitted]
    # Admitted ("" takes the unknown rank), never skipped, never mncs-kind.
    assert ".hidden.mncs" in rels
    assert "plain.mncs" in rels
