"""Query determinism and query-class coverage against one snapshot."""

import pytest
from conftest import full_build
from mncs_index.model import hex16
from mncs_index.query import QueryEngine


@pytest.fixture(scope="module")
def snapshot(kernels, tmp_path_factory):
    import shutil

    from conftest import FIXTURES

    tmp = tmp_path_factory.mktemp("q")
    corpus = tmp / "corpus"
    shutil.copytree(FIXTURES, corpus)
    store = tmp / "store"
    store.mkdir()
    snap, _, _ = full_build(kernels, str(corpus), str(store), workers=4, seed=7)
    return snap


@pytest.fixture(scope="module")
def engine(snapshot, kernels):
    return QueryEngine(snapshot, kernels, workers=4)


def test_path_lookup(engine, snapshot):
    res = engine.by_path("a.mncs")
    assert res.total == 1
    assert res.records[0].path == "a.mncs"
    assert res.snapshot_id == snapshot.snapshot_id
    assert engine.by_path("missing.mncs").total == 0


def test_kind_filter(engine):
    md = engine.by_kind(2)
    assert {d.path for d in md.records} == {"b.md"}
    mncs = engine.by_kind(1)
    assert {d.path for d in mncs.records} == {"a.mncs", "sub/g.mncs"}


def test_digest_lookup(engine, snapshot):
    target = next(d for d in snapshot.docs if d.path == "c.json")
    res = engine.by_digest_query(target.digest)
    assert res.total == 1 and res.records[0].path == "c.json"
    assert engine.by_id(hex16(target.digest)).total == 1


def test_term_substring(engine):
    res = engine.term_substring("alpha")
    assert [d.path for d in res.records] == ["a.mncs"]
    res = engine.term_substring(" fixture", limit=None)
    assert res.total == 0  # leading space is not inside any symbol token
    res = engine.term_substring("index")
    assert "c.json" in [d.path for d in res.records]


def test_long_term_uses_host_path(engine):
    res = engine.term_substring("alpha_value")
    assert [d.path for d in res.records] == ["a.mncs"]


def test_results_canonically_ordered(engine):
    res = engine.term_substring("e")
    keys = [d.sort_key() for d in res.records]
    assert keys == sorted(keys)


def test_limit_contract(engine):
    res = engine.term_substring("e", limit=1)
    assert len(res.records) == 1 and res.limited is True and res.total >= 1
    unbounded = engine.term_substring("e")
    assert unbounded.limited is False
    assert res.records[0].path == unbounded.records[0].path


def test_query_repeatable(engine):
    first = engine.term_substring("test")
    for _ in range(3):
        again = engine.term_substring("test")
        assert [d.path for d in again.records] == [d.path for d in first.records]
        assert again.total == first.total
