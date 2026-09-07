"""Kernel conformance: every MNCS kernel function pinned by execution.

Cases carry explicit expectations (hand-checked constants where the
algorithm is trivial, oracle-derived otherwise). The kernels are the
authority; `oracles.py` mirrors them for randomized cross-checks in
test_differential.py.
"""

import oracles
import pytest


def test_empty_digest_is_basis(kernels):
    assert kernels.empty_digest() == oracles.FNV_BASIS == 14695981039346656037


def test_leaf_window_record_shape(kernels):
    rec = kernels.leaf_window(oracles.FNV_BASIS, b"hi", False)
    assert rec["words"] == 1
    assert rec["lines"] == 0
    assert rec["open"] is True
    assert rec["state"] == oracles.leaf_digest(b"hi")


def test_digest_order_sensitive(kernels):
    a = kernels.leaf_window(oracles.FNV_BASIS, b"ab", False)["state"]
    b = kernels.leaf_window(oracles.FNV_BASIS, b"ba", False)["state"]
    assert a != b
    assert a == oracles.leaf_digest(b"ab")


def test_combine_asymmetric(kernels):
    assert kernels.combine(1, 2) != kernels.combine(2, 1)
    assert kernels.combine(1, 2) == oracles.combine(1, 2)


def test_tree_combine_canonical_shape(kernels):
    leaves = [11, 22, 33, 44, 55]
    once = kernels.tree_combine(leaves, 4)
    assert once == kernels.tree_combine(leaves, 1)
    # Independent oracle computation of the same fixed pairing.
    level = list(leaves)
    while len(level) > 1:
        nxt = []
        for i in range(0, len(level), 2):
            right = level[i + 1] if i + 1 < len(level) else 0
            nxt.append(oracles.combine(level[i], right))
        level = nxt
    assert once == level[0]
    assert kernels.tree_combine([], 4) == oracles.FNV_BASIS


def test_fold_window_threads(kernels):
    data = b"the quick brown fox jumps over the lazy dog, twice!" * 2
    state = oracles.FNV_BASIS
    for off in range(0, len(data), 64):
        state = kernels.fold_window(state, data[off : off + 64])
    assert state == oracles.fold_window(oracles.FNV_BASIS, data)


def test_classify_byte_table(kernels):
    assert kernels.byte_class(10) == 1
    for v in (32, 9, 13):
        assert kernels.byte_class(v) == 2
    for v in list(range(48, 58)) + list(range(65, 91)) + list(range(97, 123)) + [95]:
        assert kernels.byte_class(v) == 3
    for v in (0, 1, 33, 47, 58, 127, 200):
        assert kernels.byte_class(v) == 0
    table = kernels.byte_class_table()
    assert len(table) == 256
    assert [table[v] for v in range(256)] == [
        oracles.classify_byte(v) for v in range(256)
    ]


@pytest.mark.parametrize(
    "tok,expected",
    [
        (b"abc", True),
        (b"_x1", True),
        (b"a", True),
        (b"Z9_", True),
        (b"9ab", False),
        (b"", False),
        (b"a b", False),
        (b"a-b", False),
        (b"a" * 32, True),
        (b"a" * 33, False),
    ],
)
def test_is_symbol_token(kernels, tok, expected):
    if len(tok) > 32:
        with pytest.raises(ValueError):
            kernels.is_symbol_token(tok)
    else:
        assert kernels.is_symbol_token(tok) == expected


def test_token_digest_matches_oracle(kernels):
    for tok in (b"fn", b"alpha_value", b"a", b"_", b"index"):
        assert kernels.token_digest(tok) == oracles.token_digest(tok)


def test_validate8_bitmask(kernels):
    toks = [b"ok", b"9bad", b"_x1", b"", b"fine"]
    mask = kernels.validate8(toks)
    assert mask == 0b10101
    assert kernels.validate8([]) == 0


@pytest.mark.parametrize(
    "ext,expected",
    [
        (b"mncs", 1),
        (b"md", 2),
        (b"json", 3),
        (b"toml", 4),
        (b"yaml", 5),
        (b"yml", 5),
        (b"txt", 6),
        (b"text", 6),
        (b"log", 7),
        (b"rs", 0),
        (b"", 0),
        (b"MD", 0),
        (b"jsonx", 0),
    ],
)
def test_classify_kind(kernels, ext, expected):
    assert kernels.classify_kind(ext) == expected == oracles.classify_kind(ext)


@pytest.mark.parametrize(
    "a,b,expected",
    [
        (b"abc", b"abd", -1),
        (b"abd", b"abc", 1),
        (b"ab", b"ab", 0),
        (b"ab", b"abc", -1),
        (b"abc", b"ab", 1),
        (b"", b"", 0),
        (b"", b"x", -1),
        (b"x", b"", 1),
        (b"a", b"B", 1),
    ],
)
def test_compare_window(kernels, a, b, expected):
    got = kernels.compare_window(a, b)
    assert got == expected
    assert (a > b) - (a < b) == expected


def test_record_less_orders_canonical_key(kernels):
    # kind rank dominates path
    assert kernels.record_less(1, b"zzz", 0, 2, b"aaa", 0) is True
    assert kernels.record_less(2, b"aaa", 0, 1, b"zzz", 0) is False
    # path breaks kind ties; seq breaks full ties
    assert kernels.record_less(1, b"a", 0, 1, b"b", 0) is True
    assert kernels.record_less(1, b"b", 0, 1, b"a", 0) is False
    assert kernels.record_less(1, b"a", 0, 1, b"a", 1) is True
    assert kernels.record_less(1, b"a", 1, 1, b"a", 1) is False


@pytest.mark.parametrize(
    "hay,needle,expected",
    [
        (b"xxfnyy", b"fn", True),
        (b"fn", b"fn", True),
        (b"abc", b"abd", False),
        (b"ab", b"abc", False),
        (b"anything", b"", True),
        (b"", b"a", False),
        (b"hello", b"ell", True),
        (b"hello", b"lo!", False),
    ],
)
def test_contains8(kernels, hay, needle, expected):
    assert kernels.contains8(hay, needle) == expected


def test_query_match_clauses(kernels):
    base = {
        "kind": 2,
        "qkind": 2,
        "kind_any": False,
        "digest": 99,
        "qdigest": 99,
        "digest_any": False,
        "hay": b"hello world",
        "needle": b"wor",
        "term_any": False,
    }
    assert kernels.query_match(**base) is True
    assert kernels.query_match(**{**base, "qkind": 3}) is False
    assert kernels.query_match(**{**base, "qkind": 3, "kind_any": True}) is True
    assert kernels.query_match(**{**base, "qdigest": 100}) is False
    assert kernels.query_match(**{**base, "qdigest": 100, "digest_any": True}) is True
    assert kernels.query_match(**{**base, "needle": b"zzz"}) is False
    assert kernels.query_match(**{**base, "needle": b"zzz", "term_any": True}) is True


@pytest.mark.parametrize(
    "old_p,old_d,new_p,new_d,expected",
    [
        (True, 5, True, 5, 0),
        (False, 0, True, 9, 1),
        (True, 5, False, 0, 2),
        (True, 5, True, 6, 3),
        (False, 0, False, 0, 0),
    ],
)
def test_classify_change(kernels, old_p, old_d, new_p, new_d, expected):
    assert kernels.classify_change(old_p, old_d, new_p, new_d) == expected
