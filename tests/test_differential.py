"""Differential agreement: MNCS kernels vs host oracles on seeded random inputs.

The kernels are the authority under test; the oracles mirror the specified
algorithms. Any mismatch is a correctness finding, not a tolerance issue.
"""

import random

import oracles


def rng(seed: int) -> random.Random:
    return random.Random(seed)


def rand_bytes(r: random.Random, n: int) -> bytes:
    return bytes(r.randrange(256) for _ in range(n))


def test_fold_matches_oracle_random(kernels):
    r = rng(20260906)
    expect = oracles.FNV_BASIS
    got = oracles.FNV_BASIS
    chunks = [rand_bytes(r, r.randrange(0, 65)) for _ in range(6)]
    for chunk in chunks:
        expect = oracles.fold_window(expect, chunk)
        got = kernels.fold_window(got, chunk)
    assert got == expect


def test_tree_digest_matches_oracle(kernels):
    r = rng(777)
    for trial in range(4):
        data = rand_bytes(r, r.randrange(0, 300))
        # Reproduce the pipeline's own reduction using kernel calls.
        if not data:
            got = kernels.empty_digest()
        else:
            leaves = []
            for off in range(0, len(data), 64):
                rec = kernels.leaf_window(
                    oracles.FNV_BASIS, data[off : off + 64], False
                )
                leaves.append(rec["state"])
            level = list(leaves)
            while len(level) > 1:
                nxt = []
                for i in range(0, len(level), 2):
                    if i + 1 < len(level):
                        nxt.append(kernels.combine(level[i], level[i + 1]))
                    else:
                        nxt.append(kernels.combine(level[i], 0))
                level = nxt
            got = level[0]
        assert got == oracles.tree_digest(data)


def pipeline_stats(kernels, data: bytes) -> tuple[int, int]:
    """Mirror Pipeline._assemble exactly: parallel leaves computed with
    open_in=False, then the carried flag threaded in canonical order with
    the one boundary overcount repaired."""
    if not data:
        return 0, 0
    chunks = [data[off : off + 64] for off in range(0, len(data), 64)]
    recs = [kernels.leaf_window(oracles.FNV_BASIS, c, False) for c in chunks]
    words = 0
    for seq, rec in enumerate(recs):
        w = rec["words"]
        if (
            seq > 0
            and recs[seq - 1]["open"]
            and oracles.classify_byte(chunks[seq][0]) not in (1, 2)
        ):
            w -= 1
        words += w
    return words, sum(rec["lines"] for rec in recs)


def test_stats_match_oracle(kernels):
    r = rng(4242)
    for trial in range(6):
        data = rand_bytes(r, r.randrange(0, 200))
        assert pipeline_stats(kernels, data) == oracles.scan_stats(data)


def test_compare_matches_python_order(kernels):
    r = rng(99)
    pairs = [
        (rand_bytes(r, r.randrange(0, 65)), rand_bytes(r, r.randrange(0, 65)))
        for _ in range(16)
    ]
    for a, b in pairs:
        assert kernels.compare_window(a, b) == oracles.compare_bytes(a, b)
        assert kernels.path_less(a, b) == (a < b)


def test_record_less_matches_key_order(kernels):
    r = rng(1001)
    keys = [
        (r.randrange(0, 8), rand_bytes(r, r.randrange(0, 65)), r.randrange(0, 4))
        for _ in range(8)
    ]
    for i in range(len(keys)):
        for j in range(len(keys)):
            ka, pa, sa = keys[i]
            kb, pb, sb = keys[j]
            expect = (ka, pa, sa) < (kb, pb, sb)
            assert kernels.record_less(ka, pa, sa, kb, pb, sb) == expect


def test_contains_matches_substring(kernels):
    r = rng(31337)
    for _ in range(16):
        hay = rand_bytes(r, r.randrange(0, 65))
        if r.random() < 0.5 and hay:
            start = r.randrange(len(hay))
            needle = hay[start : start + r.randrange(0, 9)][:8]
        else:
            needle = rand_bytes(r, r.randrange(0, 9))
        assert kernels.contains8(hay, needle) == oracles.contains(hay, needle)


def test_token_validation_matches_oracle(kernels):
    r = rng(555)
    toks = set()
    while len(toks) < 16:
        toks.add(rand_bytes(r, r.randrange(0, 33)))
    toks = sorted(toks)
    for b in range(0, len(toks), 8):
        batch = toks[b : b + 8]
        mask = kernels.validate8(batch)
        for k, tok in enumerate(batch):
            assert bool(mask & (1 << k)) == oracles.is_symbol_token(tok)
    for tok in toks:
        assert kernels.token_digest(tok) == oracles.token_digest(tok)


def test_query_match_matches_composition(kernels):
    r = rng(808)
    for _ in range(12):
        kind, qkind = r.randrange(0, 8), r.randrange(0, 8)
        digest, qdigest = r.randrange(2**64), r.randrange(2**64)
        hay = rand_bytes(r, r.randrange(0, 65))
        needle = rand_bytes(r, r.randrange(0, 9))
        kind_any, digest_any, term_any = (r.random() < 0.3 for _ in range(3))
        expect = (
            (kind_any or kind == qkind)
            and (digest_any or digest == qdigest)
            and (term_any or oracles.contains(hay, needle))
        )
        assert (
            kernels.query_match(
                kind,
                qkind,
                kind_any,
                digest,
                qdigest,
                digest_any,
                hay,
                needle,
                term_any,
            )
            == expect
        )


def test_classify_change_matches_oracle(kernels):
    r = rng(909)
    for _ in range(10):
        op, od = r.random() < 0.7, r.randrange(2**64)
        np, nd = r.random() < 0.7, r.randrange(2**64)
        assert kernels.classify_change(op, od, np, nd) == oracles.classify_change(
            op, od, np, nd
        )
