"""Host-side unit tests: split rule, canonical bytes, store round-trip."""

from conftest import rebuild_bytes
from mncs_index.model import canonical_bytes
from mncs_index.pipeline import Pipeline
from mncs_index.store import Store


def test_overlong_token_dropped_wholly(kernels):
    classify = kernels.byte_class
    long_word = b"a" * 40
    toks = Pipeline._split_tokens(b"ok " + long_word + b" fine", classify)
    assert toks == [b"fine", b"ok"]
    # Exactly 32 bytes still indexes.
    toks = Pipeline._split_tokens(b"a" * 32, classify)
    assert toks == [b"a" * 32]


def test_split_boundaries(kernels):
    classify = kernels.byte_class
    assert Pipeline._split_tokens(b"", classify) == []
    assert Pipeline._split_tokens(b"   \n\t", classify) == []
    assert Pipeline._split_tokens(b"a_b2 Z9", classify) == [b"Z9", b"a_b2"]
    # Splitting is purely lexical; validity (leading digit) is an MNCS
    # verdict applied later via validate8.
    assert Pipeline._split_tokens(b"9lives _ matter", classify) == [
        b"9lives",
        b"_",
        b"matter",
    ]


def test_generation_excluded_from_canonical_bytes(kernels, workdir):
    _, corpus_dir, _ = workdir
    _, canon0 = rebuild_bytes(kernels, corpus_dir, generation=0, mncs_digest=False)
    _, canon9 = rebuild_bytes(kernels, corpus_dir, generation=9, mncs_digest=False)
    assert canon0 == canon9
    assert b"generation" not in canon0


def test_store_round_trip_reproduces_bytes(kernels, workdir):
    from conftest import full_build

    _, corpus_dir, store_dir = workdir
    snap, canon, _ = full_build(
        kernels, corpus_dir, store_dir, generation=0, workers=4, seed=7
    )
    loaded, _, _ = Store(store_dir).load_head()
    assert loaded.index_hash == snap.index_hash
    assert canonical_bytes(loaded.snapshot_id, loaded.sorted_records()) == canon
