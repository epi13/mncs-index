"""Store-backed derived projection obligations.

The fixture uses the versioned Store records directly. It proves that the
Index result is disposable, generation-aware, and not a competing canonical
persistence implementation.
"""

from mncs_index.store_feed import (
    DerivedStoreIndex,
    StoreCommit,
    StoreFeed,
    StoreRelation,
)


def _relation(kind, source, target, generation, ordinal):
    raw = bytearray(80)
    raw[:8] = b"MR\x01\x00" + bytes([kind, 0, 0, 0])
    raw[8:20] = source
    raw[20:32] = target
    raw[32:40] = generation.to_bytes(8, "big")
    raw[40:72] = bytes([ordinal]) * 32
    raw[72:80] = ordinal.to_bytes(8, "big")
    return StoreRelation.decode(bytes(raw))


def _commit(generation, relations):
    feed_raw = bytearray(56)
    feed_raw[:4] = b"MC\x01\x00"
    feed_raw[4:12] = generation.to_bytes(8, "big")
    feed_raw[16:20] = len(relations).to_bytes(4, "big")
    feed_raw[24:56] = bytes([generation]) * 32
    return StoreCommit(
        feed=StoreFeed.decode(bytes(feed_raw)),
        relations=tuple(relations),
    )


def test_destroy_and_rebuild_preserves_typed_query_result():
    pressure = bytes.fromhex("000000000000000000000001")
    store = bytes.fromhex("000000000000000000000002")
    relation = _relation(2, pressure, store, 7, 0)
    commit = _commit(7, [relation])

    index = DerivedStoreIndex.rebuild([commit])
    before = index.query_relations(target=store, kind=2)
    assert before.complete is True
    assert before.index_through_generation == 7
    assert len(before.relations) == 1

    index.destroy()
    rebuilt = DerivedStoreIndex.rebuild([commit])
    after = rebuilt.query_relations(target=store, kind=2)
    assert after.as_projection() == before.as_projection()

    empty_next = _commit(8, [])
    rebuilt_again = DerivedStoreIndex.rebuild([commit, empty_next])
    advanced = rebuilt_again.query_relations(target=store, kind=2)
    assert advanced.result_identity == before.result_identity
    assert advanced.store_generation == 8
    assert advanced.index_through_generation == 8


def test_new_store_generation_is_explicitly_stale_until_applied():
    pressure = bytes.fromhex("000000000000000000000001")
    store = bytes.fromhex("000000000000000000000002")
    commit = _commit(7, [_relation(2, pressure, store, 7, 0)])
    index = DerivedStoreIndex.rebuild([commit])
    index.observe_store_generation(8)
    stale = index.query_relations(target=store, kind=2)
    assert stale.freshness == "stale"
    assert stale.complete is False


def test_store_feed_kernel_agrees_with_projection(kernels):
    assert kernels.store_through_generation(7, 7) == 0
    assert kernels.store_through_generation(7, 8) == 1
    assert kernels.store_through_generation(9, 8) == 2
    assert kernels.store_complete(7, 7) is True
    assert kernels.store_complete(7, 8) is False
