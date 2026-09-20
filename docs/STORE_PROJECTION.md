# Store-backed Index projection

`mncs-index` now has an explicit `DerivedStoreIndex` adapter for the versioned
Store relation and commit-feed records. Its state is disposable: it contains
only derived relation maps, an Index-through generation, and a query-result
identity. It does not allocate Store logical identities, publish generations,
or own pressure/family meanings.

The `store_feed.mncs` kernel owns the freshness vocabulary:

```text
index through Store generation == Store head  → complete
index through Store generation < Store head    → stale/incomplete
index through Store generation > Store head    → invalid future state
```

The existing `runner/mncs_index/store.py` remains a scheduled migration and
differential/reference path. New Store-backed projection code must not add
canonical persistence semantics there. Full publication/MVCC cutover is a
follow-up once the live Store feed adapter is connected to a Store instance.
