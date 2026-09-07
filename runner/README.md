# Runner

**Temporary host infrastructure.** This package exists only because MNCS
cannot yet express the effects an indexing pipeline needs. Each module
names the pressure entries that will delete it.

| Module | Stands in for | Pressure |
|--------|---------------|----------|
| `bridge.py` | task spawn, in-process invocation, effect gating | PRESS-001, PRESS-010 |
| `discover.py` | filesystem traversal + bounded parallel reads | PRESS-003 |
| `pipeline.py` | threads, bounded channels, fan-out/fan-in, shutdown | PRESS-001, PRESS-002, PRESS-012 |
| `kernels.py` | (thin typed wrappers; no semantics) | — |
| `model.py` | big-integer/hashing scale-out; sort at corpus scale | PRESS-005, PRESS-006 |
| `extract.py` | line splitting/trimming/slicing; 64 B windowing; sort/dedup at scale | PRESS-005, PRESS-014 |
| `indexer.py` | incremental orchestration | PRESS-001 |
| `store.py` | durable atomic publication | PRESS-003 |
| `query.py` | parallel query fan-out; unbounded search; single-hop graph lookup | PRESS-001, PRESS-005, PRESS-015 |
| `watch.py` | filesystem watching, clocks | PRESS-007 |
| `cli.py` | process/CLI boundary | — |

Rules for this directory:

1. No indexing *meaning* here: digests, classes, kinds, orders, matches,
   and change verdicts are MNCS kernel verdicts. The host moves bytes,
   bounds queues, and sorts by the specified key.
2. Every host mirror of an MNCS rule must be differentially tested
   (`tests/test_differential.py`).
3. Caches change call counts, never meaning (pure functions only).
4. When `mncs-language` gains the missing capability, the corresponding
   module shrinks to a binding — tracked per PRESS entry.
