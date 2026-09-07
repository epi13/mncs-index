# Tests

The first executable test suite should prove behavior rather than directory shape.

Priority tests:

1. canonical hash equality across worker counts,
2. randomized schedule/delay equality,
3. bounded queue/backpressure behavior,
4. cancellation while producers/consumers are blocked,
5. worker failure propagation,
6. atomic snapshot publication,
7. incremental update equals clean rebuild,
8. stale work cannot overwrite a newer source generation.

Fixtures should be deterministic and small enough for ordinary CI; separate stress corpora may be larger.
