# Evidence

Recorded runs proving the implementation claims. Each record is a JSON
file with corpus stats, worker matrix, canonical hashes, timings, and
the exact commands used. Records are produced by
`scripts/collect_evidence.sh`; they are evidence, not fixtures — tests
never read them.

## How to reproduce

```bash
# Indexed fixture corpus (fast)
scripts/collect_evidence.sh --corpus tests/fixtures/corpus --out evidence/fixture.json

# Real MNCS corpus (slower; one subprocess per 64 B window)
scripts/collect_evidence.sh --corpus <path> --out evidence/<name>.json --workers 4,8,16
```

`MNCS_BIN` selects the executor (default: sibling
`../mncs-language/target/debug/mncs`).
