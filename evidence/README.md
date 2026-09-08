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

# Multi-repo ecosystem survey (one file per sibling mncs-* repo)
MNCS_BIN=<path-to-mncs> python3 scripts/run_ecosystem.py --out evidence/ecosystem-mncs-repos.json --workers 2,8

# MNCS-family scale campaign (family tier + five-class mutation batch)
MNCS_BIN=<path-to-mncs> python3 scripts/run_ecosystem.py --tier family --max-files-per-repo 4 --per-file-bytes 4096 --mutations --workers 1,2 --out evidence/ecosystem-mncs-family.json
```

Tiers (`runner/mncs_index/ecosystem.py`): `census` (every `*.mncs` in
every sibling repo — inventoried, priced beyond an in-session build),
`survey` (one file per repo, legacy), `family` (up to N files per repo
under a byte cap — the executed scale corpus), `mutation` (the
change/add/remove/rename/RFC+pressure batch proving incremental ==
rebuild including rich tables and invalidation).

`MNCS_BIN` selects the executor (default: sibling
`../mncs-language/target/debug/mncs`).
