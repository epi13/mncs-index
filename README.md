# mncs-index

A machine-native, highly concurrent indexing and query engine for the MNCS ecosystem, providing deterministic incremental indexing across source code, RFCs, artifacts, tests, diagnostics, and project metadata.

## Status (first working implementation)

This repository now contains a **working deterministic corpus indexer**:

- Real MNCS kernels in `src/*.mncs` (content digests, byte classification,
  symbol validation, kind classification, canonical ordering, query
  matching, change classification) — every semantic decision is MNCS.
- A thin host runner in `runner/mncs_index/` (discovery, thread pools,
  bounded queues, canonical merge, SHA-256, store, query CLI, polling
  watch) — effects MNCS cannot yet express, each mapped to a pressure entry.
- 60+ pytest tests proving worker-count equivalence, repeatability,
  schedule perturbation convergence, incremental == rebuild, query
  determinism, failure publication, stress, and watch following.
- A grounded pressure registry (`pressure/registry.md`, 12 entries with
  reproducers) against current `mncs-language`.

What does not exist yet: MNCS-native threads/channels/filesystem (P0
pressure), in-process kernel invocation (P2), crypto digests in MNCS (P1),
event-driven watching (P1). See [Pressure](#pressure) and
`pressure/registry.md`.

## Founding invariant

> For a fixed input snapshot and configuration, changing concurrency may change throughput and execution order, but must not change the canonical index or query meaning.

```text
index(snapshot, workers = 1)  -> H
index(snapshot, workers = 2)  -> H
index(snapshot, workers = 8)  -> H
index(snapshot, workers = 32) -> H
```

Proven by `tests/test_determinism.py` and `tests/test_stress.py`.

## Quick start

Prerequisites: Python 3.10+, pytest, and a built `mncs-language` executor
(`cargo build -p mncs-cli` in a sibling checkout, or set `MNCS_BIN`).

```bash
# Build an index over a corpus directory
PYTHONPATH=runner python3 -m mncs_index.cli build \
  --corpus tests/fixtures/corpus --store /tmp/demo-store --workers 8

# Query it
PYTHONPATH=runner python3 -m mncs_index.cli query --store /tmp/demo-store --term index
PYTHONPATH=runner python3 -m mncs_index.cli query --store /tmp/demo-store --kind 1
PYTHONPATH=runner python3 -m mncs_index.cli query --store /tmp/demo-store --path a.mncs

# Incremental follow-up after changing files
PYTHONPATH=runner python3 -m mncs_index.cli build \
  --corpus tests/fixtures/corpus --store /tmp/demo-store --workers 8 --incremental

# Polling watch (baseline must exist first)
PYTHONPATH=runner python3 -m mncs_index.cli watch \
  --corpus tests/fixtures/corpus --store /tmp/demo-store --generations 2
```

## Testing

```bash
python3 -m pytest tests/ -q            # full default suite (bounded for CI)
python3 -m pytest tests/ -q --run-slow # + megabyte-scale determinism
```

Suite map:

| File | Proves |
|------|--------|
| `test_kernels.py` | Every MNCS kernel function pinned by execution |
| `test_differential.py` | Kernels agree with independent oracles on seeded random inputs |
| `test_determinism.py` | Worker counts / seeds / delays / queues converge; real overlap happened |
| `test_incremental.py` | `incremental(A→B) == rebuild(B)` for add/remove/change/rename/multi |
| `test_query.py` | Query classes, canonical order, limits, repeatability |
| `test_failure.py` | Failed builds never publish; typed errors; cancellation |
| `test_stress.py` | 140+ files, duplicates, deep paths, skewed sizes, workers to 32 |
| `test_watch.py` | Polling watcher follows mutations to equivalent generations |

## How the pieces fit

```text
corpus files
    |
    v  host: discover.py (walk/read/normalize; PRESS-003)
bounded work queue (PRESS-002)
    |
    +--> kernel workers: ONE `mncs execute` per item (PRESS-001/010)
    |       digest.mncs  leaf/combine/fold  (content identity)
    |       scan.mncs    classify/validate/token digests
    |       kind.mncs    extension -> kind rank
    |       order.mncs   compare/match/change verdicts
    v
deterministic assembly (ordered Merkle tree, carry fix-up)
    |
    v
canonical merge (sort by the MNCS-specified rule; PRESS-005)
    |
    +--> canonical bytes -> SHA-256 (PRESS-006) + MNCS fold
    +--> atomic publish (tmp + rename)
    +--> query readers / incremental base / watcher
```

## MNCS implementation ratio

| Layer | Language | Evidence |
|-------|----------|----------|
| Content digest fold + tree combine | MNCS (`src/digest.mncs`) | kernel + differential tests |
| Byte classes, token validity/digests | MNCS (`src/scan.mncs`) | kernel + differential tests |
| Kind classification | MNCS (`src/kind.mncs`) | kernel tests |
| Ordering, matching, change verdicts | MNCS (`src/order.mncs`) | kernel + differential tests |
| Discovery, threads, queues, merge scale-out, SHA-256, store, CLI | Python (temporary) | labeled per-module; PRESS-001/002/003/006 |

Host scale-out code that mirrors an MNCS rule (lexicographic sort,
substring search beyond 8 B, token splitting) is differentially tested
against the kernel, never trusted silently.

## Pressure

Twelve grounded entries: `pressure/registry.md`, plus minimal MNCS
reproducers under `pressure/reproducers/`. Highest-priority next language
work: in-process/batch kernel invocation (PRESS-010), integer bitwise ops
(PRESS-004), `u64` traversal domains (PRESS-005), crypto digests (PRESS-006).

## Layout

```text
.
├── rfcs/                 Project design decisions
├── docs/                 Architecture, integrations, testing and pressure guidance
├── src/                  MNCS kernels (*.mncs) + execution corpora notes
├── runner/mncs_index/    Thin host infrastructure (temporary, labeled)
├── tests/                Pytest suites + fixtures + test-only oracles
├── pressure/             Registry + minimal reproducers
├── evidence/             Recorded runs (hashes, timings, worker matrices)
├── .github/              Foundation contract + index CI
├── AGENTS.md
├── CONTRIBUTING.md
├── ROADMAP.md
└── SECURITY.md
```

## RFCs

1. [RFC 0001 — Project Charter and Architecture](rfcs/0001-project-charter.md)
2. [RFC 0002 — Deterministic Concurrent Indexing](rfcs/0002-deterministic-concurrent-indexing.md)
3. [RFC 0003 — Canonical Index Model](rfcs/0003-canonical-index-model.md)
4. [RFC 0004 — Query Model](rfcs/0004-query-model.md)
5. [RFC 0005 — Incremental and Watch Model](rfcs/0005-incremental-watch-model.md)
6. [RFC 0006 — Language Pressure Methodology](rfcs/0006-language-pressure-methodology.md)

## License

Apache-2.0.
