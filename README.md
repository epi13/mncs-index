# mncs-index

A machine-native, highly concurrent indexing and query engine for the MNCS ecosystem, providing deterministic incremental indexing across source code, RFCs, artifacts, tests, diagnostics, and project metadata.

## Why this exists

MNCS is becoming a family of repositories, runtimes, backends, artifacts, RFCs, tests, pressure reports, and machine-generated evidence. Finding the right fact should not depend on a human remembering which repository or document contains it.

`mncs-index` is intended to become the shared indexing substrate for that ecosystem.

It is also deliberately a **language pressure project**. Indexing has abundant natural parallelism, but useful indexing requires much more than spawning threads: bounded queues, cancellation, fan-out/fan-in, deterministic reduction, concurrent state, snapshots, incremental invalidation, graceful shutdown, and repeatable results under different schedules.

## Founding invariant

> For a fixed input snapshot and configuration, changing concurrency may change throughput and execution order, but must not change the canonical index or query meaning.

A central conformance target is therefore:

```text
index(snapshot, workers = 1)  -> H
index(snapshot, workers = 2)  -> H
index(snapshot, workers = 8)  -> H
index(snapshot, workers = 32) -> H
```

where `H` is the same canonical index hash.

## Intended inputs

- MNCS source and compiler-visible metadata
- RFCs and architectural documentation
- tests and test evidence
- diagnostics and compiler output
- git/project metadata
- CI and harness artifacts
- language-pressure findings
- future normalized semantic records from `mncs-ingest`
- future graph/memory relationships from `mncs-memory`

## Intended consumers

- Atlas and agents
- mncs-harness
- mncs-fabric
- mncs-language tooling
- mncs-memory / mncs-ingest
- project-local developer tooling
- future MNCS-native query and analysis tools

## Repository state

This first project merge defines the charter, RFCs, architecture, integration boundaries, testing strategy, and pressure methodology. It does **not** claim a production indexer exists yet.

Implementation work should remain overwhelmingly MNCS-language. Another implementation language must not become the quiet escape hatch when MNCS encounters pressure; pressure should be recorded and fed back to `mncs-language`.

## Layout

```text
.
├── rfcs/                 Project design decisions
├── docs/                 Architecture, integrations, testing and pressure guidance
├── src/                  MNCS-native implementation area
├── tests/                Conformance and pressure tests
├── pressure/             Language-pressure registry and evidence
├── .github/              Lightweight repository validation
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

## Near-term proof

The first meaningful implementation milestone is intentionally narrow:

1. index a deterministic fixture tree,
2. process files concurrently,
3. emit canonical records in a schedule-independent order,
4. hash the canonical output,
5. repeat the same corpus under multiple worker counts and randomized scheduling,
6. prove identical hashes and query results,
7. record every MNCS-language gap encountered while doing so.

That proof is more valuable than a broad indexer that silently avoids the hard concurrency problems.

## License

Apache-2.0.
