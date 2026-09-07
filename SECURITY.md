# Security

`mncs-index` will ingest data from repositories, build artifacts, diagnostics, and potentially machine-generated sources. Treat indexed input as untrusted unless the surrounding system establishes stronger provenance.

## Security model goals

- parsing must not imply execution,
- indexing a repository must not grant that repository ambient authority,
- query results must preserve provenance,
- canonicalization must not erase security-relevant distinctions,
- resource use must be bounded,
- cancellation must remain effective under hostile input,
- malformed input must not poison unrelated index partitions,
- remote/Fabric execution must retain explicit capability boundaries.

## Never by default

Indexing must not automatically:

- execute discovered binaries or scripts,
- follow arbitrary network references,
- expand unbounded archives,
- dereference paths outside the declared source root,
- reveal indexed secrets to consumers lacking authority.

## Concurrency-specific concerns

Security review should include races around snapshot publication, stale authorization state, cancellation, resource exhaustion, queue saturation, and shared mutable caches.

Report security-sensitive findings privately through the repository owner's established GitHub security channel when available rather than publishing exploit details in an issue.
