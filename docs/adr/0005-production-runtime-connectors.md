# ADR 0005: Production runtime connector architecture

## Context

dpone has evolved from a BigQuery-oriented batch framework into an OSS data movement runtime that supports databases, REST APIs, Kafka, schema evolution, reconciliation, CDC/state offsets, quality gates, and operational artifacts.

Without a connector architecture decision, new integrations could easily bypass the runtime contracts and create one-off loaders, hidden state, direct target mutations, or optional dependency imports at package import time.

## Decision

Production connectors must plug into the runtime through explicit source, sink, connector, state, and artifact contracts.

The runtime architecture uses these rules:

- Sources produce typed artifacts such as in-memory rows, streaming rows, file exports, partitioned file exports, or internal query artifacts.
- Database sinks load through staging or shadow tables before target promotion.
- Kafka sinks append events and never mutate topics as if they were tables.
- State is explicit and committed only after sink success.
- Schema evolution runs before staging/final load and applies only safe changes automatically.
- Reconciliation and physical delete handling are opt-in and staging-first.
- Optional dependencies are imported lazily, so `import dpone` stays lightweight.
- Connector docs, examples, tests, and certification artifacts are part of the connector contract.

## Consequences

- New connectors are slightly more structured to implement, but easier to test and certify.
- Performance paths can be connector-native: MSSQL `bcp`, Postgres `COPY`, ClickHouse TSV/HTTP/client paths, Kafka partitioned produce, and BigQuery native load jobs.
- Operational behavior is consistent across source -> sink combinations.
- The runtime can expose self-service commands such as `doctor`, `plan`, `state`, `run-report`, `connectors certify`, and `perf advise` without duplicating connector logic.

## Alternatives considered

### One connector class with ad-hoc methods

Rejected. It is simple at first but tends to mix credentials, SQL rendering, loading, state, and diagnostics in one object.

### Direct source-to-sink pair implementations

Rejected for the default architecture. Pair-specific optimizations are allowed, but only behind shared artifact and sink strategy contracts.

### Make Kafka a streaming runtime

Deferred. Kafka v1 is a bounded batch source/sink that fits the current dpone batch execution model. Infinite streaming can be introduced later as a separate runtime mode.
