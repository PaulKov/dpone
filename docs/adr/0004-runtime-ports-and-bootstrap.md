# ADR 0004: Runtime ports and bootstrap

Status: accepted

## Context

`dpone` supports many optional systems: Postgres, MSSQL, ClickHouse, BigQuery, Kafka, REST APIs, Vault, and provider SDKs. Importing all runtime dependencies eagerly would make the base package fragile and heavy.

## Decision

Use explicit runtime ports and lazy bootstrap wiring. Factories create sources, sinks, state backends, quality gates, and schema evolution services only when the resolved manifest requires them.

## Consequences

- `import dpone` stays lightweight.
- Missing optional extras fail with actionable runtime diagnostics.
- Connector packages can be tested independently.
- CLI commands that do not execute a connector remain usable without all extras installed.

## Related docs

- [Import rules](../import-rules.md)
- [Connector overview](../connectors.md)
- [Doctor command](../cli-reference.md)
