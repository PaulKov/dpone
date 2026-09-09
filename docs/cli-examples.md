# CLI examples

This page contains practical command examples for day-to-day `dpone` usage.

## Validate a manifest

```bash
dpone manifest validate examples/batch/landing_orders.batch.yaml
```

The command validates the manifest structure, load strategy, source/sink configuration, and optional sections such as state, schema evolution, reconciliation, quality, and observability.

## Explain a manifest

```bash
dpone manifest explain examples/batch/landing_orders.batch.yaml --format text
```

Use this before running a pipeline to confirm resolved defaults, dependency edges, selected strategies, and runtime wiring.

## Generate a dry-run execution plan

```bash
dpone plan examples/batch/landing_orders.batch.yaml --format md
```

The plan is read-only. It shows the source boundary, staging/shadow table path, schema evolution actions, reconciliation behavior, state transitions, partitioning, and quality gates.

## Run a batch manifest locally

```bash
dpone batch run examples/batch/landing_orders.batch.yaml
```

For production usage, prefer running through your orchestrator and keep credentials in a secret backend.

## Inspect runtime dependencies

```bash
dpone doctor --profile local
```

The doctor command checks optional extras, local clients such as `bcp` and `clickhouse-client`, Docker availability, and configured credentials. Secrets are always redacted.

## Inspect state

```bash
dpone state inspect --backend postgres --connection-id state_postgres --state-kind run
```

State commands support `inspect`, `export`, `compare`, `replay-from`, and guarded destructive `reset` operations.

## Reset state with an explicit guard

```bash
dpone state reset --backend mssql --connection-id state_mssql --state-kind kafka_offsets --yes
```

Destructive state operations require `--yes`. Use `dpone plan` or `state export` first when preparing a migration.

## Connector certification

```bash
dpone connectors certify \
  --artifact-dir test_artifacts/connector-certification \
  --format markdown
```

Certification writes `connector-certification.json` and
`connector-certification.md` and exits `1` while required evidence is missing.
Use `--report-only` only for a deliberately non-gating report.

## Performance advice

```bash
dpone perf advise examples/batch/postgres_to_mssql.yaml --format text
```

The advisor suggests partitioning, batch size, native bulk paths, ClickHouse direct TSV/HTTP loading, Kafka producer settings, and state/reconciliation tuning.
