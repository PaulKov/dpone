# CDC live runtime adapters

CDC live runtime adapters turn the generic runtime loop into a real source ->
sink replication tick. The first supported live route is `mssql -> clickhouse`:

1. read SQL Server CDC or Change Tracking changes through an injected reader;
2. apply the normalized `CDCBatch` to ClickHouse through `ClickHouseCdcSinkApplier`;
3. save the SQL-backed offset only when the sink receipt is durable;
4. write the normal `cdc_runtime_run.json` and `cdc_runtime_run.md` reports.

The safety rule is the same as the local runtime orchestrator: commit offset
only after durable sink apply. A failed or non-durable ClickHouse receipt blocks
the offset commit.

Short rule for runbooks and reviews: commit offset only after durable sink apply.

## User workflow

1. Run route readiness, CDC apply certification, handoff, observability,
   recovery, schema evolution, and promotion gates for the stream.
2. Create SQL Server and ClickHouse credentials in the configured credentials
   source.
3. Enable SQL Server CDC or Change Tracking permissions for the runner.
4. Run `dpone ops cdc-runtime-run --mode live`.
5. Review `cdc_runtime_run.json`, ClickHouse row counts, and SQL-backed offset
   state before scheduling the command.

## Live CLI example

Run one live `mssql -> clickhouse` tick with SQL Server CDC:

```bash
dpone ops cdc-runtime-run \
  --mode live \
  --source mssql \
  --sink clickhouse \
  --backend mssql_cdc \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --source-connection-id mssql-prod \
  --sink-connection-id clickhouse-prod \
  --credentials-source env \
  --state-schema etl_state \
  --state-table etl_cdc_offset \
  --output-dir test_artifacts/cdc_runtime/mssql_to_clickhouse/orders \
  --format json
```

For SQL Server Change Tracking, use `--backend mssql_change_tracking`.
`--change-tracking-key` is repeatable and defaults to `--unique-key`.

## ClickHouse apply model

`ClickHouseCdcSinkApplier` writes an append-only CDC log table. Each row
contains:

| Column | Purpose |
| --- | --- |
| `dpone_cdc_stream_id` | Canonical runtime stream identity. |
| `dpone_cdc_operation` | `insert`, `update`, or `delete`. |
| `dpone_cdc_position` | SQL Server LSN or Change Tracking version. |
| `dpone_cdc_event_hash` | Canonical event hash used for idempotency evidence. |
| `dpone_cdc_unique_key_hash` | Hash of the configured unique key payload. |
| `dpone_cdc_payload_json` | Source row payload as canonical JSON. |
| `dpone_cdc_before_json` | Optional before image as canonical JSON. |
| `dpone_cdc_deleted` | `1` for delete events. |
| `dpone_cdc_ingested_at` | ClickHouse ingest timestamp. |

The default apply mode is `clickhouse_append_cdc_log`. It is intentionally
safe for a first live replication path: downstream materialized views,
ReplacingMergeTree tables, or later compaction jobs can derive serving tables
without forcing heavy target mutations in the offset-critical path.

## SQL-backed offset store

Live mode uses the existing SQL Server `MSSQLCDCOffsetStorage` through
`SqlCdcOffsetStoreAdapter`. The primary key includes pipeline name, source
schema, source table, and backend. The offset is saved only after ClickHouse
returns a durable `CdcApplyReceipt`.

## Generated artifacts

| Artifact | Purpose |
| --- | --- |
| `cdc_runtime_run.json` | Machine-readable stream, offset, receipt, blocker, and metric contract. |
| `cdc_runtime_run.md` | Human-readable review artifact. |
| SQL-backed offset row | Durable next LSN or Change Tracking version. |
| ClickHouse CDC log rows | Durable sink-side event log. |

## Local Docker integration test

Run the live adapter test against local SQL Server and ClickHouse containers:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql clickhouse

DPONE_RUN_INTEGRATION=1 \
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=51433 \
DPONE_IT_MSSQL_PASSWORD='Dp0ne.Strong.Pw.2026!' \
uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
```

The test creates an isolated SQL Server database, enables Change Tracking,
runs insert, update, and delete ticks through `CdcRuntimeOrchestrator`, verifies
ClickHouse CDC log rows, and then verifies that a forced ClickHouse apply
failure does not advance the SQL-backed offset.

## Runbook

| Symptom | Action |
| --- | --- |
| `cdc_runtime.duplicate_events` | Check SQL Server CDC window boundaries, Change Tracking version reuse, and unique key configuration. |
| `clickhouse_cdc_apply.failed` | Inspect ClickHouse credentials, table permissions, disk space, and insert errors in `sink_receipt.metrics.error`. |
| Offset does not advance | Confirm ClickHouse receipt has `passed=true` and `durable=true`; failed applies intentionally do not commit offsets. |
| SQL Server CDC LSN is behind retention | Re-run snapshot handoff and promotion gates before restarting live mode. |
| Schema drift appears in payload JSON | Run CDC schema evolution evidence and update downstream ClickHouse serving table design before compaction. |

## Related docs

- [CDC runtime orchestrator](cdc-runtime-orchestrator.md)
- [Developer CDC live runtime adapters](developer-cdc-live-runtime-adapters.md)
- [MSSQL -> ClickHouse guide](source-sink/mssql-to-clickhouse.md)
