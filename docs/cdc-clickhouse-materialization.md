# ClickHouse CDC materialization

ClickHouse CDC materialization turns the append-only `dpone_cdc_*` log written
by `ClickHouseCdcSinkApplier` into a current-state serving table. It is the
serving layer after `dpone ops cdc-runtime-run --mode live`: the runtime remains
offset-critical and append-only, while this command can rebuild the latest view
of each source key on demand.

The first production route is `mssql -> clickhouse`, but the table contract is
generic for any future CDC source that writes the normalized `dpone_cdc_*`
columns.

For business-facing current-state tables with declared ClickHouse types, use
[ClickHouse CDC typed materialization](cdc-clickhouse-typed-materialization.md)
after the append-only CDC log is populated.

## Workflow

1. Run the CDC runtime until the ClickHouse CDC log table contains durable
   events.
2. Choose a serving table name separate from the append-only log.
3. Run `dpone ops cdc-materialize-clickhouse`.
4. Review `cdc_materialization.json`, `cdc_materialization.md`, and row counts.
5. Promote dashboards and downstream jobs to the serving table only after the
   materialization report has `passed=true`.

Example:

```bash
dpone ops cdc-materialize-clickhouse \
  --cdc-dataset analytics.orders_cdc \
  --target-dataset serving.orders_current \
  --unique-key order_id \
  --sink-connection-id clickhouse-prod \
  --credentials-source env \
  --delete-mode exclude_deleted \
  --output-dir test_artifacts/cdc_materialization/mssql_to_clickhouse/orders \
  --format json
```

For compound keys, repeat `--unique-key`:

```bash
dpone ops cdc-materialize-clickhouse \
  --cdc-dataset analytics.order_lines_cdc \
  --target-dataset serving.order_lines_current \
  --unique-key order_id \
  --unique-key line_id \
  --sink-connection-id clickhouse-prod
```

## Delete modes

| Mode | Behavior | Use when |
| --- | --- | --- |
| `--delete-mode exclude_deleted` | Latest deleted keys are omitted from the serving table. | Consumers expect only active current rows. |
| `--delete-mode tombstone` | Latest deleted keys remain in the serving table with `dpone_cdc_deleted = 1`. | Consumers need deletion audit, cache invalidation, or soft-delete semantics. |

Both modes dedupe by `dpone_cdc_unique_key_hash` and choose the latest event by
CDC ingest time, source position length/value, source sequence, and event hash.
The command writes through a shadow-table replace so failed materializations do
not partially mutate the target table.

## Serving table columns

The serving table keeps the normalized CDC metadata and canonical JSON payload:

| Column | Purpose |
| --- | --- |
| `dpone_cdc_unique_key_hash` | Stable hash used as the materialization dedupe key. |
| `dpone_cdc_unique_key_json` | Canonical JSON key payload for audits and debugging. |
| `dpone_cdc_payload_json` | Latest source row payload as canonical JSON. |
| `dpone_cdc_operation` | Latest operation for the key. |
| `dpone_cdc_position` | Source LSN, Change Tracking version, or future source position token. |
| `dpone_cdc_deleted` | `1` when the latest event is a delete. |
| `dpone_cdc_materialized_at` | Time when the serving row was rebuilt. |

## Evidence artifacts

| Artifact | Purpose |
| --- | --- |
| `cdc_materialization.json` | Stable machine-readable contract for CI, release gates, and run registry ingestion. |
| `cdc_materialization.md` | Human-readable review artifact for operators. |
| ClickHouse serving table | Current-state table rebuilt from the CDC log. |

## Local Docker integration test

The opt-in vendor-live test proves the full `mssql -> clickhouse` path:

1. SQL Server Change Tracking produces insert, update, and delete events.
2. `CdcRuntimeOrchestrator` writes the append-only ClickHouse CDC log.
3. `ClickHouseCdcMaterializationService` builds active-row and tombstone
   serving tables.
4. The test checks current-state row counts and materialization evidence.

Run it locally:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql clickhouse

DPONE_RUN_INTEGRATION=1 \
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=51433 \
DPONE_IT_MSSQL_DATABASE=dpone_it \
DPONE_IT_MSSQL_CDC_HOST=127.0.0.1 \
DPONE_IT_MSSQL_CDC_PORT=51433 \
DPONE_IT_MSSQL_CDC_DATABASE=dpone_it \
DPONE_IT_MSSQL_PASSWORD='Dp0ne.Strong.Pw.2026!' \
DPONE_IT_MSSQL_CDC_PASSWORD='Dp0ne.Strong.Pw.2026!' \
DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE=yes \
DPONE_IT_CH_HOST=127.0.0.1 \
DPONE_IT_CH_PORT=59000 \
DPONE_IT_CH_HTTP_PORT=58123 \
DPONE_IT_CH_DATABASE=dpone_it \
DPONE_IT_CH_USER=default \
DPONE_IT_CH_PASSWORD=dpone \
uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
```

## Runbook

| Symptom | Action |
| --- | --- |
| `clickhouse_cdc_materialization.failed` | Inspect `metrics.error`, ClickHouse permissions, source CDC table existence, disk space, and target database permissions. |
| Serving table has fewer rows than expected | Check `--delete-mode`; `exclude_deleted` intentionally removes latest tombstones. |
| Deleted rows are still visible | Use `--delete-mode exclude_deleted` for active-only tables, or filter `dpone_cdc_deleted = 0` downstream. |
| A downstream report sees stale data | Re-run the bounded CDC runtime tick first, then re-run materialization and compare `rows_source_events`. |
| Materialization is slow | Keep the append-only CDC log ordered by stream/key/position and schedule compaction only after the runtime offset is durable. |
