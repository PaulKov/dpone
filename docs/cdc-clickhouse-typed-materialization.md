# ClickHouse CDC typed materialization

ClickHouse CDC typed materialization projects canonical `dpone_cdc_*` JSON
payload rows into a typed current-state serving table. It is the typed serving
layer after the append-only CDC log exists: `dpone ops cdc-runtime-run --mode
live` keeps offsets safe, `dpone ops cdc-materialize-clickhouse` rebuilds JSON
current state, and `dpone ops cdc-materialize-clickhouse-typed` creates a
business-friendly table with declared ClickHouse column types.

The first production route is `mssql -> clickhouse`, but the projection contract
is generic for any CDC source that writes the normalized `dpone_cdc_*` log.

## Workflow

1. Run the CDC runtime until the ClickHouse CDC log contains durable events.
2. Choose a typed serving table name separate from the append-only log.
3. Declare every projected payload column with `--column name=ClickHouseType`.
4. Run `dpone ops cdc-materialize-clickhouse-typed`.
5. Review `cdc_typed_materialization.json`, `cdc_typed_materialization.md`,
   optional `cdc_typed_parse_quarantine.json`, row counts, delete mode, and the
   generated typed column inventory.
6. Promote dashboards and downstream jobs only after the report has
   `passed=true`.

Example:

```bash
dpone ops cdc-materialize-clickhouse-typed \
  --cdc-dataset analytics.orders_cdc \
  --target-dataset serving.orders_current_typed \
  --unique-key order_id \
  --column order_id=Int32 \
  --column status=Nullable(String) \
  --column amount=Decimal(18,2) \
  --sink-connection-id clickhouse-prod \
  --credentials-source env \
  --delete-mode exclude_deleted \
  --fail-on-parse-errors \
  --max-parse-error-ratio 0 \
  --quarantine-dataset serving.orders_parse_quarantine \
  --schema-drift-mode strict \
  --output-dir test_artifacts/cdc_typed_materialization/mssql_to_clickhouse/orders \
  --format json
```

For compound keys, repeat `--unique-key`:

```bash
dpone ops cdc-materialize-clickhouse-typed \
  --cdc-dataset analytics.order_lines_cdc \
  --target-dataset serving.order_lines_current_typed \
  --unique-key order_id \
  --unique-key line_id \
  --column order_id=Int64 \
  --column line_id=Int32 \
  --column quantity=Nullable(Decimal(18,4)) \
  --sink-connection-id clickhouse-prod
```

## Column declarations

Each `--column` value uses `name=ClickHouseType`. The column name is the target
serving column and, by default, the JSON payload key. Supported first-class
projection types are:

| Family | Examples |
| --- | --- |
| Text and boolean | `String`, `Nullable(String)`, `Bool`, `Nullable(Bool)` |
| Integers | `Int32`, `UInt64`, `Nullable(Int64)` |
| Floating point | `Float64`, `Nullable(Float32)` |
| Exact decimals | `Decimal(18,2)`, `Nullable(Decimal(18,4))` |
| Temporal | `Date`, `DateTime`, `DateTime64(3, 'UTC')` |

The materializer keeps CDC metadata columns next to the typed projection so
operators can audit the source key, operation, position, delete flag, raw
payload, and materialization timestamp.

## Quality gates

Typed materialization is fail-closed when quality gates are enabled. The service
evaluates schema drift and parse failures before it swaps the shadow table into
the serving target, so a bad projection cannot silently replace a healthy
current-state table.

| Flag | Behavior |
| --- | --- |
| `--fail-on-parse-errors` | Blocks promotion when typed conversion failures exceed `--max-parse-error-ratio`. |
| `--max-parse-error-ratio` | Maximum allowed typed parse failure ratio. Use `0` for strict replication-grade certification. |
| `--quarantine-dataset` | Writes failed typed projection rows to a ClickHouse quarantine table for replay and triage. |
| `--schema-drift-mode strict` | Blocks promotion on missing required payload keys and additive payload keys. |
| `--schema-drift-mode warn_additive` | Blocks missing required keys and warns on additive keys. This is the default. |
| `--schema-drift-mode off` | Disables schema drift blocking for exploratory runs. |
| `--quality-sample-limit` | Caps payload-key sampling used for schema drift evidence. |

Parse quarantine stores the failed column name, ClickHouse type, unique key,
event position, event hash, raw payload, and reason. This makes the report useful
for operators, CI gates, and support bundles without requiring direct access to
the production serving table.

## Delete modes

| Mode | Behavior | Use when |
| --- | --- | --- |
| `--delete-mode exclude_deleted` | Latest deleted keys are omitted from the typed serving table. | Consumers expect only active current rows. |
| `--delete-mode tombstone` | Latest deleted keys remain with `dpone_cdc_deleted = 1`. | Consumers need deletion audit, cache invalidation, or soft-delete semantics. |

For `tombstone` tables, nullable business columns are recommended because a
delete event may contain only key values and CDC metadata.

## Evidence artifacts

| Artifact | Purpose |
| --- | --- |
| `cdc_typed_materialization.json` | Stable machine-readable contract for CI, release gates, and run registry ingestion. |
| `cdc_typed_materialization.md` | Human-readable review artifact for operators. |
| `cdc_typed_parse_quarantine.json` | Machine-readable parse quarantine summary with failed columns, ratios, and blockers. |
| `cdc_typed_parse_quarantine.md` | Human-readable parse quarantine triage report. |
| Typed ClickHouse serving table | Current-state table with declared business columns plus CDC metadata. |

The JSON report uses schema version
`dpone.cdc_clickhouse_typed_materialization.v1` and includes source/target
datasets, unique keys, typed columns, delete mode, row counts, blockers,
warnings, schema policy, schema drift evidence, and parse quarantine evidence.

## Local Docker integration test

The opt-in vendor-live test proves the full `mssql -> clickhouse` path:

1. SQL Server Change Tracking produces insert, update, and delete events.
2. `CdcRuntimeOrchestrator` writes the append-only ClickHouse CDC log.
3. `ClickHouseCdcTypedMaterializationService` builds active-row and tombstone
   typed serving tables.
4. The test checks typed values, delete handling, row counts, and evidence
   artifacts.

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
| `clickhouse_cdc_typed_materialization.failed` | Inspect `metrics.error`, ClickHouse permissions, source CDC table existence, target database permissions, and unsupported column types. |
| `clickhouse_cdc_typed_materialization.parse_quarantine` | Open `cdc_typed_parse_quarantine.json`, review failed columns and source keys, fix upstream payload formatting or relax the declared ClickHouse type, then rerun materialization. |
| `clickhouse_cdc_typed_materialization.schema_drift` | Compare declared columns to payload keys, add a missing payload field upstream, adjust `--column`, or rerun with `--schema-drift-mode warn_additive` for additive-only rollout. |
| Typed column is always NULL | Check the payload key spelling, source payload JSON, and whether the ClickHouse type can parse the JSON value. |
| Tombstone materialization fails on non-null columns | Use nullable projected columns for fields that may be absent from delete events. |
| Serving table has fewer rows than expected | Check `--delete-mode`; `exclude_deleted` intentionally removes latest tombstones. |
| A dashboard needs raw payload details | Keep `dpone_cdc_payload_json` in the typed table and join/debug from it rather than reading the append-only log directly. |
