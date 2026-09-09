# Route refresh execute

`dpone ops route-refresh-execute` consumes a `route_refresh_plan.json` file and
writes a route-scoped execution receipt for one bounded backfill, replay,
refresh, or resync. It is the execution evidence companion to
[`dpone ops route-refresh-plan`](route-refresh-plan.md) for any
`source -> sink -> strategy` route.

The command is fail-closed. By default it runs in `dry_run` mode and performs no
data movement. Real chunk execution requires `--execute` and a configured
`RouteRefreshExecutor` backend. The first built-in concrete backend is
`mssql_clickhouse` for `mssql -> clickhouse -> incremental_merge`; all other
routes still require host-application injection or a new backend adapter. The
OSS CLI does not silently mutate databases.

## CLI example

Dry-run a reviewed plan:

```bash
uv run dpone ops route-refresh-execute \
  --route-refresh-plan-json test_artifacts/route_refresh/mssql_to_clickhouse/orders/route_refresh_plan.json \
  --runner-id operator-a \
  --output-dir test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders \
  --format json
```

Validate the expected route before execution:

```bash
uv run dpone ops route-refresh-execute \
  --route-refresh-plan-json test_artifacts/route_refresh/mssql_to_clickhouse/orders/route_refresh_plan.json \
  --runner-id operator-a \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --output-dir test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders \
  --format json
```

Explicit execution is opt-in:

```bash
uv run dpone ops route-refresh-execute \
  --route-refresh-plan-json test_artifacts/route_refresh/mssql_to_clickhouse/orders/route_refresh_plan.json \
  --runner-id route-worker-1 \
  --execute \
  --executor mssql_clickhouse \
  --executor-config-json test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/executor-config.json \
  --output-dir test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders \
  --format json
```

If no concrete `RouteRefreshExecutor` is configured, `--execute` exits non-zero
with `route_refresh_execution.executor_unavailable`.

## MSSQL -> ClickHouse executor config

The built-in `mssql_clickhouse` backend uses SQL Server `bcp queryout` to export
each bounded chunk into a local TSV transfer file and `clickhouse-client` to
load that file into ClickHouse. The executor config is explicit JSON so local
tests, Docker-live jobs, and vendor-live jobs can use the same contract.

```json
{
  "source_dataset": "dbo.orders",
  "target_dataset": "analytics.orders",
  "boundary_column": "id",
  "columns": ["id", "name", "amount", "updated_at"],
  "mssql": {
    "host": "127.0.0.1",
    "port": 1433,
    "database": "source_db",
    "user": "sa",
    "password": "${MSSQL_PASSWORD}",
    "options": {
      "bcp_path": "bcp",
      "batch_size": 100000,
      "packet_size": 16384,
      "timeout_seconds": 1800,
      "trust_server_certificate": true
    }
  },
  "clickhouse": {
    "host": "127.0.0.1",
    "port": 9000,
    "database": "analytics",
    "user": "default",
    "password": "${CLICKHOUSE_PASSWORD}",
    "options": {
      "client_command": "clickhouse-client",
      "input_format": "TabSeparated",
      "timeout_seconds": 1800,
      "settings": {
        "async_insert": 1,
        "wait_for_async_insert": 1
      }
    }
  }
}
```

The default chunk query is:

```sql
SELECT [columns] FROM [source_dataset]
WHERE [boundary_column] BETWEEN <chunk.start> AND <chunk.end>
ORDER BY [boundary_column]
```

Use `query_template` only for reviewed operator-owned SQL. It can reference
`{columns}`, `{source_table}`, `{boundary_column}`, `{start}`, `{end}`, and
`{partition}`. Keep templates deterministic and bounded by the chunk window.

Per chunk, the backend writes
`chunks/mssql_clickhouse_refresh_chunk_0001.json` plus the transfer TSV. The
chunk artifact includes the route id, source/target datasets, query hash,
redacted commands, target-window prepare evidence, transfer checksum, row
counts, blockers, and warnings. The built-in loader runs a bounded ClickHouse
`ALTER TABLE ... DELETE WHERE <boundary> BETWEEN <start> AND <end> SETTINGS
mutations_sync = 2` before loading each chunk so replaying the same
`route_refresh_plan.json` is idempotent for the chunk window.

## MSSQL -> ClickHouse refresh executor live certification

The Docker-live certification test proves that the built-in backend can execute
the same route twice without duplicates:

- build a bounded `mssql -> clickhouse -> incremental_merge` refresh plan;
- execute it through `--executor mssql_clickhouse`;
- replay the same chunks to verify idempotent target-window cleanup;
- compare ClickHouse rows with a deterministic typed hash;
- require per-chunk artifacts and `route_refresh_execution.json`.

Run the opt-in gate against disposable Docker services:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql clickhouse

DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_REFRESH_EXECUTOR_LIVE=1 \
DPONE_REFRESH_EXECUTOR_LIVE_ARTIFACT_DIR=test_artifacts/live_certification/refresh-executor/mssql-clickhouse \
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=51433 \
DPONE_IT_MSSQL_DATABASE=master \
DPONE_IT_MSSQL_USER=sa \
DPONE_IT_MSSQL_PASSWORD='Dp0ne.Strong.Pw.2026!' \
DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE=yes \
DPONE_IT_MSSQL_BCP_PATH=/opt/mssql-tools18/bin/bcp \
DPONE_IT_CH_HOST=127.0.0.1 \
DPONE_IT_CH_PORT=59000 \
DPONE_IT_CH_DATABASE=dpone_it \
DPONE_IT_CH_USER=default \
DPONE_IT_CH_PASSWORD=dpone \
DPONE_IT_CH_CLIENT_COMMAND='docker exec -i dpone-it-clickhouse clickhouse-client' \
DPONE_IT_CH_CLIENT_HOST=127.0.0.1 \
DPONE_IT_CH_CLIENT_PORT=9000 \
uv run pytest tests/integration/mssql/test_mssql_clickhouse_refresh_executor_live_integration.py -q
```

The test uses the Python service API for speed, but it covers the same backend
path selected by this CLI shape:

```bash
uv run dpone ops route-refresh-execute \
  --route-refresh-plan-json test_artifacts/live_certification/refresh-executor/mssql-clickhouse/plan/route_refresh_plan.json \
  --runner-id refresh-live-certification \
  --execute \
  --executor mssql_clickhouse \
  --executor-config-json test_artifacts/live_certification/refresh-executor/mssql-clickhouse/executor-config.json \
  --output-dir test_artifacts/live_certification/refresh-executor/mssql-clickhouse/execute-replay \
  --format json
```

Expected evidence:

| File | Why it matters |
| --- | --- |
| `execute-replay/route_refresh_execution.json` | Replay receipt proving every chunk succeeded and is ready for state-promotion review. |
| `execute-replay/chunks/*.json` | Per-chunk query hash, export command, prepare command, load command, row counts, and transfer checksum. |
| `executor-config.json` | The exact backend connection and native-tool config used by the run. |

Local hosts must provide `bcp`; GitHub Actions installs
`/opt/mssql-tools18/bin/bcp`. `clickhouse-client` may be provided by the
ClickHouse container via `DPONE_IT_CH_CLIENT_COMMAND`.

## Postgres -> MSSQL executor config

The built-in `postgres_mssql` backend uses Postgres `COPY (...) TO STDOUT` with
the dpone MSSQL-delimited bulk text contract and SQL Server `bcp in` to load
each bounded chunk. The backend uses the same generic native refresh pipeline
shape as other concrete executors: prepare the target window, export the source
chunk, load the transfer file, and write a chunk artifact.

```json
{
  "source_dataset": "public.orders",
  "target_dataset": "dbo.orders",
  "boundary_column": "id",
  "columns": ["id", "name", "amount", "updated_at"],
  "postgres": {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "source_db",
    "user": "postgres",
    "password": "${POSTGRES_PASSWORD}"
  },
  "mssql": {
    "host": "127.0.0.1",
    "port": 1433,
    "database": "target_db",
    "user": "sa",
    "password": "${MSSQL_PASSWORD}",
    "trust_server_certificate": true,
    "options": {
      "bcp_path": "bcp",
      "batch_size": 100000,
      "packet_size": 16384,
      "timeout_seconds": 1800,
      "trust_server_certificate": true
    }
  }
}
```

The default chunk query is:

```sql
SELECT "columns" FROM "source_schema"."source_table"
WHERE "boundary_column" BETWEEN <chunk.start> AND <chunk.end>
ORDER BY "boundary_column"
```

Before loading each chunk, the backend executes:

```sql
DELETE FROM [target_schema].[target_table]
WHERE [boundary_column] BETWEEN <chunk.start> AND <chunk.end>
```

That prepare step makes replaying the same `route_refresh_plan.json`
idempotent for the bounded window.

## Postgres -> MSSQL refresh executor live certification

Run the opt-in Docker-live gate against disposable Postgres and MSSQL services:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mssql

DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_REFRESH_EXECUTOR_LIVE=1 \
DPONE_REFRESH_EXECUTOR_LIVE_ARTIFACT_DIR=test_artifacts/live_certification/refresh-executor/postgres-mssql \
DPONE_IT_PG_HOST=127.0.0.1 \
DPONE_IT_PG_PORT=55432 \
DPONE_IT_PG_DATABASE=dpone_it \
DPONE_IT_PG_USER=dpone \
DPONE_IT_PG_PASSWORD=dpone \
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=51433 \
DPONE_IT_MSSQL_DATABASE=master \
DPONE_IT_MSSQL_USER=sa \
DPONE_IT_MSSQL_PASSWORD='Dp0ne.Strong.Pw.2026!' \
DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE=yes \
DPONE_IT_MSSQL_BCP_PATH=/opt/mssql-tools18/bin/bcp \
uv run pytest tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py -q
```

The test covers this CLI shape:

```bash
uv run dpone ops route-refresh-execute \
  --route-refresh-plan-json test_artifacts/live_certification/refresh-executor/postgres-mssql/plan/route_refresh_plan.json \
  --runner-id refresh-live-certification \
  --execute \
  --executor postgres_mssql \
  --executor-config-json test_artifacts/live_certification/refresh-executor/postgres-mssql/executor-config.json \
  --output-dir test_artifacts/live_certification/refresh-executor/postgres-mssql/execute-replay \
  --format json
```

Expected evidence:

| File | Why it matters |
| --- | --- |
| `execute-replay/route_refresh_execution.json` | Replay receipt proving every chunk succeeded and is ready for state-promotion review. |
| `execute-replay/chunks/*.json` | Per-chunk query hash, prepare command, Postgres COPY export evidence, MSSQL bcp load evidence, row counts, and transfer checksum. |
| `executor-config.json` | The exact backend connection and native-tool config used by the run. |

## Outputs

| File | Description |
| --- | --- |
| `route_refresh_execution.json` | Stable schema `dpone.route_refresh_execution.v1` with route identity, plan checksum, mode, chunk results, artifacts, blockers, warnings, and next actions. |
| `route_refresh_execution.md` | Human-readable execution receipt and Runbook. |
| `route_refresh_snapshot_capture.json` | Stable schema `dpone.route_refresh_snapshot_capture.v1` written by `route-refresh-capture-snapshots` after a successful replay or execution receipt. |
| `source_route_refresh_snapshot.json` | Source-side stable schema `dpone.route_refresh_snapshot.v1` with per-chunk row count, boundary, duplicate/null key, and typed hash snapshots. |
| `sink_route_refresh_snapshot.json` | Sink-side stable schema `dpone.route_refresh_snapshot.v1` with the same per-chunk contract for post-load comparison. |
| `route_refresh_verification.json` | Stable schema `dpone.route_refresh_verification.v1` written by `route-refresh-verify` after source/sink post-load reconciliation. |
| `route_refresh_verification.md` | Human-readable verification receipt and state-promotion Runbook. |

Key report fields:

| Field | Meaning |
| --- | --- |
| `mode` | `dry_run` or `execute`. |
| `executed` | Whether a concrete executor actually ran chunks. |
| `status` | `dry_run`, `succeeded`, `partial_failure`, `blocked`, or `approval_required`. |
| `ready_for_state_promotion` | True only after explicit execution succeeded for every chunk. |
| `chunks` | Per-chunk status, row counts, idempotency keys, blockers, and warnings. |
| `plan_sha256` | Checksum of the immutable `route_refresh_plan.json` input. |

## Route refresh snapshot capture

`dpone ops route-refresh-capture-snapshots` is the read-only bridge between a
successful route refresh execution and verification. It reads
`route_refresh_execution.json`, captures source and sink rows for every executed
chunk through injected `RouteRefreshRowsReader` adapters, computes route-aware
typed hashes, and writes the three artifacts that `route-refresh-verify` can
consume directly.

The command does not run heavy tests, does not mutate source or sink data, and
fails closed when the execution receipt is a dry-run, blocked, failed, or
missing file. Reader selection is generic: the CLI can read prebuilt JSON row
fixtures, or use the same backend config as the executor through
`--executor mssql_clickhouse` or `--executor postgres_mssql`.

```bash
uv run dpone ops route-refresh-capture-snapshots \
  --route-refresh-execution-json test_artifacts/live_certification/refresh-executor/mssql-clickhouse/execute-replay/route_refresh_execution.json \
  --runner-id route-refresh-capture \
  --executor mssql_clickhouse \
  --executor-config-json test_artifacts/live_certification/refresh-executor/mssql-clickhouse/executor-config.json \
  --key order_id \
  --boundary-column order_id \
  --column order_id \
  --column status \
  --column amount \
  --type order_id=int \
  --type amount=decimal\(18,2\) \
  --output-dir test_artifacts/live_certification/refresh-executor/mssql-clickhouse/capture \
  --format json
```

For credential-free OSS examples, pass JSON row fixtures instead of live
readers:

```bash
uv run dpone ops route-refresh-capture-snapshots \
  --route-refresh-execution-json test_artifacts/route_refresh_execution/example/route_refresh_execution.json \
  --source-rows-json test_artifacts/route_refresh_execution/example/source_rows.json \
  --sink-rows-json test_artifacts/route_refresh_execution/example/sink_rows.json \
  --runner-id docs-example \
  --key order_id \
  --boundary-column order_id \
  --column order_id \
  --column amount \
  --type amount=decimal\(18,2\) \
  --output-dir test_artifacts/route_refresh_execution/example/capture \
  --format json
```

Artifacts written:

| File | Description |
| --- | --- |
| `route_refresh_snapshot_capture.json` | Capture receipt with route identity, execution checksum, chunk capture status, source/sink snapshot artifact checksums, blockers, warnings, and next actions. |
| `source_route_refresh_snapshot.json` | Source snapshot document keyed by chunk ordinal. |
| `sink_route_refresh_snapshot.json` | Sink snapshot document keyed by chunk ordinal. |
| `route_refresh_snapshot_capture.md` | Human-readable capture receipt and Runbook. |

The Docker-live refresh certification gates for `mssql -> clickhouse` and
`postgres -> mssql` now run snapshot capture against 10,000 rows and 200 columns
per route before verification. They also rerun capture and exact verify after
an additive schema evolution step, so physical-contract conversions such as
integer, bigint, decimal, boolean, UUID/uniqueidentifier, date, time,
datetime/datetime2, binary/varbinary, text, and float are checked with the same
typed hash contract used by release evidence.

## Route refresh verification

`dpone ops route-refresh-verify` is the post-load reconciliation gate for a
succeeded `route_refresh_execution.json`. It does not move data. It reads the
execution receipt, compares source and sink snapshots for every executed chunk,
and writes `route_refresh_verification.json`.

The verification contract checks:

- source and sink row counts;
- chunk min/max boundary values;
- duplicate and null key counters on the sink side;
- route-aware typed hash equality for declared columns and type hints.

Snapshot artifacts use schema `dpone.route_refresh_snapshot.v1`:

```json
{
  "schema_version": "dpone.route_refresh_snapshot.v1",
  "side": "source",
  "route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"},
  "dataset": "analytics.orders",
  "chunks": [
    {
      "ordinal": 1,
      "row_count": 2,
      "min_boundary": "1",
      "max_boundary": "2",
      "typed_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "duplicate_keys": 0,
      "null_keys": 0
    }
  ]
}
```

Run verification from the CLI:

```bash
uv run dpone ops route-refresh-verify \
  --route-refresh-execution-json test_artifacts/live_certification/refresh-executor/mssql-clickhouse/route_refresh_execution.json \
  --source-snapshot-json test_artifacts/live_certification/refresh-executor/mssql-clickhouse/source_route_refresh_snapshot.json \
  --sink-snapshot-json test_artifacts/live_certification/refresh-executor/mssql-clickhouse/sink_route_refresh_snapshot.json \
  --runner-id route-refresh-verifier \
  --key order_id \
  --boundary-column order_id \
  --column order_id \
  --column status \
  --column amount \
  --type order_id=int \
  --type amount=decimal\(18,2\) \
  --output-dir test_artifacts/live_certification/refresh-executor/mssql-clickhouse/verify \
  --format json
```

The JSON report includes `status`, `passed`, `ready_for_state_promotion`,
per-chunk source/sink snapshots, blockers, and an artifact checksum for the
input `route_refresh_execution.json`.

## Safety model

The executor refuses to run when:

| Blocker | Meaning |
| --- | --- |
| `route_refresh_execution.plan_blocked` | The input plan is not `ready`. |
| `route_refresh_execution.plan_approval_required` | The input plan requires approval before execution. |
| `route_refresh_execution.route_mismatch` | Optional `--source`, `--sink`, or `--strategy` does not match the plan. |
| `route_refresh_execution.executor_unavailable` | `--execute` was requested without a configured backend. |
| `route_refresh_execution.chunk_failed:<n>` | Chunk `<n>` failed and later chunks were skipped. |
| `route_refresh_verification.execution_not_succeeded` | Verification was requested for a failed, blocked, dry-run, or missing execution receipt. |
| `route_refresh_snapshot_capture.execution_not_succeeded` | Snapshot capture was requested for a failed, blocked, dry-run, or missing execution receipt. |
| `route_refresh_verification.typed_hash_mismatch:<n>` | Chunk `<n>` has different source/sink typed hashes. |
| `route_refresh_verification.row_count_mismatch:<n>` | Chunk `<n>` has different source/sink row counts. |
| `mssql_clickhouse_refresh_executor.config_missing` | `--executor mssql_clickhouse` was selected without `--executor-config-json`. |
| `mssql_clickhouse_refresh_executor.route_unsupported` | The selected backend does not support the route in the plan. |
| `mssql_clickhouse_refresh_executor.chunk_failed` | Native export or load failed; inspect the per-chunk artifact. |

The service never promotes source state. After a successful execution, write
route execution ledger evidence and route state promotion evidence explicitly.

## Route release gate usage

Attach refresh execution evidence to release candidates that perform replay,
repair, backfill, schema backfill, or retention-gap recovery:

```bash
uv run dpone ops route-release-gate \
  --release v0.10.0-rc2 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact route_refresh_execution=test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/route_refresh_execution.json \
  --artifact route_refresh_snapshot_capture=test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/capture/route_refresh_snapshot_capture.json \
  --artifact route_refresh_verification=test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/verify/route_refresh_verification.json \
  --require route_refresh_execution
```

## Python API

```python
from dpone.ops.route_refresh_execute import RouteRefreshExecutionService

report = RouteRefreshExecutionService().execute(
    route_refresh_plan_json=(
        "test_artifacts/route_refresh/mssql_to_clickhouse/orders/"
        "route_refresh_plan.json"
    ),
    output_dir="test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders",
    runner_id="operator-a",
)
```

To execute real chunks, inject a concrete `RouteRefreshExecutor` implementation
into `RouteRefreshExecutionService(executor=...)`.

For the built-in backend:

```python
from dpone.ops.route_refresh_execute import RouteRefreshExecutionService
from dpone.ops.routes.refresh_executors import (
    MssqlClickHouseRefreshConfig,
    MssqlClickHouseRouteRefreshExecutor,
)

executor = MssqlClickHouseRouteRefreshExecutor.from_config(
    MssqlClickHouseRefreshConfig.from_json("executor-config.json")
)

report = RouteRefreshExecutionService(executor=executor).execute(
    route_refresh_plan_json="test_artifacts/route_refresh/mssql_to_clickhouse/orders/route_refresh_plan.json",
    output_dir="test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders",
    runner_id="route-worker-1",
    execute=True,
)
```

## Runbook

1. Generate `route_refresh_plan.json` and verify that its status is `ready`.
2. Run `dpone ops route-refresh-execute` without `--execute` and review the
   `dry_run` receipt.
3. Capture approval outside the CLI when the route refresh plan requires it,
   then regenerate the plan with approval granted.
4. Execute only through a configured `RouteRefreshExecutor` and explicit
   `--execute`. For the built-in path, use `--executor mssql_clickhouse` and a
   reviewed executor config JSON.
5. If a chunk fails, stop source-state promotion, inspect the chunk blocker,
   and replay only idempotent failed chunks.
6. Run `dpone ops route-refresh-capture-snapshots` and keep
   `route_refresh_snapshot_capture.json`, `source_route_refresh_snapshot.json`,
   and `sink_route_refresh_snapshot.json` with the execution evidence.
7. Run `dpone ops route-refresh-verify` and require
   `route_refresh_verification.json` to pass before state promotion.
8. Attach `route_refresh_execution.json`, `route_refresh_execution.md`,
   `route_refresh_snapshot_capture.json`, `source_route_refresh_snapshot.json`,
   `sink_route_refresh_snapshot.json`, `route_refresh_verification.json`, and
   `route_refresh_verification.md` to route release evidence.
9. Record `route_execution_ledger.json` and `state_promotion.json` only after
   execution and verification are green.

## Related docs

- [Route refresh plan](route-refresh-plan.md)
- [Route execution ledger](route-execution-ledger.md)
- [Route state promotion](route-state-promotion.md)
- [Route release gate](route-release-gate.md)
- [Developer route refresh execute](developer-route-refresh-execute.md)
