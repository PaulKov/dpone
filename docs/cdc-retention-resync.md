# CDC retention gap auto-resync

CDC retention gap auto-resync protects replication-grade streams from the
failure mode where the committed CDC offset falls outside source retention. It
checks source retention bounds, writes a fail-closed decision, builds a bounded
snapshot resync plan, and executes that plan through the same idempotent sink
applier used by CDC runtime, replay, and repair.

The first live route is `mssql -> clickhouse`. SQL Server Change Tracking or
SQL Server CDC exposes the retained source window; ClickHouse receives resync
events in the append-only CDC log. Resync execution never mutates CDC offsets.
The operator runbook below is safe for local CI fixtures and opt-in live
incident recovery.

## When to use it

Use this gate before restarting a stalled CDC stream, after a long outage, or
before promoting offsets when retention pressure is visible:

- a worker was down longer than the source retention window;
- the offset store is old or was restored from backup;
- Change Tracking reports a higher min valid version than the committed
  version;
- SQL Server CDC cleanup advanced the min LSN past the stored LSN;
- compare/repair finds missing target rows after an outage and the source
  stream must be rebuilt from a bounded snapshot.

## Check retention locally

Local mode is credential-free and is the right default for CI fixtures:

```bash
dpone ops cdc-retention-check \
  --mode local \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --committed-offset 99 \
  --min-available-offset 100 \
  --high-watermark 150 \
  --current-offset 150 \
  --retention-seconds 172800 \
  --output-dir test_artifacts/cdc_retention_resync/orders/retention \
  --format json
```

The command writes `cdc_retention_check.json` and
`cdc_retention_check.md`.

Decision levels:

| Level | Meaning | Exit code |
| --- | --- | --- |
| `healthy` | The committed offset is inside the retained source interval. | `0` |
| `at_risk` | The committed offset is still valid but close to cleanup. | `0` |
| `gap_detected` | The committed offset is older than the retained source interval. | `1` |

## Check live MSSQL retention

Live mode currently supports `mssql -> clickhouse` source retention probes:

```bash
dpone ops cdc-retention-check \
  --mode live \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --committed-offset 105 \
  --source-connection-id mssql-prod \
  --credentials-source env \
  --output-dir .dpone/cdc-retention/orders \
  --format json
```

For SQL Server CDC use `--backend mssql_cdc` and pass
`--capture-instance` when the capture instance is not the default
`<schema>_<table>`.

## Build a resync plan

When `cdc_retention_check.json` reports `gap_detected`, build a bounded
snapshot plan from source rows:

```bash
dpone ops cdc-resync-plan \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --retention-report-json .dpone/cdc-retention/orders/cdc_retention_check.json \
  --rows-json test_artifacts/cdc_retention_resync/orders/source_snapshot.json \
  --max-rows 10000 \
  --output-dir .dpone/cdc-retention/orders/resync-plan \
  --format json
```

The command writes:

- `cdc_resync_plan.json` - planning report;
- `cdc_resync_plan.md` - operator review;
- `cdc_resync_actions.json` - replayable bounded resync actions consumed by
  execution.

## Execute resync

Execute only after reviewing the bounded plan:

```bash
dpone ops cdc-resync-execute \
  --mode live \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --sink-connection-id clickhouse-prod \
  --credentials-source env \
  --resync-plan-json .dpone/cdc-retention/orders/resync-plan/cdc_resync_actions.json \
  --max-actions 10000 \
  --output-dir .dpone/cdc-retention/orders/resync-execute \
  --format json
```

`cdc_resync_execution.json` always records `committed=false`. Resume CDC only
after resync execution is green and a fresh compare/repair run proves the
current state is aligned.

## Runbook

1. Stop automatic offset promotion for the affected stream.
2. Run `cdc-retention-check` with the last committed offset.
3. If the level is `healthy`, resume the normal bounded CDC runtime.
4. If the level is `at_risk`, increase retention or drain the CDC backlog
   before cleanup reaches the committed offset.
5. If the level is `gap_detected`, run a bounded source snapshot export.
6. Run `cdc-resync-plan` with that source snapshot.
7. Review `cdc_resync_actions.json`; use `--max-rows` or `--max-actions` for
   staged incident recovery.
8. Run `cdc-resync-execute`.
9. Run `cdc-compare-repair` and keep the green report as consistency evidence.
10. Resume `cdc-runtime-run` only after the sink is green and the operator has
    accepted the new checkpoint strategy.

## Docker-live verification

Default OSS CI remains credential-free. Docker-live is opt-in for local route
confidence:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql clickhouse

DPONE_RUN_INTEGRATION=1 \
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=51433 \
DPONE_IT_MSSQL_PASSWORD='Dp0ne.Strong.Pw.2026!' \
DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE=yes \
DPONE_IT_CH_HOST=127.0.0.1 \
DPONE_IT_CH_PORT=59000 \
DPONE_IT_CH_HTTP_PORT=58123 \
DPONE_IT_CH_DATABASE=dpone_it \
DPONE_IT_CH_USER=default \
DPONE_IT_CH_PASSWORD=dpone \
uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
```

The Docker-live gate verifies MSSQL retention probing, ClickHouse resync
execution through the existing sink applier, no offset mutation during resync,
and post-resync compare evidence.
