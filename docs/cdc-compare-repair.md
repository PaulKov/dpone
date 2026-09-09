# CDC compare and repair

CDC compare and repair proves that a source table and its sink-side CDC current
state are still in sync. It is the post-runtime consistency gate for
replication-grade routes: compare first, write a bounded repair plan, then
execute repair actions only through an injected sink applier.

The first live profile is `mssql -> clickhouse`. It compares the MSSQL source
table with the latest row projection of the ClickHouse CDC log. Repair actions
write new CDC events back into that append-only log and do not mutate CDC
offsets.

## When to use it

Use this command after CDC runtime apply, replay, schema apply, or incident
recovery when you need evidence that source and sink are consistent:

- scheduled route certification;
- post-incident validation after poison replay;
- release promotion before offset handoff;
- targeted repair for missing, extra, or mismatched rows.

## Compare locally

Local mode is credential-free and accepts row JSON fixtures:

```bash
dpone ops cdc-compare-repair \
  --mode local \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --column order_id \
  --column status \
  --source-rows-json test_artifacts/cdc_compare/orders/source_rows.json \
  --target-rows-json test_artifacts/cdc_compare/orders/target_rows.json \
  --output-dir test_artifacts/cdc_compare/orders \
  --format json
```

The command writes `cdc_compare_repair.json`, `cdc_compare_repair.md`, and
`cdc_repair_plan.json`.

## Compare live MSSQL to ClickHouse

Live mode reads the MSSQL source table and compares it with the latest current
state derived from the ClickHouse CDC log:

```bash
dpone ops cdc-compare-repair \
  --mode live \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --column order_id \
  --column status \
  --source-connection-id mssql-prod \
  --sink-connection-id clickhouse-prod \
  --credentials-source env \
  --output-dir .dpone/cdc-compare/orders \
  --format json
```

`--target-dataset` is the ClickHouse CDC log dataset. The reader derives the
current target state with a latest-event window over `dpone_cdc_unique_key_hash`.

## Execute repair

Repair execution takes `cdc_repair_plan.json` and applies replayable actions.
Do not mutate CDC offsets from repair execution. The report always records
`committed=false`; normal bounded runtime ticks remain the only offset-commit
path.

```bash
dpone ops cdc-repair-execute \
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
  --repair-plan-json .dpone/cdc-compare/orders/cdc_repair_plan.json \
  --output-dir .dpone/cdc-compare/orders/repair \
  --format json
```

The command writes `cdc_repair_execution.json` and
`cdc_repair_execution.md`. For ClickHouse, repair uses the same
`ClickHouseCdcSinkApplier` duplicate-event protection as quarantine replay.

## Diff taxonomy

| Diff kind | Meaning | Repair operation |
| --- | --- | --- |
| `missing_in_target` | Source key is absent from the target current state. | `insert` |
| `extra_in_target` | Target key is absent from the source current state. | `delete` |
| `value_mismatch` | Source and target payload hashes differ. | `update` |
| `delete_mismatch` | Source and target delete flags differ. | `update` or `delete` depending on source state. |

## Artifacts

| Artifact | Schema | Purpose |
| --- | --- | --- |
| `cdc_compare_repair.json` | `dpone.cdc_compare_repair.v1` | Compare result, row counts, diff samples, blockers, warnings, metrics, and embedded repair plan. |
| `cdc_compare_repair.md` | Markdown | Human-readable consistency review. |
| `cdc_repair_plan.json` | `dpone.cdc_repair_plan.v1` | Replayable bounded repair actions for missing, extra, and mismatched rows. |
| `cdc_repair_execution.json` | `dpone.cdc_repair_execution.v1` | Repair sink receipt, blocker list, and `committed=false` offset safety flag. |
| `cdc_repair_execution.md` | Markdown | Operator-readable repair execution summary. |

## Runbook

1. Run `dpone ops cdc-compare-repair` before repair.
2. If `cdc_compare_repair.json` is green, keep the report as consistency
   evidence and do not run repair.
3. For `missing_in_target`, verify the source row is valid and the CDC runtime
   window did not skip the key.
4. For `extra_in_target`, verify the source row was deleted or filtered before
   accepting a delete repair.
5. For `value_mismatch`, check schema evolution, type projection, and latest
   event ordering before repair.
6. Review `cdc_repair_plan.json` and limit execution with `--max-actions` when
   doing a small incident repair.
7. Run `dpone ops cdc-repair-execute`.
8. Re-run `dpone ops cdc-compare-repair`.
9. Promote the stream only when compare is green and upstream runtime,
   quarantine, schema, and observability evidence are green.

## Docker-live verification

Default OSS CI stays credential-free. Run the vendor-live Docker check only when
local services are explicitly started and `DPONE_RUN_INTEGRATION=1` is set:

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

The live gate verifies MSSQL source reads, ClickHouse CDC log current-state
projection, compare report generation, repair safety contracts, and existing
runtime offset safety.
