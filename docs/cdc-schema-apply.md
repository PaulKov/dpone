# CDC schema apply

`dpone ops cdc-schema-apply` turns a governed CDC schema-change event into a
target-side DDL dry-run or apply attempt. It sits after
[CDC schema evolution evidence](cdc-schema-evolution-evidence.md) and before
typed serving promotion. The first concrete DDL adapter is ClickHouse for the
`mssql -> clickhouse` CDC typed serving route.

The command is control-plane only. It does not read source CDC, write CDC log
events, or commit offsets.

## Workflow

1. Capture a source schema change and generate schema evolution evidence.
2. Run `dpone ops cdc-schema-apply --mode dry_run`.
3. Review `cdc_schema_apply_plan.json` and `cdc_schema_apply_result.json`.
4. Run `dpone ops cdc-schema-apply --mode apply` only after approval.
5. Use `--typed-refresh` to rebuild the typed serving table and embed
   `cdc_typed_materialization.json` as promotion evidence.

Dry-run example:

```bash
dpone ops cdc-schema-apply \
  --schema-change-json test_artifacts/cdc_schema/mssql_to_clickhouse/orders/schema_change.json \
  --sink clickhouse \
  --target-dataset serving.orders_current_typed \
  --mode dry_run \
  --require-approval \
  --output-dir test_artifacts/cdc_schema_apply/mssql_to_clickhouse/orders \
  --format json
```

Apply example with typed refresh:

```bash
dpone ops cdc-schema-apply \
  --schema-change-json test_artifacts/cdc_schema/mssql_to_clickhouse/orders/schema_change.json \
  --sink clickhouse \
  --target-dataset serving.orders_current_typed \
  --mode apply \
  --sink-connection-id clickhouse-prod \
  --credentials-source env \
  --typed-refresh \
  --cdc-dataset analytics.orders_cdc \
  --unique-key order_id \
  --column order_id=Int32 \
  --column status=Nullable(String) \
  --column amount=Nullable(Decimal(18,2)) \
  --column status_reason=Nullable(String) \
  --fail-on-parse-errors \
  --schema-drift-mode strict \
  --quarantine-dataset serving.orders_parse_quarantine \
  --require-approval \
  --output-dir test_artifacts/cdc_schema_apply/mssql_to_clickhouse/orders \
  --format json
```

## Evidence artifacts

| Artifact | Purpose |
| --- | --- |
| `cdc_schema_apply_plan.json` | Target DDL plan, mapped target type, backfill SQL, blockers, and warnings. |
| `cdc_schema_apply_result.json` | Stable schema `dpone.cdc_schema_apply.v1` with applied status, typed refresh evidence, blockers, and artifact paths. |
| `cdc_schema_apply.md` | Human-readable operator review. |
| `typed_refresh/cdc_typed_materialization.json` | Optional typed serving refresh evidence when `--typed-refresh` is used. |

## Local Docker integration test

The opt-in Docker test covers MSSQL Change Tracking, ClickHouse CDC apply,
typed serving materialization, malformed payload quarantine, and schema apply.

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

## Runbook

| Symptom | Action |
| --- | --- |
| `cdc_schema_apply.unsupported_change` | Use expand-contract migration for drop, rename, or unsafe type changes. |
| `cdc_schema_apply.breaking_change` | Get explicit approval and keep the stream out of offset promotion until governance is green. |
| `cdc_schema_apply.unsupported_type` | Add a source-to-ClickHouse type mapping or route the column through an explicit typed projection. |
| `cdc_schema_apply.typed_refresh_failed` | Open the embedded `typed_refresh/cdc_typed_materialization.json`, fix typed projection or quarantine blockers, and rerun apply. |
| DDL was generated but not executed | Confirm `--mode apply` and `--sink-connection-id`; dry-run mode never mutates ClickHouse. |
