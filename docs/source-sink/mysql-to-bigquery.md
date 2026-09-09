# MySQL -> BigQuery

This guide is a copy/paste-ready starting point for loading data from **MySQL**
into **BigQuery** with `dpone`.

**Status:** Batch ETL supported

CSV wire + `mysql_to_bigquery_analytics_v1` type profile are hermetically
contracted. Vendor-live evidence uses a **wide** typed fixture and covers
Docker MySQL → BigQuery strategies
`full_refresh`, `incremental_append`, `incremental_merge`, `replace`,
`partition_replace`, `snapshot_diff`, `scd2`, and `backfill` via
`tests/integration/mysql/test_mysql_to_bigquery_vendor_live_integration.py`
(billing-enabled GCP project required). File CSV extracts receive
`StrategyMetadataEnricher` hash/SCD2 columns before staging (same path as
every source→sink route). See
[Route live wide certification](../testing/route-live-wide-certification.md).

### Vendor-live IT (manual)

```bash
docker compose -f docker/docker-compose.integration.yml up -d mysql
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
export BIGQUERY_DWH_PROJECT_ID=<gcp-project>
export BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE=/absolute/path/to/sa.json
export DPONE_IT_BQ_DATASET=dpone_it_mysql
uv run pytest tests/integration/mysql/test_mysql_to_bigquery_vendor_live_integration.py -q
```

Never commit the service-account JSON. Missing credentials or
`billingNotEnabled` are reported as SKIP, not PASS.

## When to use this path

Use this path when MySQL (or MariaDB) is the system of record and BigQuery
is the landing, warehouse, event-log, or downstream replication target. v1 uses
watermark/`incremental_column` extract (no binlog CDC).

## Copy/paste manifest

```yaml
# yaml-language-server: $schema=../../src/dpone/schema/etl-batch-manifest.schema.json
kind: dpone.batch.v1

defaults:
  name: mysql_to_bigquery_example
  source:
    type: mysql
    connection_id: mysql_source
    options:
      batch_size: 50000
      incremental_column: updated_at
      export_format: csv
  sink:
    type: bigquery
    connection_id: bigquery_dwh
    table:
      schema: landing
      name: orders
    strategy:
      mode: incremental_merge
      unique_key: order_id
      merge_policy: delete_insert
      duplicate_policy: fail

quality:
  gates:
    - id: source_target_rows
      type: row_count_reconciliation
      severity: error
      tolerance:
        mode: pct
        value: 0.1

schemas:
  app:
    tables:
      - orders
```

Landing-oriented batch authoring:

```bash
dpone plan examples/batch/landing_mysql_to_bigquery.batch.yaml --format md
```

Compact guide example (still valid):

```bash
dpone plan examples/source-sink/mysql-to-bigquery.yaml --format md
dpone run examples/source-sink/mysql-to-bigquery.yaml
```

The checked source file is `examples/source-sink/mysql-to-bigquery.yaml`; CI
compares its parsed YAML with this block. MySQL export defaults to `csv` for
BigQuery sinks; `mssql-delimited` and `clickhouse-tsv` fail closed.

If you change the strategy to `full_refresh` and empty output is invalid,
row-count reconciliation is not enough: it can pass a `0` source / `0` target
comparison. Add an explicit non-empty target gate:

```yaml
quality:
  gates:
    - id: target_min_rows
      type: min_rows
      side: target
      threshold: 1
      severity: error
```

## Supported load strategies

These rows describe public runtime contracts, not certification of this exact
source, sink, transport, schema-evolution mode, and runtime combination.

| Strategy | Status | Notes |
|---|---|---|
| `full_refresh` | Supported | Uses staging first, then applies the target-specific finalization plan. |
| `incremental_append` | Supported | Uses staging first, then applies the target-specific finalization plan. |
| `incremental_merge` | Supported | Default merge policy follows the sink; requires `unique_key`. |
| `replace` | Supported | Uses staging first, then applies the target-specific finalization plan. |
| `partition_replace` | Supported where sink allows | See Load strategies for native/fallback paths. |
| `snapshot_diff` | Supported | Requires a complete bounded snapshot and `unique_key`. |

See [Load strategies](../load-strategies.md) for the detailed algorithm for each strategy.
MySQL binlog CDC is deferred in v1.

## Strategy behavior

- `full_refresh`: extract the selected source boundary, load into staging, and apply the target's safe finalization path.
- `incremental_append`: extract only the incremental boundary and append rows through staging or event production.
- `incremental_merge`: load into staging, validate duplicates, then apply the sink merge policy.
- `replace`: reload a bounded predicate window through staging and then atomically replace the matching target slice.
- `snapshot_diff`: compare a complete current source snapshot with the target by `unique_key`, then apply the configured insert, update, and delete policy.
- `partition_replace`: extract a complete partition slice, load it into staging, and replace only partitions represented by `partition.column`.

Snapshot reconciliation is separate from the load strategy. Runtime planning
reports that capability as `reconciliation.mode=snapshot`; in the official
`dpone.batch.v1` authoring schema, enable it with `reconciliation: true`.

## Runtime algorithm

This sink does not currently implement `StagedLoadPort`, so this route records
`governance_finalization=legacy_post_finalize`. Blocking gates run only after
the sink has mutated or finalized the target. A failure prevents source-state
advancement but cannot roll back that target mutation; inspect and repair or
deduplicate the target before retrying. See
[Load governance](../load-governance.md#runtime-lifecycle).

```mermaid
flowchart TD
    A["Resolve manifest and registry entries"] --> B["Create MySQL source"]
    B --> C["Plan bounded extract"]
    C --> D["Read through streaming SELECT"]
    D --> E["Emit ExtractResult artifact for sink load"]
    E --> F["Plan schema evolution"]
    F --> G["Create sink staging or event batch"]
    G --> H["Load staging / target path"]
    H --> I["Finalize with sink-native strategy"]
    I --> J["Run quality and reconciliation checks (legacy_post_finalize)"]
    J --> K["Advance state only after success"]
```


## Schema evolution and type mapping

Schema evolution is enabled by default and runs before the staging/final load path:

1. Read source schema from `ExtractResult.schema`.
2. Introspect the BigQuery target schema.
3. Apply safe additions and widening operations.
4. Fail breaking changes by default.
5. If configured, route incompatible type changes to `__dpone__nc__<column>`.

Use [Schema evolution](../schema-evolution.md) and [Type mapping matrix](../type-mapping-matrix.md) when adding columns or changing source types.

Pair profile: `mysql_to_bigquery_analytics_v1` (MySQL COLUMN_TYPE → BigQuery
`INT64`/`BOOL`/`STRING`/`BYTES`/`DATE`/`DATETIME`/`TIMESTAMP`/`JSON`/`NUMERIC`/`BIGNUMERIC`).
ENUM/SET/spatial and `BIGINT UNSIGNED` require explicit `schema_contract` and
stay hermetic-only (not in the live wide fixture). Live evidence asserts
schema types plus scalar spot checks for the round-tripable profile columns.

## Self-service golden path

Copy-paste CJM for the checked-in example (wide vendor-live certified route):

```bash
dpone doctor --profile local
pip install "dpone[gcp,mysql]"
dpone plan examples/source-sink/mysql-to-bigquery.yaml --format md
dpone schema type-matrix --source mysql --sink bigquery --format md
dpone run examples/source-sink/mysql-to-bigquery.yaml
```

Landing convention (vault/GitOps-oriented): `examples/batch/landing_mysql_to_bigquery.batch.yaml`.

See [Route live wide certification](../testing/route-live-wide-certification.md) for the maintainer vendor-live IT evidence path (SKIP ≠ PASS).

## Runbook

1. Start with `dpone doctor --profile local` and fix missing extras or native clients.
2. Install `pip install "dpone[mysql,gcp]"`.
3. Run `dpone plan examples/source-sink/mysql-to-bigquery.yaml --format md` and review source boundary, staging path, schema evolution, state, and quality gates.
4. Run a small bounded window first.
5. Inspect the run artifact under `.dpone/runs/mysql_to_bigquery`.
6. For incremental jobs, verify watermark/`incremental_column` before enabling a schedule.
7. Promote the manifest through GitOps after the plan and artifact are reviewed.

## Cross-links

- [Source -> sink matrix](../source-sink-matrix.md)
- [Load strategies](../load-strategies.md)
- [Schema evolution](../schema-evolution.md)
- [Type mapping matrix](../type-mapping-matrix.md)
- [MySQL -> MSSQL](mysql-to-mssql.md)
- [Reconciliation and CDC](../cdc.md)
- [Performance guide](../performance.md)
