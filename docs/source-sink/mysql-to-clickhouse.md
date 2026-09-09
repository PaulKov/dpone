# MySQL -> ClickHouse

This guide is a copy/paste-ready starting point for loading data from **MySQL**
into **ClickHouse** with `dpone`.

**Status:** Batch ETL supported

This route uses the MySQL **source family** with ClickHouse-safe **TabSeparated**
export (artifact format `clickhouse-tsv`) into existing ClickHouse staging /
HTTP bulk finalize. Type profile: `mysql_to_clickhouse_analytics_v1`.
Vendor-live evidence uses a **wide** typed fixture and covers Docker MySQL →
ClickHouse strategies `full_refresh`, `incremental_append`, `incremental_merge`,
`replace`, `partition_replace`, `snapshot_diff`, `scd2`, and `backfill` (inner
`replace`) via
`tests/integration/mysql/test_mysql_to_clickhouse_vendor_live_integration.py`.
`snapshot_diff` / `scd2` use staging-first ClickHouse finalizers (default
`lightweight_delete_insert`; see
`docs/feature-design-clickhouse-snapshot-diff-scd2-v1.md`). See
[Route live wide certification](../testing/route-live-wide-certification.md).

### Vendor-live IT (manual)

```bash
docker compose -f docker/docker-compose.integration.yml up -d mysql clickhouse
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv run pytest tests/integration/mysql/test_mysql_to_clickhouse_vendor_live_integration.py -q
```

Public manifests keep `export_format: csv` (schema enum); runtime resolves that
to ClickHouse TabSeparated wire for this pair. Do **not** set
`export_format: mssql-delimited` for this sink (fail-closed at plan and
runtime).

## When to use this path

Use this path when MySQL (or MariaDB) is the system of record and ClickHouse
is the analytics landing target. v1 uses watermark/`incremental_column` extract
(no binlog CDC).

## Copy/paste manifest

```yaml
# yaml-language-server: $schema=../../src/dpone/schema/etl-batch-manifest.schema.json
kind: dpone.batch.v1

defaults:
  name: mysql_to_clickhouse_example
  source:
    type: mysql
    connection_id: mysql_source
    options:
      batch_size: 50000
      incremental_column: updated_at
      export_format: csv
  sink:
    type: clickhouse
    connection_id: clickhouse_analytics
    table:
      schema: analytics
      name: orders
    strategy:
      mode: incremental_merge
      unique_key: order_id
      merge_policy: lightweight_delete_insert
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

Run it locally:

```bash
pip install "dpone[mysql,clickhouse]"
dpone plan examples/source-sink/mysql-to-clickhouse.yaml --format md
dpone run examples/source-sink/mysql-to-clickhouse.yaml
```

Landing-oriented batch example:
`examples/batch/landing_mysql_to_clickhouse.batch.yaml`.

The checked source file is `examples/source-sink/mysql-to-clickhouse.yaml`; CI
compares its parsed YAML with this block.

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

These rows describe public runtime contracts for this Batch ETL route.

| Strategy | Status | Notes |
|---|---|---|
| `full_refresh` | Supported | Staging first, then ClickHouse finalization. |
| `incremental_append` | Supported | Staging first, then ClickHouse finalization. |
| `incremental_merge` | Supported | Default merge policy follows the sink; requires `unique_key`. |
| `replace` | Supported | Staging first, then ClickHouse finalization. |
| `partition_replace` | Supported where sink allows | See Load strategies for native/fallback paths. |
| `backfill` | Supported | Inner mode `replace` via bounded `custom_predicate`. |
| `snapshot_diff` | Supported | Staging-first hard/soft/ignore delete policies; default `lightweight_delete_insert`. |
| `scd2` | Supported | Expire/ignore currents via `ALTER UPDATE`; insert new current versions. |

See [Load strategies](../load-strategies.md) for the detailed algorithm for each strategy.
MySQL binlog CDC is deferred in v1.

## Strategy behavior

- `full_refresh`: extract the selected source boundary, load into staging, and apply the target's safe finalization path.
- `incremental_append`: extract only the incremental boundary and append rows through staging or event production.
- `incremental_merge`: load into staging, validate duplicates, then apply the sink merge policy.
- `replace`: reload a bounded predicate window through staging and then atomically replace the matching target slice.
- `snapshot_diff`: compare a complete current source snapshot with the target by `unique_key`, then apply the configured insert, update, and delete policy.
- `partition_replace`: extract a complete partition slice, load it into staging, and replace only partitions represented by `partition.column`.
- `scd2`: maintain type-2 history with expire/ignore delete policy and current-flag columns.
- `backfill`: reload a bounded predicate window using the configured inner mode (`replace` is vendor-live certified).

Snapshot reconciliation is separate from the load strategy. Runtime planning
reports that capability as `reconciliation.mode=snapshot`; in the official
`dpone.batch.v1` authoring schema, enable it with `reconciliation: true`.

## Runtime algorithm

ClickHouse implements `StagedLoadPort`, so this route records
`governance_finalization=pre_finalize`. Blocking gates evaluate projected
staging rows before target finalization; a failure aborts and cleans staging
without advancing source state. See
[Load governance](../load-governance.md#runtime-lifecycle).

```mermaid
flowchart TD
    A["Resolve manifest and registry entries"] --> B["Create MySQL source"]
    B --> C["Plan bounded extract"]
    C --> D["Stream SELECT to ClickHouse TabSeparated"]
    D --> E["Emit FileExportArtifact format=clickhouse-tsv"]
    E --> F["Plan schema evolution"]
    F --> G["ClickHouse staging / HTTP bulk"]
    G --> H["Run blocking quality gates against staging"]
    H --> I["Apply ClickHouse finalization strategy"]
    I --> J["Advance watermark only after success"]
```

## Schema evolution and type mapping

Schema evolution is enabled by default and runs before the staging/final load path:

1. Read source schema from `ExtractResult.schema`.
2. Introspect the ClickHouse target schema.
3. Apply safe additions and widening operations.
4. Fail breaking changes by default.
5. If configured, route incompatible type changes to `__dpone__nc__<column>`.

Pair profile: **`mysql_to_clickhouse_analytics_v1`**
(`dpone.type_system.source_sink.mysql_clickhouse.MySQLClickHouseTypeMapper`).

Notable mappings:

| MySQL | ClickHouse | Notes |
|---|---|---|
| `tinyint(1)` / `bool` | `Bool` | TSV `0`/`1` |
| `int unsigned` | `UInt32` | Unsigned family |
| `bigint unsigned` | `UInt64` | Unsigned family |
| `datetime` / `timestamp` | `DateTime64(6)` | ClickHouse-safe timestamp text |
| `json` | `String` | JSON text via TSV |
| `blob` / `varbinary` | `String` | Hex text; confirm consumer contract |
| `ENUM` / `SET` / spatial | `String` | Explicit `schema_contract` required |

Nulls use ClickHouse TabSeparated `\N`. Do not feed MSSQL BCP text into this sink.

Use [Schema evolution](../schema-evolution.md) and [Type mapping matrix](../type-mapping-matrix.md)
when adding columns or changing source types.

## Self-service golden path

Copy-paste CJM for the checked-in example (wide vendor-live certified route):

```bash
dpone doctor --profile local
pip install "dpone[mysql,clickhouse]"
dpone plan examples/source-sink/mysql-to-clickhouse.yaml --format md
dpone schema type-matrix --source mysql --sink clickhouse --format md
dpone run examples/source-sink/mysql-to-clickhouse.yaml
```

Landing convention (vault/GitOps-oriented): `examples/batch/landing_mysql_to_clickhouse.batch.yaml`.

See [Route live wide certification](../testing/route-live-wide-certification.md) for the maintainer vendor-live IT evidence path (SKIP ≠ PASS).

## Runbook

1. Start with `dpone doctor --profile local` and fix missing extras or native clients.
2. Install `pip install "dpone[mysql,clickhouse]"`.
3. Run `dpone plan examples/source-sink/mysql-to-clickhouse.yaml --format md` and review source boundary, staging path, schema evolution, state, and quality gates.
4. Confirm `source.options.export_format: csv` (never `mssql-delimited` for ClickHouse).
5. Run a small bounded window first.
6. Inspect the run artifact under `.dpone/runs/mysql_to_clickhouse`.
7. For incremental jobs, verify watermark/`incremental_column` before enabling a schedule.
8. Promote the manifest through GitOps after the plan and artifact are reviewed.

## Cross-links

- [Source -> sink matrix](../source-sink-matrix.md)
- [Load strategies](../load-strategies.md)
- [Schema evolution](../schema-evolution.md)
- [Type mapping matrix](../type-mapping-matrix.md)
- [MySQL -> MSSQL](mysql-to-mssql.md)
- [MySQL -> Postgres](mysql-to-postgres.md)
- [Feature design](../feature-design-mysql-to-clickhouse-batch-v1.md)
- [Reconciliation and CDC](../cdc.md)
- [Performance guide](../performance.md)
