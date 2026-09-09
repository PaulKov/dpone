# Physical design

Physical design controls target-specific DDL: concrete column types,
partitioning, indexes, clustering, table engines, compression, and storage
settings. It complements [schema contracts](schema-contracts.md): contracts are
portable, physical design is sink-specific.

## Configuration

```yaml
sink:
  options:
    physical_design:
      enabled: true
      mode: auto
      apply: online
      apply_runtime: true
      reconciliation:
        mode: block
      columns:
        status:
          target_type:
            clickhouse: LowCardinality(String)
      partitioning:
        strategy: auto
        column: business_date
        granularity: month
      indexes:
        strategy: auto
        primary_key: [order_id]
      storage:
        mssql:
          compression: page
          clustered_columnstore: false
          filegroup: DATA
          textimage_filegroup: LOB
          index_fillfactor: 90
        postgres:
          fillfactor: 90
        clickhouse:
          engine: MergeTree
          partition_by: toYYYYMM(business_date)
          order_by: [business_date, order_id]
          low_cardinality:
            mode: auto
        bigquery:
          partition_by: business_date
          clustering: [customer_id, status]
```

## Apply modes

| Mode | Behavior |
| --- | --- |
| `online` | Apply only low-risk online-safe changes automatically. |
| `safe_window` | Allow planned blocking DDL inside a maintenance window. |
| `plan_only` | Render DDL and risks, do not apply. |
| `manual_approval` | Require approval artifact before execution. |

New target tables can receive the full physical design. Existing targets only
receive online-safe changes automatically. The execution decision is handled by
[Physical DDL apply runtime](physical-ddl-apply.md), keeping planning,
approval, and connector-specific execution testable.

## Existing-table drift reconciliation

`physical_design.reconciliation` controls what happens when an existing target
table differs from the desired `physical_design` contract.

| Mode | Existing-table behavior |
| --- | --- |
| `block` | Default. Detect drift, emit blockers/evidence, and stop before load. |
| `auto_safe` | Apply only target-certified online-safe physical changes, then continue if no blockers remain. |
| `safe_window` | Apply target-certified blocking changes (for example MSSQL compression REBUILD) when approval evidence is attached. |
| `plan_only` | Emit the reconciliation plan and block before load; never execute DDL. |

ClickHouse v1 supports `auto_safe` only for table settings that can be changed
with `ALTER TABLE ... MODIFY SETTING`:
[Table settings manipulations](https://clickhouse.com/docs/sql-reference/statements/alter/setting).
Some valid `CREATE TABLE ... SETTINGS` values, including `index_granularity`,
are create-time physical design choices and are not treated as existing-table
auto-safe changes.
Engine, partition, sorting key, primary key, and physical column-type drift are
blockers because they require a shadow table migration or schema-evolution
workflow. This keeps runtime loads from silently changing storage layout.

Use the read-only CLI before changing production routes:

```bash
dpone schema physical-diff \
  --manifest manifests/orders.yaml \
  --actual actual-clickhouse-physical.json \
  --format json
```

The `actual` JSON uses the same portable shape emitted by target introspection:

```json
{
  "sink_type": "clickhouse",
  "table": "landing.orders",
  "engine": "MergeTree",
  "partition_by": "toYYYYMM(created_at)",
  "order_by": ["created_at", "order_id"],
  "columns": {
    "order_id": {"type": "Nullable(Int64)"}
  },
  "table_settings": {
    "min_rows_for_wide_part": 0
  }
}
```

## Target examples

### MSSQL

```yaml
sink:
  options:
    physical_design:
      indexes:
        primary_key: [order_id]
      storage:
        mssql:
          compression: page
          clustered_columnstore: false
```

Planned SQL shape:

```sql
CREATE TABLE [landing].[orders] (...) WITH (DATA_COMPRESSION = PAGE);
ALTER TABLE [landing].[orders]
  ADD CONSTRAINT [pk_landing_orders_order_id]
  PRIMARY KEY CLUSTERED ([order_id])
  WITH (DATA_COMPRESSION = PAGE);
```

The MSSQL contract is finite and fail-closed. `indexes.primary_key` always
creates a real unique `PRIMARY KEY CLUSTERED` constraint; it is never rendered
as an ordinary index. `index_fillfactor` is valid only with that primary key.
`textimage_filegroup` requires both `filegroup` and a LOB-capable target
column. Clustered columnstore is supported only with `compression: none` and
without `indexes.primary_key`; row/page compression and a rowstore primary key
cannot be silently discarded in favor of columnstore. Unsupported MSSQL
storage/index/partition settings fail during planning, before target mutation.

For a new MSSQL target, `filegroup` and `textimage_filegroup` must use the
exact spelling returned by the target database's `sys.filegroups` catalog and
must identify writable `ROWS_FILEGROUP` entries. dpone reads that authority
before source-row extraction and rejects a missing, read-only, or differently
cased name instead of letting `CREATE TABLE` fail after COPY has started. Exact
spelling is intentional: Python case folding cannot reproduce every SQL Server
database collation, including case-sensitive catalogs with distinct names.

`mode: off`, `enabled: false`, `apply_runtime: false`, `apply: plan_only`, and
`apply: manual_approval` never execute physical-design DDL. Runtime target
creation uses the same typed capability contract as `dpone plan`, so a
shadow/full-refresh path cannot reinterpret or ignore the authored design.

For existing tables, compression changes still require a blocking
`ALTER TABLE ... REBUILD WITH (DATA_COMPRESSION = PAGE)` and therefore
`reconciliation.mode: safe_window` plus explicit approval evidence:

```yaml
physical_design:
  apply_runtime: true
  apply: safe_window
  reconciliation:
    mode: safe_window
    approval:
      approved_by: dba-team
      approved_risks:
        - table_settings.compression
      table: dwh_example.ch.marketing__sample_web_sync
      expires_at: "2026-07-11T21:00:00Z"
  storage:
    mssql:
      compression: page
```

### Postgres

```yaml
sink:
  options:
    physical_design:
      indexes:
        primary_key: [order_id]
      storage:
        postgres:
          fillfactor: 90
```

Planned SQL uses `CREATE INDEX CONCURRENTLY` for existing-table index paths
where possible.

### ClickHouse

```yaml
sink:
  options:
    physical_design:
      storage:
        clickhouse:
          engine: MergeTree
          partition_by: toYYYYMM(business_date)
          order_by: [business_date, order_id]
          low_cardinality:
            mode: auto
            max_distinct_values: 10000
            max_distinct_ratio: 0.05
```

#### Table-level TTL

Table TTL is a first-class physical_design string expression (same shape as
`partition_by`), not a `table_settings` key and not an ALTER post-hook:

```yaml
sink:
  options:
    physical_design:
      storage:
        clickhouse:
          engine: ReplacingMergeTree(updated_at)
          partition_by: toYYYYMM(created_at)
          order_by: [project_execution_id, id]
          ttl: "created_at + toIntervalDay(7)"
          table_settings:
            ttl_only_drop_parts: 1
```

- Pass the expression only — do not include the `TTL` keyword; the renderer
  emits `CREATE TABLE ... ORDER BY ... TTL <expression> SETTINGS ...`.
- `ttl_only_drop_parts` remains a MergeTree setting under `table_settings`.
- `table_ttl` is a deprecated synonym for `ttl`.
- Desired≠actual TTL drift is classified as `shadow_required` (same as
  `partition_by` / `engine`). This release does not apply online
  `ALTER ... MODIFY TTL`.

ClickHouse `ORDER BY` and `PRIMARY KEY` columns should normally be not nullable.
ClickHouse can allow `Nullable(...)` key expressions with the MergeTree
`allow_nullable_key` table setting, but its own MergeTree documentation
describes Nullable key expressions as possible but strongly discouraged and
states that `NULL` values sort with `NULLS_LAST` semantics:
[MergeTree primary keys and indexes](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree#primary-keys-and-indexes-in-queries).
ClickHouse documents `allow_nullable_key` as a MergeTree table setting that can
be supplied in the storage-specific `SETTINGS` clause of `CREATE TABLE`:
[allow_nullable_key](https://clickhouse.com/docs/operations/settings/merge-tree-settings#allow_nullable_key),
[CREATE TABLE clause order](https://clickhouse.com/docs/sql-reference/statements/create/table#comment-clause).

dpone best practice:

1. Prefer `physical_design.storage.clickhouse.nullability.mode:
   non_nullable_by_default` for inferred ClickHouse key columns.
2. Use `allow_nullable_key` only as an explicit compatibility escape hatch when
   preserving nullable key semantics is more important than ClickHouse's
   recommended physical design.
3. Keep table settings separate from insert settings: `allow_nullable_key`
   belongs to target table creation, not `clickhouse_bulk.insert_settings`.

Target table-settings syntax:

```yaml
sink:
  options:
    physical_design:
      storage:
        clickhouse:
          order_by: [optional_code]
          table_settings:
            allow_nullable_key: 1
```

`table_settings` is the storage-specific target table contract. It affects
`CREATE TABLE ... SETTINGS ...`; it does not affect data loading. Insert-time
settings still belong to `sink.options.clickhouse_bulk.insert_settings`, and
inferred type nullability still belongs to
`physical_design.storage.clickhouse.nullability`.

dpone validates table settings before DDL execution:

- setting names must be safe SQL identifiers;
- values must be scalar strings, numbers, integers, or booleans;
- known wrong-context settings fail with an actionable alternative;
- unknown safe ClickHouse settings are allowed with a warning because ClickHouse
  table settings evolve between dpone releases.

Wrong-context example:

```yaml
sink:
  options:
    physical_design:
      storage:
        clickhouse:
          table_settings:
            async_insert: 1
```

This fails because `async_insert` is an insert setting. Use:

```yaml
sink:
  options:
    clickhouse_bulk:
      insert_settings:
        async_insert: 1
```

### BigQuery

```yaml
sink:
  options:
    physical_design:
      storage:
        bigquery:
          partition_by: business_date
          clustering: [customer_id, status]
```

BigQuery physical design is planned as table creation or controlled recreation.
Unsafe changes to existing partitioning/clustering are not applied silently.

## ClickHouse LowCardinality modes

| Mode | Behavior |
| --- | --- |
| `off` | Never generate `LowCardinality`. |
| `auto` | Use profiler cardinality thresholds for string-like columns. |
| `explicit` | Apply only to configured columns. |
| `force` | Apply to configured columns even when profiler disagrees, with warning. |
| `preserve` | Keep existing/source `LowCardinality`, but do not generate new ones. |

Auto mode requires a string-like column and a stable low-cardinality profile:

- `distinct_count <= 10000`
- `distinct_ratio <= 0.05`
- column name does not look like an identifier, UUID, email, URL, hash, or token

`LowCardinality` is certified only for string-like columns by default. Numeric,
date, timestamp, binary and JSON columns are not automatically wrapped. If a
future ClickHouse-specific contract needs non-string `LowCardinality`, add it
as an explicit documented physical override and extend the certification suite
first.

Decision categories in physical plans:

| Example | Category |
| --- | --- |
| `physical_design.columns.amount.target_type.clickhouse: String` | `explicit_physical_override` |
| `schema_contract.columns.amount.type: decimal` | `explicit_logical_contract` |
| Source metadata `numeric(18,4)` -> `Decimal(18,4)` | `auto_inferred` |
| No metadata/sample confidence | `quarantine_required` / safe fallback |

## CLI

```bash
dpone schema physical-plan --manifest manifests/orders.batch.yaml --format md
dpone plan manifests/orders.batch.yaml --selector public.orders --format json
```

## Runbook

| Symptom | Action |
| --- | --- |
| Index/compression DDL is blocked | Switch to `safe_window` or create an approval artifact. |
| ClickHouse query is slow after load | Review `ORDER BY` and partition cardinality. |
| LowCardinality hurts performance | Set `low_cardinality.mode: off` or move the column out of `columns`. |
| BigQuery partition change is required | Use a shadow/recreate plan, then validate downstream permissions. |
| MSSQL table is write-heavy | Prefer row compression or no compression over page compression. |

## Related docs

- [Type inference](type-inference.md)
- [Schema contracts](schema-contracts.md)
- [Physical DDL apply](physical-ddl-apply.md)
- [Online schema evolution](online-schema-evolution.md)
- [Performance](performance.md)
