# ClickHouse Integration

`dpone` supports ClickHouse as an analytical source and sink. The production
focus is fast append/full-refresh ingest, MSSQL/PostgreSQL file streaming,
bounded upserts, and snapshot reconciliation that avoids mutation-heavy
`ALTER TABLE ... UPDATE` correction paths.

## Install

```bash
pip install "dpone[clickhouse]"
```

For maximum MSSQL -> ClickHouse throughput, prefer ClickHouse HTTP bulk or
`clickhouse-client` bulk ingest instead of Python row parsing. See
[Performance](performance.md) for local 15M benchmark commands.

ClickHouse HTTP and `clickhouse-client` bulk paths are stream-capable sinks.
When a source can produce a bounded byte stream, dpone can send
`INSERT ... FORMAT` payload bytes directly to ClickHouse. For MSSQL
`bcp queryout`, v0.46 adds an optional FIFO route that streams raw
`CustomSeparated` bytes into ClickHouse without first materializing the full
BCP file. File/object-backed physical chunks remain the conservative fallback
when delimiter safety is not certified. The canonical FIFO setting and
migration from the old name are documented in
[MSSQL -> ClickHouse fast ingest](mssql.md#mssql-clickhouse-fast-ingest).

For ClickHouse-to-ClickHouse transforms, use
[SQL file transform workloads](sql-file-workloads.md): `dpone` resolves a
SELECT-only SQL file, stages with `INSERT INTO staging SELECT ...`, projects
lineage, runs quality gates and finalizes atomically without Python row
materialization.

## Staged load governance

ClickHouse bulk/native routes use a governed staged lifecycle:

```text
stage payload -> project __dpone__ lineage -> run quality gates -> validate staged keys -> finalize target -> cleanup
```

This matters for high-throughput routes because files, physical chunks,
RowBinary and Native blocks cannot be enriched row-by-row in Python without
losing throughput. `ClickHouseSinkSideLineageProjector` adds lineage columns
with ClickHouse SQL before target mutation, and `LoadGovernanceFinalizationCoordinator`
runs error-level quality gates against the projected staging table before
`RENAME`, `INSERT SELECT`, merge or partition replacement.

For `incremental_merge`, `snapshot_diff`, and `scd2`, the ClickHouse staged-load
validator then checks every `unique_key` component for NULL and checks the
complete key tuple for duplicate groups on the exact post-lineage table. This
runs after quality gates and before the finalizer's target lookup, target
schema evolution, or data mutation. It fails with
`DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_NULL` or
`DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_DUPLICATE`. The target and checkpoint stay
untouched. On this pre-target failure, cleanup is attempted for every
attempt-local staging table even when an earlier drop fails; cleanup failure
preserves the primary key-integrity error. The failure step records
`operation_tables`, `cleanup_status`, and
`cleanup_verification_required`; operators must use the
[recovery procedure](dbt-self-service-runbook.md#verify-clickhouse-attempt-cleanup)
before retrying.

Each handle is validated exactly once before its native target guard. The
validated finalization path consumes immutable copies of the validated load
configuration and handle, so later caller mutation cannot change the effective
table or key. A sink-owned single-use token rejects forged or replayed
finalization, and the path does not repeat the probe after the guard. Nested
root/child packages validate every member before the first member may finalize,
so an unsafe child key cannot leave an earlier member committed.

Cleanup ownership is explicit: before target invocation, `abort_staged_load`
owns rollback and cleanup exactly once. Normal `cleanup_staged_load` runs after
successful finalization or a post-commit outcome; the coordinator does not run
both operations for the same failed-before-target attempt.

After the target guard is armed, any finalizer exception is
`COMMIT_UNKNOWN`, even when the adapter cannot prove that its first statement
mutated data. Dpone does not abort or clean the staged tables in that state;
they remain available for reconciliation. A nested package reports
`nested_package_partial_finalize` and likewise retains the currently invoked
and not-yet-invoked members with exact operation-table identities in the
failure step. Operators must follow the
[retained-staging reconciliation procedure](dbt-self-service-runbook.md#reconcile-retained-clickhouse-staging)
before cleanup or retry. If target finalization succeeds but cleanup fails,
`staged_cleanup_failed` reports `target_outcome: committed` and
`safe_to_retry: false`; replay is forbidden and only the named attempt tables
may be cleaned.

```yaml
sink:
  type: clickhouse
  options:
    lineage:
      enabled: true
      preset: bulk_standard
      row_identity:
        mode: auto
        unsupported_policy: warn
    load_governance:
      enabled: true
      finalization_phase: pre_finalize
      audit:
        mode: standard
        state_schema: etl_state
        clickhouse:
          engine: auto
      cleanup:
        staging_policy: eager
```

Core lineage columns are always projected for native/bulk loads when lineage is
enabled: `__dpone__run_id`, `__dpone__load_id`, `__dpone__loaded_at` and
`__dpone__extracted_at`. `__dpone__row_id` is projected only with a configured
`unique_key`; otherwise dpone records `row_identity_unique_key_missing` or
fails when `unsupported_policy: fail`.

For governed ClickHouse loads, dpone creates and writes ClickHouse-backed audit
tables by default:

- `etl_state.__dpone__loads`
- `etl_state.__dpone__load_steps`

`__dpone__load_steps.details_json` contains machine-readable step evidence such
as staged rows, schema width, lineage projection evidence, quality gate results
and finalization counters. These tables are runtime-owned OSS contracts.
Airflow pack/KPO DAGs consume the same behavior as CLI, Python API, Docker and
local runs.

For clustered ClickHouse targets, `engine: auto` renders audit/state DDL with
`ON CLUSTER` and `ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', __dpone__loaded_at)`.
For single-node targets it keeps the existing local audit layout. Use explicit
`MergeTree`, `ReplacingMergeTree` or `ReplicatedReplacingMergeTree` only when a
platform owner has a reason to override the default.

## MSSQL -> ClickHouse delimiter safety

Delimiter-based MSSQL -> ClickHouse routes can be very fast, but they must keep
row and column boundaries certified. Legacy `source_encoded` routes export with
SQL Server `bcp queryout` and stream into ClickHouse with
`INSERT ... FORMAT TabSeparated`. `TabSeparated` is delimiter-based, so raw
MSSQL text containing tabs or newlines is unsafe.

When the MSSQL source sees a ClickHouse sink using direct client/HTTP bulk mode,
`dpone` wraps exported columns in SQL expressions that produce ClickHouse-safe
values:

| MSSQL source value | File value for ClickHouse |
| --- | --- |
| `NULL` | `\\N` |
| Backslash | `\\\\` |
| Tab | `\\t` |
| LF newline | `\\n` |
| CR newline | `\\r` |

This avoids Python row parsing while preserving row and column boundaries for
large exports. If `clickhouse_bulk.mode` is set to `python`, `driver`, or
`native_driver`, dpone does not apply the direct TSV wrapper and the Python file
reader path is used instead.

The v0.46 `typed_raw_streaming_staging` route uses `CustomSeparated` control
delimiters and does not inject MSSQL-side ClickHouse `REPLACE(...)` escaping.
It is fail-closed under `delimiter_safety: certified_only` until delimiter
safety is proven; benchmark/advisory runs must use explicit count/hash
reconciliation.

## MSSQL exact type fidelity

For MSSQL sources, ClickHouse table creation uses a lossless-first mapper:

- `decimal(p,s)` and `numeric(p,s)` become `Decimal(p,s)`, not `Float64`.
- `money` and `smallmoney` become fixed-scale `Decimal`.
- `uniqueidentifier` becomes `UUID`.
- `datetime` becomes `DateTime64(3)`, `datetime2(p)` becomes `DateTime64(p)`, and `smalldatetime` becomes `DateTime64(0)`.
- MSSQL timezone-naive timestamps can use `type_fidelity.temporal.naive_timestamp.transfer_encoding: epoch` as a native transport encoding while still landing as ClickHouse `DateTime64(p)`.
- `datetimeoffset(p)` uses `type_fidelity.temporal.offset_timestamp`; default `utc_instant` becomes `DateTime64(p, 'UTC')`, while `fixed_timezone`, `preserve_offset` and `preserve_text` are explicit opt-ins.
- `binary`, `varbinary`, `rowversion` land as `String` unless an explicit byte codec policy is configured.
- `time(p)` lands as `String` by default, or as `UInt32` seconds since midnight when explicitly configured.

Use `dpone plan` to inspect the `type_fidelity` section before first production load. For certification, prefer the MSSQL -> ClickHouse `typed_hash` reconciliation profile because it compares typed values instead of connector-specific JSON formatting.

```yaml
source:
  type: mssql
  options:
    type_fidelity:
      binary_encoding: hex
      time_encoding: seconds_since_midnight
      temporal:
        offset_timestamp:
          mode: utc_instant
          timezone: UTC
```

See [MSSQL -> ClickHouse](source-sink/mssql-to-clickhouse.md#binary-and-time-codec-policy) for policy values, tradeoffs and the typed reconciliation runbook.

## Incremental merge policies

ClickHouse `merge_policy: auto` resolves to `lightweight_delete_insert`.

```yaml
sink:
  type: clickhouse
  table: {schema: analytics, name: orders}
  strategy:
    mode: incremental_merge
    unique_key: [id]
    merge_policy: lightweight_delete_insert
    duplicate_policy: fail
    mutations_sync: 1
```

Default SQL shape:

```sql
DELETE FROM analytics.orders
WHERE id IN (SELECT id FROM analytics.orders__dpone_staging_ab12cd34)
SETTINGS mutations_sync = 1;

INSERT INTO analytics.orders
SELECT * FROM analytics.orders__dpone_staging_ab12cd34;
```

Supported policies:

| Policy | Status | Notes |
| --- | --- | --- |
| `lightweight_delete_insert` | Default | Fast bounded upsert; ClickHouse performs logical deletes and cleans data asynchronously. |
| `shadow_swap` | Supported | Rebuilds a full shadow table and swaps it into the canonical name; heavier but reader-friendly. |
| `mutation_delete_insert` | Non-recommended opt-in | Uses `ALTER TABLE ... DELETE`; requires `allow_non_recommended_policy: true`. |

Non-recommended mutation opt-in:

```yaml
sink:
  strategy:
    mode: incremental_merge
    unique_key: [id]
    merge_policy: mutation_delete_insert
    allow_non_recommended_policy: true
    mutations_sync: 2
```

## Partition replace

Use `partition_replace` when the source provides a complete replacement slice
for one or more ClickHouse partitions.

```yaml
sink:
  type: clickhouse
  strategy:
    mode: partition_replace
    partition:
      column: business_date
      values_from_staging: true
      max_partitions_per_run: 32
```

Runtime shape:

```sql
CREATE TABLE analytics.orders__dpone_staging_ab12cd34 AS analytics.orders;

INSERT INTO analytics.orders__dpone_staging_ab12cd34
SELECT ...;

ALTER TABLE analytics.orders
REPLACE PARTITION '2026-06-03'
FROM analytics.orders__dpone_staging_ab12cd34;
```

The staging table is created from target metadata so partition expressions and
engines remain compatible for `REPLACE PARTITION ... FROM`.

## Cluster and access-table DDL

ClickHouse cluster topology is part of the same physical design contract as
engine, partitioning, sorting key, and table settings. Use
`physical_design.storage.clickhouse.cluster: <name>` when dpone should render
cluster-wide DDL with `ON CLUSTER`, and use `access_table` only when you need a
separate read/query facade such as `Distributed`.

```yaml
sink:
  type: clickhouse
  table:
    schema: Example_Datamarts
    name: orders_local
  options:
    physical_design:
      storage:
        clickhouse:
          engine: "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
          cluster: dwh
          order_by:
            - order_date
            - customer_id
            - order_id
```

The generated DDL creates both database and table across the configured
ClickHouse cluster:

```sql
CREATE DATABASE IF NOT EXISTS `Example_Datamarts` ON CLUSTER `dwh`;

CREATE TABLE `Example_Datamarts`.`orders_local` ON CLUSTER `dwh` (...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (`order_date`, `customer_id`, `order_id`);
```

For true multi-shard read fan-out, add an explicit access table:

```yaml
sink:
  options:
    physical_design:
      storage:
        clickhouse:
          cluster: dwh
          access_table:
            name: orders_all
            engine: Distributed
            sharding_key: cityHash64(order_id)
```

This produces a second DDL statement:

```sql
CREATE TABLE IF NOT EXISTS `Example_Datamarts`.`orders_all` ON CLUSTER `dwh`
AS `Example_Datamarts`.`orders_local`
ENGINE = Distributed('dwh', 'Example_Datamarts', 'orders_local', cityHash64(order_id));
```

Do not create a `Distributed` facade just because a table is replicated. On a
single-shard cluster with several replicas, direct `ReplicatedMergeTree` access
is usually simpler and avoids an unnecessary write/read routing layer.
`Distributed` is for a real multi-shard access path or an explicit compatibility
facade.

When a deployment creates local DDL manually but still needs a generated
`Distributed` facade, use the explicit object form:

```yaml
sink:
  options:
    physical_design:
      storage:
        clickhouse:
          cluster:
            name: dwh
            ddl_scope: local
          access_table:
            name: orders_all
            engine: Distributed
            sharding_key: cityHash64(order_id)
```

Legacy manifests with `cluster: {name: dwh, on_cluster: true}` remain accepted,
but new manifests should prefer the string shorthand or `ddl_scope`.

### Cluster topology preflight

When `physical_design.storage.clickhouse.cluster` is enabled, dpone checks the
target topology before source extraction starts. This protects weak-worker
routes from spending minutes exporting data only to fail during finalization.

The preflight reads `system.clusters` and `clusterAllReplicas(..., system.tables)`
and blocks when an existing target table is only present on part of the cluster
or when replicas disagree on the table UUID/engine. The decision is emitted as
`decision_id=clickhouse.cluster_target_topology` in runtime decision audit and,
for governed loads, in `__dpone__load_steps.details_json`.

Common blocker codes:

| Blocker | Meaning | Remediation |
| --- | --- | --- |
| `clickhouse_cluster_target_missing_replicas` | The target exists on only some cluster hosts. | Recreate or repair the table with the same UUID and `ON CLUSTER`, then rerun. |
| `clickhouse_cluster_target_uuid_mismatch` | Hosts have tables with the same name but different UUIDs. | Stop using the target until DDL is reconciled; different UUIDs mean different replicated paths when `{uuid}` is used. |
| `clickhouse_cluster_target_engine_mismatch` | Hosts disagree on the engine or physical layout. | Reconcile physical DDL before loading; finalization would be inconsistent. |
| `clickhouse_cluster_not_found` | The configured cluster name is absent from `system.clusters`. | Fix `physical_design.storage.clickhouse.cluster` or ClickHouse cluster config. |

For `ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')`,
repairing a partial table must preserve the UUID of the healthy table. A safe
manual repair shape is:

```sql
CREATE TABLE IF NOT EXISTS mart.orders
UUID 'existing-table-uuid'
ON CLUSTER dwh
(...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (...);
```

Do not create the missing replicas without preserving the UUID when the engine
path contains `{uuid}`; that creates independent replicated tables with the
same name rather than one consistent table.

## Physical design drift

When `sink.options.physical_design.apply_runtime: true`, existing ClickHouse
targets are checked against the desired `physical_design` contract before load.
The default reconciliation mode is safe:

```yaml
sink:
  options:
    physical_design:
      apply_runtime: true
      reconciliation:
        mode: block
```

Use `mode: auto_safe` only when the route is allowed to apply ClickHouse
table-setting drift. In v1, dpone renders only
`ALTER TABLE ... MODIFY SETTING` for online-mutable settings such as
`min_rows_for_wide_part`. Create-time settings such as `index_granularity`
remain valid `CREATE TABLE ... SETTINGS` options, but are not treated as
existing-table auto-safe changes.
Engine, partition key, sorting key, primary key, and physical column type drift
remain blockers and should be handled with schema evolution, expand-contract, or
a shadow table migration. ClickHouse documents table setting changes through
[`ALTER TABLE ... MODIFY|RESET SETTING`](https://clickhouse.com/docs/sql-reference/statements/alter/setting);
the broader [column ALTER](https://clickhouse.com/docs/sql-reference/statements/alter/column)
surface is not used by dpone for automatic layout migration.

## Physical deletes without update mutations

ClickHouse implements `ALTER TABLE ... UPDATE` and classic `ALTER TABLE ... DELETE`
as table mutations. Mutations are asynchronous, can accumulate in
`system.mutations`, and make reconciliation SLOs hard to reason about. For this
reason, dpone does **not** use `ALTER TABLE ... UPDATE` for ClickHouse snapshot
reconciliation.

The default ClickHouse reconciliation strategy is `shadow_table_swap`:

1. Stage disappeared source keys in a temporary `Memory` table.
2. Create an empty shadow table with the same structure as the target table.
3. Copy all target rows into the shadow table.
4. For rows matching staged deleted keys, set `__dpone__deleted_at = now()` and
   refresh `__dpone__loaded_at = now()`.
5. Rename the original table to a backup and the shadow table to the canonical
   target name.
6. Drop temporary artifacts.

This keeps raw target-table semantics correct without ClickHouse mutations. It is
heavier than an append-only tombstone strategy because it rewrites the table, but
it is deterministic and avoids mutation backlog.

## Staging-first load strategies

ClickHouse sink loads data into staging tables before touching the canonical
target table:

- `FULL_REFRESH`: load into staging, then rename staging into the target name.
- `INCREMENTAL_APPEND`: load into staging, then append from staging into target.
- `INCREMENTAL_MERGE`: load into staging, reject NULL key components and
  duplicate key groups on the exact post-lineage table before finalizer target
  lookup or mutation, then finalize with `lightweight_delete_insert`,
  `shadow_swap`, or guarded
  `mutation_delete_insert`.
- `REPLACE`: load into staging, copy non-replaced target rows into a shadow table,
  append staging rows into the shadow table, then rename the shadow table into
  the target name.
- `PARTITION_REPLACE`: create staging from target metadata, load replacement rows,
  then run `ALTER TABLE target REPLACE PARTITION ... FROM staging` for each staged
  partition value.

The sink does not use target `TRUNCATE`. `mutation_delete_insert` is available
only as an explicit, non-recommended opt-in.

The same post-lineage, pre-finalizer NULL and duplicate key gate applies to
`snapshot_diff` and `scd2`. A rejected attempt leaves the target and checkpoint
untouched and attempts cleanup of every attempt-local staging table. If cleanup
itself fails, dpone preserves the original integrity error; verify and remove
only the evidence-identified attempt tables before retrying.

By default, ClickHouse runtime artifacts are created next to the target table
for backward compatibility. To keep business schemas clean, set an explicit
technical staging schema. dpone will create staging, decoded, projected, shadow
and backup tables there, while the final target stays in `sink.table.schema`.

```yaml
sink:
  type: clickhouse
  table:
    schema: mart
    name: orders
  staging:
    schema: ops
  options:
    load_governance:
      audit:
        enabled: true
        state_schema: ops
        clickhouse:
          engine: auto
```

When `physical_design.storage.clickhouse.cluster` is configured, dpone renders
managed-artifact `CREATE`, `DROP`, `RENAME` and `REPLACE PARTITION` DDL with
`ON CLUSTER` so cleanup does not leave tables on individual replicas. The same
cluster setting also applies to ClickHouse audit/state tables when
`load_governance.audit.clickhouse.engine` is `auto` or a replicated engine.

## Schema evolution

ClickHouse sink applies safe schema evolution before staging:

- `ADD COLUMN` for new nullable/default-safe columns;
- `MODIFY COLUMN` only for planner-approved widening;
- `__dpone__nc__<column>` generated columns for explicit incompatible type-change
  routing.

Schema evolution does not use ClickHouse mutations. Full refresh, replace, and
reconciliation still use staging/shadow table swaps. See
[Schema evolution](schema-evolution.md).

## LowCardinality and physical design

Use [Physical design](physical-design.md) to control ClickHouse engines,
`PARTITION BY`, `ORDER BY`, codecs, and `LowCardinality` behavior.

```yaml
sink:
  type: clickhouse
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

Modes:

| Mode | Behavior |
| --- | --- |
| `off` | Never generate `LowCardinality`. |
| `auto` | Use profiler cardinality thresholds for string-like columns. |
| `explicit` | Apply only to configured columns. |
| `force` | Apply to configured columns even when profiler disagrees, with warning. |
| `preserve` | Keep existing/source `LowCardinality`, but do not generate new ones. |

Runbook:

1. If low-cardinality columns are not selected, inspect `dpone schema infer` and verify sample coverage.
2. If high-cardinality columns were selected, switch to `explicit` or `off`.
3. If downstream DDL must be exact, use `physical_design.columns.<column>.target_type.clickhouse`.
4. Run `dpone schema physical-plan --manifest ... --format md` before changing existing tables.

## Nullable sorting keys and `allow_nullable_key`

ClickHouse can create MergeTree tables where a `Nullable(...)` expression is
used in `PRIMARY KEY` or `ORDER BY`, but this is an escape hatch, not the
recommended physical design. The ClickHouse MergeTree guide says Nullable key
expressions are possible only when `allow_nullable_key` is enabled, and the same
section explicitly discourages this pattern. It also documents that `NULL`
values in `ORDER BY` follow `NULLS_LAST` ordering:
[MergeTree primary keys and indexes](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree#primary-keys-and-indexes-in-queries).

`allow_nullable_key` is a MergeTree table setting. ClickHouse documents that
MergeTree settings can be defined per table in the `SETTINGS` clause of
`CREATE TABLE`, and the `allow_nullable_key` setting specifically allows
Nullable types as primary keys:
[MergeTree table settings](https://clickhouse.com/docs/operations/settings/merge-tree-settings#allow_nullable_key).
The ClickHouse `CREATE TABLE` docs also clarify that storage-specific settings
belong with the storage clauses, before query-level settings:
[CREATE TABLE](https://clickhouse.com/docs/sql-reference/statements/create/table#comment-clause).

Best practice in dpone:

1. Prefer not-null ClickHouse key columns. For MSSQL -> ClickHouse, use
   `physical_design.storage.clickhouse.nullability.mode: non_nullable_by_default`
   for inferred target types that participate in `ORDER BY`.
2. Keep `allow_nullable_key` only for cases where preserving source nullability
   in the key is a deliberate compatibility decision.
3. Do not put `allow_nullable_key` into `clickhouse_bulk.insert_settings`; insert
   settings affect loading, while `allow_nullable_key` affects table creation.
4. Run `dpone schema physical-plan --manifest ... --format md` and review the
   final `CREATE TABLE` before applying the design.

Use this explicit target table-settings escape hatch only when nullable key
semantics are intentional:

```yaml
sink:
  type: clickhouse
  options:
    physical_design:
      storage:
        clickhouse:
          order_by: [optional_code]
          table_settings:
            allow_nullable_key: 1
```

The generated DDL contains the storage clause:

```sql
CREATE TABLE `landing`.`orders` (`optional_code` Nullable(String))
ENGINE = MergeTree
ORDER BY (`optional_code`)
SETTINGS allow_nullable_key = 1
```

## Requirements

- Reconciliation must be enabled and configured with a stable `unique_key`.
- The ClickHouse target table must contain `__dpone__loaded_at` and
  `__dpone__deleted_at`.
- The connector must be able to read `system.columns` for target schema discovery.
- The target database should support atomic `RENAME TABLE`; ClickHouse `Atomic`
  databases do.

## Example

```yaml
source:
  type: mssql
  connection_id: mssql_source
  table: {schema: dbo, name: orders}

sink:
  type: clickhouse
  connection_id: clickhouse_dwh
  table: {schema: analytics, name: orders}
  strategy:
    mode: incremental_append
    unique_key: [id]
  reconciliation:
    enabled: true
```

## Operational guidance

- Use ClickHouse reconciliation for periodic correctness gates, not per-minute
  high-churn delete streams.
- For high-delete workloads, prefer CDC/Change Tracking into a versioned
  table design or run reconciliation by partition.
- Keep readers on the canonical table name; dpone swaps the shadow table into
  that name only after the full rewrite has completed.
- If a run fails before the swap, dpone drops the shadow table and leaves the
  canonical target unchanged.

## Typed bulk wire ingest for MSSQL native transfers

MSSQL -> ClickHouse native transfer can avoid expensive MSSQL-side TSV
escaping by using typed bulk wire. For large certified schemas, the preferred
route is `typed_binary` with `source_native_format: bcp_native` and
`binary_format: native`: MSSQL exports with `bcp queryout -n`, dpone decodes
the source-native binary artifact, and ClickHouse loads `Native` columnar
blocks into staging before finalization.

Recommended options:

```yaml
source:
  options:
    extract_mode: bcp_queryout
    bulk:
      mode: bcp
      bcp:
        file_format: native
    native_transfer:
      wire:
        mode: typed_binary
        source_native_format: bcp_native
        binary_format: native
        block_rows: 65536
        block_bytes: 64MiB
        acceleration:
          mode: auto
sink:
  options:
    clickhouse_bulk:
      mode: native_tcp
      native_tcp:
        enabled: true
        backend: auto
        compression: auto
        port: 9000
        connection_pool_size: 2
        query_timeout_seconds: 3600
      ingest_contract: typed_binary_staging
      insert_settings:
        async_insert: 1
        wait_for_async_insert: 1
        max_insert_block_size: 1000000
```

`acceleration.mode: auto` uses the optional `dpone[accel]` fused provider only
when the route, schema, and platform are certified. Set `acceleration.mode:
required` for release routes that must fail before source IO if the provider is
missing or unsupported. Set `acceleration.mode: off` for reference-path
debugging and differential tests.

For v0.33, `native_tcp.backend: auto` records a second-level protocol decision:
`direct` when the certified `dpone-native-accel` provider is installed,
otherwise `client` with an explicit fallback reason. The direct backend sends
ClickHouse Native blocks through the Native TCP protocol without a
`clickhouse-client` subprocess. `backend: direct` is fail-closed and blocks
before MSSQL export if the provider is unavailable, unsupported, or uncertified.

When BCP native is not certified for a source type, use
`source_native_format: odbc_row_stream` with `mssql_export_mode: row_stream`.
That fallback still avoids MSSQL-side `REPLACE(...)` projections, but the source
rows are read through ODBC and then encoded as RowBinary by dpone. Set
`binary_format: rowbinary` when you need the v0.26 row-oriented path for
compatibility or benchmark comparison.

`typed_raw` with `delimiter_profile: ascii_control` remains available for
benchmarks and certified schemas where delimiter, quote and NULL safety is
already proven. If a generated MSSQL query contains `REPLACE(`, the route is the
legacy `source_encoded` text path and should not be treated as the default
production route for large view-backed loads.

If `async_insert: 1` is set without `wait_for_async_insert`, dpone
automatically sets `wait_for_async_insert: 1` so the source state is not
advanced before ClickHouse accepts the insert.
