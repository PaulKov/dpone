# PostgreSQL Integration

`dpone` supports PostgreSQL as a source, sink, and state backend. Install the optional dependency with:

```bash
pip install "dpone[postgres]"
```

## Recommended manifests

Column cursor source into PostgreSQL sink:

```yaml
source:
  type: postgres
  connection_id: postgres_oltp
  connection_type: vault
  table: {schema: public, name: orders}
  options:
    incremental_strategy: column
    incremental_column: updated_at

sink:
  type: postgres
  connection_id: postgres_dwh
  connection_type: vault
  table: {schema: landing, name: orders}
  strategy:
    mode: incremental_merge
    unique_key: [id]
    merge_policy: delete_insert
    duplicate_policy: fail
```

This target choice is deliberate. PostgreSQL column-cursor extraction is not
supported for an MSSQL sink. The legacy algorithm derives the next checkpoint
from `MAX(incremental_column)` in the target and applies a strict `>` predicate.
That is not a source snapshot boundary: a concurrent transaction can commit a
row with the same finite-precision timestamp after the earlier snapshot, or
with an older timestamp, and the next run would skip it. Manifest validation
returns `POSTGRES_MSSQL_COLUMN_CURSOR_UNSAFE`; programmatic runtime construction
fails with `postgres_mssql.column_cursor_atomic_composite_state_required`
before connector I/O.

For PostgreSQL→MSSQL use XMin, optionally with same-snapshot key
reconciliation for physical deletes. Do not work around the gate with a raw
predicate, precision change, overlap delay, or direct strategy construction.
A future column mode must carry a typed composite `(cursor, tie-breaker...)`
lower and upper bound from one PostgreSQL repeatable-read snapshot and commit
that boundary atomically with the MSSQL target receipt.

Postgres XMin source into any DB sink:

```yaml
source:
  type: postgres
  connection_id: postgres_oltp
  connection_type: vault
  table: {schema: public, name: orders}
  options:
    incremental_strategy: xmin

sink:
  type: mssql
  connection_id: mssql_dwh
  connection_type: vault
  table: {schema: landing, name: orders}
  strategy:
    mode: incremental_merge
    unique_key: [id]
```

See [Postgres XMin deep dive](postgres-xmin.md) for the full algorithm and code-level walkthrough.

## Load strategy behavior

| Strategy | PostgreSQL sink behavior |
| --- | --- |
| `full_refresh` | `TRUNCATE + INSERT` by default, or exchange pattern when configured. |
| `incremental_append` | Append all rows, or insert only missing keys with `only_new_rows: true`. |
| `incremental_merge` | Default `merge_policy: delete_insert`; optional `shadow_swap`. |
| `replace` | Delete rows matching `custom_predicate`, then insert staged rows. |
| `partition_replace` | Native declarative partition detach/attach when possible; partition-column delete/insert fallback otherwise. |

### Refresh an existing table without replacing its structure

For a pre-provisioned target, use this `sink` fragment in your manifest:

```yaml
sink:
  type: postgres
  connection_id: postgres_target
  table: {schema: landing, name: orders}
  strategy:
    mode: full_refresh
    overwrite_type: truncate_insert
```

Omitting `overwrite_type` has the same behavior. Internal queries, files and
in-memory rows first populate owned staging, then run the selected strategy.
Default refresh replaces rows inside the existing table: its logical OID,
constraints, indexes, defaults, grants, triggers and view dependencies remain.
An empty source empties the target. Creating an absent target derives column
names and types; it does **not** copy all source constraints. Provision approved
target DDL before the first run when constraints must be enforced.

The runtime role needs staging schema creation/table privileges and target
`TRUNCATE`/`INSERT` privileges (plus schema-evolution privileges when needed).
Constraint violations reject the insertion; a confirmed rollback restores the
previous committed rows. Incoming foreign keys reject TRUNCATE without CASCADE
or automatic mode switching. TRUNCATE takes an exclusive lock and has MVCC
limitations; plan a load window or configure database lock/statement timeouts.
See [PostgreSQL TRUNCATE](https://www.postgresql.org/docs/16/sql-truncate.html).

Only explicit `overwrite_type: exchange` replaces the database object. Exchange
can discard target metadata and can fail when views or other objects depend on
the target. It has no zero-downtime or dependency-preservation guarantee.
`PostgresSink.load` owns begin/commit/rollback for both full-refresh modes;
PostgreSQL transactional DDL performs rollback without a compensating target DROP.
Direct strategy calls require a caller-owned transaction; use `PostgresSink.load`
for the public Python loading boundary. Append micro-batch commits retain their
separate existing behavior.

Native `partition_replace` holds an exclusive target lock while checking scope.
If a physical partition contains values outside the staged value, or several
staged values map to one child, the existing predicate fallback preserves other
rows. NULL partition values use null-safe predicate replacement, so replay
replaces previous NULL rows. `native_mode: required` rejects such a plan before
replacement.

For run artifacts, independent catalog checks and repair of previously damaged
targets, follow the [Postgres-to-Postgres runbook](source-sink/postgres-to-postgres.md#runbook).

## `incremental_merge` SQL

Default `delete_insert`:

```sql
DELETE FROM landing.orders AS t
WHERE EXISTS (
    SELECT 1
    FROM staging.orders_stg AS s
    WHERE s.id::text = t.id::text
);

INSERT INTO landing.orders (...)
SELECT ... FROM staging.orders_stg;
```

Optional `shadow_swap`:

```sql
CREATE TABLE landing.orders__dpone_shadow_ab12cd34
(LIKE landing.orders INCLUDING ALL);

INSERT INTO landing.orders__dpone_shadow_ab12cd34
SELECT t.*
FROM landing.orders AS t
WHERE NOT EXISTS (
    SELECT 1 FROM staging.orders_stg AS s WHERE s.id::text = t.id::text
);

INSERT INTO landing.orders__dpone_shadow_ab12cd34 (...)
SELECT ... FROM staging.orders_stg;

ALTER TABLE landing.orders RENAME TO orders__dpone_backup_ab12cd34;
ALTER TABLE landing.orders__dpone_shadow_ab12cd34 RENAME TO orders;
DROP TABLE landing.orders__dpone_backup_ab12cd34;
```

## `partition_replace` SQL

Native declarative partition replacement:

```sql
CREATE TABLE landing.orders__dpone_part_ab12cd34
(LIKE landing.orders_20260603 INCLUDING ALL);

INSERT INTO landing.orders__dpone_part_ab12cd34 (...)
SELECT ...
FROM staging.orders_stg
WHERE business_date::text = '2026-06-03';

ALTER TABLE landing.orders
DETACH PARTITION landing.orders_20260603;

ALTER TABLE landing.orders
ATTACH PARTITION landing.orders__dpone_part_ab12cd34
FOR VALUES FROM ('2026-06-03') TO ('2026-06-04');

DROP TABLE landing.orders_20260603;
```

Native prerequisites:

1. Target table uses PostgreSQL declarative partitioning.
2. Existing partition bounds can be resolved for every staged partition value.
3. The source emits a complete replacement slice for each partition value.
4. `native_mode: required` fails before fallback when a partition bound cannot be resolved.

Fallback SQL:

```sql
DELETE FROM landing.orders AS t
WHERE EXISTS (
    SELECT 1
    FROM staging.orders_stg AS s
    WHERE s.business_date::text = t.business_date::text
);

INSERT INTO landing.orders (...)
SELECT ... FROM staging.orders_stg;
```

This fallback stages the input before deleting matching target rows. Native
detach/attach remains available when the partition scope and metadata qualify.

Native replacement holds an exclusive lock while validating scope, counting
old rows, building the replacement and switching partitions. Concurrent readers
of the parent can wait until commit. The exact old-row count adds work
proportional to the affected child; `max_partitions_per_run` limits partition
count, not row count or lock duration. Measure representative row widths,
indexes and concurrent traffic before choosing an operational window.

### Local large-partition measurements

The Docker campaign on source `bef37a7752db43dcae42298da7bd62509a186535`
exercised the real native replacement path with a primary key, CHECK constraint,
128-byte text payload and 1,000 untouched rows in another partition. Each size
ran three times: changed input followed by two replays. The table reports
medians with observed ranges, in seconds.

| Rows replaced | Load | Exclusive lock | Old-child COUNT |
| --- | --- | --- | --- |
| 100,000 | 0.272 (0.233–0.293) | 0.186 (0.165–0.215) | 0.003 (0.003–0.007) |
| 1,000,000 | 1.937 (1.723–2.369) | 1.425 (1.250–1.551) | 0.020 (0.019–0.021) |
| 5,000,000 | 12.059 (11.680–15.395) | 8.139 (7.985–9.435) | 0.143 (0.112–0.166) |

The environment used PostgreSQL 16.15 with durability enabled and an ARM64
Docker VM with 10 CPUs and 7.75 GiB RAM. The 5m-row child occupied about 1.03 GB
including indexes. All nine cases passed row, payload, metric, parent identity,
untouched-partition and staging-cleanup checks. A concurrent reader was observed
waiting on the loader's lock.

These are local observations, not a production latency guarantee. An earlier
series with the same PostgreSQL implementation took 55.391–71.458 seconds at
5m rows, with locks lasting 37.995–47.591 seconds. Cache state and host contention
were uncontrolled; the difference does not establish a code speedup. The COUNT
timing measures that call, not the overall slowdown against a version without it.
For reproduction, raw attempts, source hashes and independent review, see the
[campaign report](https://github.com/PaulKov/dpone/blob/codex/postgres-preservation-reviewed/test_artifacts/postgres-strategy-preservation/performance-review.md).

## Cross-links

- [Load strategies](load-strategies.md)
- [Source -> sink matrix](source-sink-matrix.md)
- [Schema evolution](schema-evolution.md)
- [Type mapping matrix](type-mapping-matrix.md)
- [Postgres XMin deep dive](postgres-xmin.md)
