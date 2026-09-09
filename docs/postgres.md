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

This fallback is safe and staging-first. A future optimizer can replace it with declarative partition detach/attach when partition metadata is certified.

## Cross-links

- [Load strategies](load-strategies.md)
- [Source -> sink matrix](source-sink-matrix.md)
- [Schema evolution](schema-evolution.md)
- [Type mapping matrix](type-mapping-matrix.md)
- [Postgres XMin deep dive](postgres-xmin.md)
