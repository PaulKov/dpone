# BigQuery Integration

`dpone` supports BigQuery as an analytical sink and state backend through the `gcp` extra:

```bash
pip install "dpone[gcp]"
```

## Recommended manifest

```yaml
source:
  type: postgres
  connection_id: postgres_oltp
  connection_type: vault
  table: {schema: public, name: orders}
  options:
    incremental_column: updated_at

sink:
  type: bigquery
  connection_id: bigquery_dwh
  connection_type: vault
  table: {schema: landing, name: orders}
  strategy:
    mode: incremental_merge
    unique_key: [id]
    merge_policy: delete_insert
    duplicate_policy: fail
```

## Load strategy behavior

| Strategy | BigQuery sink behavior |
| --- | --- |
| `full_refresh` | Create/replace via staging and configured exchange path. |
| `incremental_append` | Insert staged rows, optionally insert only missing keys. |
| `incremental_merge` | Default `merge_policy: delete_insert`; optional CTAS `shadow_swap`. |
| `replace` | Delete rows matching `custom_predicate`, then insert staged rows. |
| `partition_replace` | Native time-partition overwrite through partition decorators when possible; partition-column scoped delete/insert fallback otherwise. |

## `incremental_merge` SQL

Default `delete_insert`:

```sql
DELETE FROM `project.landing.orders` AS t
WHERE EXISTS (
    SELECT 1
    FROM `project.staging.orders__tmp` AS s
    WHERE s.id = t.id
);

INSERT INTO `project.landing.orders` (...)
SELECT ... FROM `project.staging.orders__tmp`;
```

Optional `shadow_swap`:

```sql
CREATE TABLE `project.landing.orders__dpone_shadow_ab12cd34` AS
SELECT t.*
FROM `project.landing.orders` AS t
WHERE NOT EXISTS (
    SELECT 1 FROM `project.staging.orders__tmp` AS s WHERE s.id = t.id
);

INSERT INTO `project.landing.orders__dpone_shadow_ab12cd34` (...)
SELECT ... FROM `project.staging.orders__tmp`;

ALTER TABLE `project.landing.orders` RENAME TO orders__dpone_backup_ab12cd34;
ALTER TABLE `project.landing.orders__dpone_shadow_ab12cd34` RENAME TO orders;
DROP TABLE `project.landing.orders__dpone_backup_ab12cd34`;
```

BigQuery table rename can fail for recently streamed tables or restricted metadata states. In that case use the default `delete_insert` policy or rerun after the table is no longer under streaming rename restrictions.

## `partition_replace` SQL

Native time-partition overwrite uses BigQuery query job destination partition decorators and `WRITE_TRUNCATE`:

```sql
-- Runtime submits this SELECT as a BigQuery query job.
-- Destination table: project.landing.orders$20260603
-- Write disposition: WRITE_TRUNCATE
SELECT ...
FROM `project.staging.orders__tmp`
WHERE CAST(`business_date` AS STRING) = @partition_value;
```

Native prerequisites:

1. Target table has BigQuery time partitioning.
2. Staged partition values can be converted to `YYYYMMDD` decorators.
3. The source emits a complete replacement slice for every staged partition value.
4. `native_mode: required` fails before fallback when any partition cannot use the native path.

Fallback SQL:

```sql
DELETE FROM `project.landing.orders`
WHERE CAST(`business_date` AS STRING) IN (
    SELECT DISTINCT CAST(`business_date` AS STRING)
    FROM `project.staging.orders__tmp`
);

INSERT INTO `project.landing.orders` (...)
SELECT ... FROM `project.staging.orders__tmp`;
```

Use `native_mode: fallback` when you intentionally want the safe SQL fallback and do not want query-job partition decorators.

## Cross-links

- [Load strategies](load-strategies.md)
- [Source -> sink matrix](source-sink-matrix.md)
- [Schema evolution](schema-evolution.md)
- [Type mapping matrix](type-mapping-matrix.md)
- [Connections and credentials](connections.md)
