# POSTGRES_MSSQL_COLUMN_CURSOR_UNSAFE

`dpone check` rejected a PostgreSQL-to-MSSQL column-cursor incremental
pipeline before it could contact either database.

The column strategy derives its cursor from `MAX(incremental_column)` in the
target and applies a strict `>` source predicate. A single finite-precision
value is not a complete PostgreSQL snapshot boundary: concurrent rows may have
the same value, and older late or `NULL` values can be omitted permanently.

For PostgreSQL incremental loading, select the source-owned XMin strategy and
remove the column cursor:

```yaml
source:
  type: postgres
  options:
    incremental_strategy: xmin
```

Do not combine `incremental_strategy: xmin` with `incremental_column`. The
certified MSSQL `incremental_merge` route also requires a sink `unique_key`,
same-source `reconciliation.mode: key_snapshot`, and externally provisioned
target-atomic MSSQL state; use the complete copy/paste example linked below.
If XMin is not appropriate for the workload, use a complete full or bounded
replace mode supported by the route.

After changing the authoring source, rerun:

```bash
dpone check <pipeline-directory> --format json
```

See [PostgreSQL to MSSQL](../source-sink/postgres-to-mssql.md) and
[PostgreSQL XMin](../postgres-xmin.md) for the checkpoint contract.
