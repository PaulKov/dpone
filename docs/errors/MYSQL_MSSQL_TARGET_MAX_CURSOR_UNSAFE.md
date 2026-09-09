# MYSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE

`dpone check` rejected a MySQL-to-MSSQL `incremental_append` or
`incremental_merge` pipeline before it could contact either database.

The current MySQL incremental implementation reads
`MAX(incremental_column)` from the target and then selects source rows with a
strict `>` predicate. This can permanently omit equal-precision values,
out-of-order commits, older late rows, and `NULL` cursor values. The MSSQL
receipt can make the target mutation atomic, but it cannot reconstruct rows
that were outside that source predicate.

Use `full_refresh` or another complete bounded replace/backfill mode supported
by the route. Re-enable incremental loading only when a source-owned composite
or binlog checkpoint can be committed with the target receipt.

After changing the authoring source, rerun:

```bash
dpone check <pipeline-directory> --format json
```

See [MySQL to MSSQL](../source-sink/mysql-to-mssql.md) for supported route
modes.
