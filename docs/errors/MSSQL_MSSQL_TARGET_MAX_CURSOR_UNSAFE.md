# MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE

`dpone check` rejected an MSSQL-to-MSSQL `incremental_append` or
`incremental_merge` pipeline before it could contact either database.

The current MSSQL source strategy derives its cursor from
`MAX(incremental_column)` in the target and applies a strict `>` source
predicate. Equal finite-precision values, out-of-order commits, older late
rows, and `NULL` values are not represented by that checkpoint and can be
missed permanently. A target-atomic receipt does not make this source boundary
complete.

Use `full_refresh` or another complete bounded replace/backfill mode supported
by the route. Re-enable incremental loading only after a source-owned
composite or CDC checkpoint is committed with the target receipt.

After changing the authoring source, rerun:

```bash
dpone check <pipeline-directory> --format json
```

See [MSSQL to MSSQL](../source-sink/mssql-to-mssql.md) for supported route
modes.
