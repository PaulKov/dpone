# CLICKHOUSE_MSSQL_TARGET_MAX_CURSOR_UNSAFE

`dpone check` rejected a ClickHouse-to-MSSQL `incremental_append`
pipeline before it could contact either database.

The current ClickHouse incremental implementation reads
`MAX(incremental_column)` from the target and then selects source rows with a
strict `>` predicate. That value is not a complete source checkpoint: rows
with an equal finite-precision value, late rows with an older value, and rows
whose cursor is `NULL` can remain absent forever. A lookback or `unique_key`
does not make that checkpoint atomic.

Use one of the complete modes supported by your route:

- `full_refresh` for a complete table;
- `replace`, `partition_replace`, or an explicitly bounded backfill when the
  selected window is complete.

Do not silence the error by changing state metadata or by adding a lookback.
After changing the authoring source, rerun:

```bash
dpone check <pipeline-directory> --format json
```

See [ClickHouse to MSSQL](../source-sink/clickhouse-to-mssql.md) for the route
contract and safe alternatives.
