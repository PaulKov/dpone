# POSTGRES_XMIN_EXECUTION_INVALID

`dpone check` rejected `source.options.xmin_execution` before connector I/O.

The object is closed and requires exactly two fields:

```yaml
xmin_execution:
  mode: initial  # or incremental
  handoff_id: orders_v1
```

`handoff_id` starts with a lower-case ASCII letter and may contain only
lower-case letters, digits, `_`, `.`, and `-`. Both phases must use the same
stable value. Remove unknown fields and do not author `mode: auto`; automatic
compatibility is represented by omitting `xmin_execution` entirely.

See [PostgreSQL to MSSQL](../source-sink/postgres-to-mssql.md#large-initial-load-and-xmin-handoff).
