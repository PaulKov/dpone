# POSTGRES_XMIN_HANDOFF_ROUTE_UNSUPPORTED

The explicit XMin initial/incremental handoff is certified only for a
PostgreSQL source and an MSSQL sink. `dpone check` rejected another endpoint
pair before connector I/O.

Use:

```yaml
source:
  type: postgres
  options:
    incremental_strategy: xmin
    xmin_execution: {mode: initial, handoff_id: orders_v1}
sink:
  type: mssql
```

For another route, omit `xmin_execution` and select a strategy that is
explicitly supported by that source/sink capability contract. Do not spoof
endpoint names to pass validation.

See [PostgreSQL to MSSQL](../source-sink/postgres-to-mssql.md#large-initial-load-and-xmin-handoff).
