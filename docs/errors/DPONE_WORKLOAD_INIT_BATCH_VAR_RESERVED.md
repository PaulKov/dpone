# DPONE_WORKLOAD_INIT_BATCH_VAR_RESERVED

The batch manifest template tried to override a reserved variable such as
`src_schema` or `src_table`. Reserved names are injected by the batch compiler
and cannot appear in table-level `vars`.

## Fix

Remove the reserved override from the batch manifest tables section, or switch to
catalog layout for a single-process manifest:

```bash
dpone workload init marketing/my_app \
  --source clickhouse --sink mssql --strategy full_refresh \
  --layout catalog --apply
```

See [manifest schemas](../reference/manifest-schemas.md) for batch naming rules.
