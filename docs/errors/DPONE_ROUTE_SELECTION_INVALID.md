# DPONE_ROUTE_SELECTION_INVALID

The interactive route picker received a value outside the displayed numeric
choices.

Run the command again and choose one of the shown numbers:

```bash
dpone init pipeline --help
```

For automation, avoid the interactive picker and pass an explicit route:

```bash
dpone init pipeline orders_daily \
  --route mssql:clickhouse:incremental_merge
```
