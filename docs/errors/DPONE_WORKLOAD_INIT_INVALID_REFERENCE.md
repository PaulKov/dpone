# DPONE_WORKLOAD_INIT_INVALID_REFERENCE

The workload reference or domain flags are invalid. Use `DOMAIN/WORKLOAD_ID`
syntax or pass `--domain` when supplying a bare workload id.

## Fix

```bash
dpone workload init marketing/sample_web_sync \
  --source clickhouse --sink mssql --strategy full_refresh
```

Do not mix `--domain` with a conflicting domain prefix in the reference.
