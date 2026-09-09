# DPONE_WORKLOAD_INIT_OWNERSHIP_CONFLICT

`ownership.yaml` already lists the workload with a different owner than the
`--owner` flag you passed.

## Fix

Pass the existing owner on retry:

```bash
dpone workload init marketing/my_app \
  --source clickhouse --sink mssql --strategy full_refresh \
  --owner existing_team
```

Or edit `ownership.yaml` manually if ownership should change, then rerun init to
confirm idempotency.
