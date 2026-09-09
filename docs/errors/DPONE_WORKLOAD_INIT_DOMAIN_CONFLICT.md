# DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT

The domain catalog already declares the workload id you requested. `dpone
workload init` only adds new workloads; it does not repoint an existing entry.

## Fix

Pick a new workload id or update the domain catalog manually if you intentionally
replace the manifest reference:

```bash
dpone workload init marketing/my_app_v2 \
  --source clickhouse --sink mssql --strategy full_refresh
```

Inspect the current declaration:

```bash
dpone gitops airflow pack \
  --workload-set dpone_workloads/gitops/gitops.yaml \
  --mode plan
```
