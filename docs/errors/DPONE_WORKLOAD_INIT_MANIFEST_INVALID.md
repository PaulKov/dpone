# DPONE_WORKLOAD_INIT_MANIFEST_INVALID

The generated manifest failed dpone metadata validation after `--apply`. The
scaffold was written, but the command exits failed until the manifest compiles.

## Fix

Read the validation message in CLI output or JSON `errors[0].message`, fix the
listed fields in the manifest path, then rerun:

```bash
dpone workload init marketing/my_app \
  --source clickhouse --sink mssql --strategy full_refresh \
  --apply
```

Prove compile readiness before commit:

```bash
dpone gitops airflow pack \
  --workload-set dpone_workloads/gitops/gitops.yaml \
  --mode plan
```
