# DPONE_WORKLOAD_INIT_SCAFFOLD_CONFLICT

`dpone workload init` found an existing scaffold file with different content.
The command never overwrites user-owned files.

## Fix

Review the diff in `--format json` output or rerun with `--format json` to see
the unified diff for the conflicting path. Either:

1. rename the workload (`DOMAIN/NEW_ID`) and rerun plan-first init; or
2. merge your local edits manually, then rerun init to confirm `no_op`.

```bash
dpone workload init marketing/my_app \
  --source clickhouse --sink mssql --strategy full_refresh \
  --format json
```

After manual merge:

```bash
dpone workload init marketing/my_app \
  --source clickhouse --sink mssql --strategy full_refresh \
  --apply
```
