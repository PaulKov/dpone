# POSTGRES_XMIN_INCREMENTAL_CONTRACT_INVALID

`dpone check` rejected an explicit PostgreSQL XMin incremental phase before
any database access.

The incremental process must use `sink.strategy.mode: incremental_merge` and
typed `reconciliation.mode: key_snapshot` with `enabled: true`. Its
`source.options.xmin_execution.handoff_id` must exactly match the manual
initial process. Runtime additionally requires the deterministic initial seed
receipt; a manifest that validates cannot read source payload until that
receipt exists.

Use the complete example in
[PostgreSQL to MSSQL](../source-sink/postgres-to-mssql.md#large-initial-load-and-xmin-handoff).
Do not seed XMin state manually or point the process at a legacy shared state
table.

After correcting authoring, rerun:

```bash
dpone check <pipeline-directory> --format json
```
