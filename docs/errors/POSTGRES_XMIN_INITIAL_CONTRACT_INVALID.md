# POSTGRES_XMIN_INITIAL_CONTRACT_INVALID

`dpone check` rejected an explicit PostgreSQL XMin initial phase before any
database access.

The initial phase must use `sink.strategy.mode: backfill`, a deterministic
`backfill.chunk`, and one of the two supported publication routes:

- the backward-compatible direct route with `inner_mode: incremental_merge`;
- the resumable publication route with `inner_mode: incremental_append`,
  `publication.mode: shadow_swap`, `publication.retain_backup: true`, and
  `only_new_rows: false`.

Both routes require the durable `audit_schema` state backend with
`require_distributed_lock: true`. They also need a non-empty sink `unique_key`,
`source.options.incremental_strategy: xmin` and a stable
`source.options.xmin_execution.handoff_id`.

Use the complete example in
[PostgreSQL to MSSQL](../source-sink/postgres-to-mssql.md#large-initial-load-and-xmin-handoff).
Do not remove chunking or change the id to bypass a failed campaign; rerun the
same initial process so committed chunks are resumed from their receipts.

After correcting authoring, rerun:

```bash
dpone check <pipeline-directory> --format json
```
