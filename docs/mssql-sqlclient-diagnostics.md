# MSSQL SqlClient diagnostics

Use JSON output for automation. Stable codes are machine contracts; localized
messages are operator context.

| Observation | Meaning | Action | Escalate when |
|---|---|---|---|
| `DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED` | Logical references have no verified runtime handoff. | Run strict init-fetch, mount its context, and pass the pinned plan variables. | The code remains after the exact context and plan are mounted. |
| `DPONE_RUNTIME_CONNECTION_CONTEXT_UNVERIFIED` | Context and pinned plan are missing or disagree. | Recreate both artifacts from the same deployment release. | Recreated artifacts still disagree. |
| `mssql_sqlclient.*required` or `runtime_unavailable` | Execution image is incomplete. | Follow [installation](mssql-sqlclient-installation.md). | Doctor still fails in the rebuilt image. |
| `mssql_native.live_preflight_required` | Only offline planning ran. | Execute from the qualified deployment runtime. | Runtime cannot complete catalog, permission, or capacity checks. |
| terminal `UNKNOWN` | The writer result was lost or target state is ambiguous. | Run `inspect`, then `reconcile`; do not rerun the source. | Reconciliation cannot obtain exact locks or identity. |
| `PARTIAL_PROVED` | Two stable observations prove an unpublished partial stage. | Persist non-publication, then use the offered `retire` action. | Stage ownership or absence proof changes. |
| `SUCCEEDED` with held custody | Publication succeeded but final cleanup did not. | Use the offered `resume` action. | Receipt or target identity no longer matches. |
| `RETIRED` with held custody | Safe retirement is durable; cleanup remains. | Use the offered `retire` action again. | Exact-owner cleanup cannot be proven. |

The exact first-run diagnostic can be reproduced without connector I/O:

```bash
dpone run examples/native/clickhouse-to-mssql-sqlclient.yaml \
  --interval-end 2026-09-28T00:00:00Z --format json
```

Without the verified runtime context it exits `2`, reports
`DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED`, `attempts: 0`, and
`extracted_rows: 0`.

Recovery commands require the manifest-bound opaque invocation identity shown
by `inspect`. Copy that identity from the local recovery inventory; do not infer
it from table names. See [source-free recovery](mssql-native-recovery.md) for
complete command forms, exit behavior, and cleanup authority.
