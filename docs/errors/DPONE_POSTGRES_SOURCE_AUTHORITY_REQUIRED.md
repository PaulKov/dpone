# `DPONE_POSTGRES_SOURCE_AUTHORITY_REQUIRED`

`dpone check --connections` found a strict PostgreSQL→MSSQL target-atomic
route whose source connection has no signed `postgres_source_authority`.
Runtime will not infer physical identity from a hostname, current catalog, or
the first successful execution.

## Fix

Using the deployment runtime principal, discover and review the topology role,
database and principal OIDs, and the canonical schema/relation OIDs. Use the
version-2 `catalog_identity` profile for ordinary least-privilege extraction.
Add version-1 system-identifier and timeline pins only when enhanced physical
cluster verification is required and the exact control-function grants are
available. Add the closed document to the environment-owned source connection
registry. Follow the
[signed source authority runbook](../source-sink/postgres-to-mssql.md#signed-postgresql-source-authority).

Re-run:

```bash
dpone check <pipeline> --connections --environment <environment> --format json
```

Do not copy pins from another environment or create them at runtime. Promotion,
restore, role rotation, or relation recreation requires a reviewed rotation and
a new scheduler invocation.
