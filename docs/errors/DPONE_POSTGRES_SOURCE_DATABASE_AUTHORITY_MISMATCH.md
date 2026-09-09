# `DPONE_POSTGRES_SOURCE_DATABASE_AUTHORITY_MISMATCH`

The route's authored source database or the resolved source connection database
does not exactly equal the canonical database in its signed PostgreSQL source
authority. Dpone rejects this before creating a source connector.

## Fix

Confirm which physical database the route is intended to read. Correct the
editable route/connection coordinates when the signed pin is still current. If
the database was restored, recreated, promoted from another authority, or
intentionally relocated, discover all physical facts again, review and publish
a registry rotation, and start a new scheduler invocation. Do not rename the
signed database or reuse an old receipt to fit a reachable endpoint.

Use the
[source-authority discovery and rotation runbook](../source-sink/postgres-to-mssql.md#signed-postgresql-source-authority)
before re-running `dpone check --connections`.
