# `DPONE_MSSQL_STAGING_DATABASE_AUTHORITY_REQUIRED`

The governed MSSQL route can stage in a database that is not covered by the
target connection's finite `database_authorities` map. Dpone refuses the route
before source export or staging DDL.

## Fix

Add a separate signed pin for the exact `sink.staging.database`. Even when the
same connection credential reaches target and staging, each physical database
needs its own `database_id`, style-126 `create_token`, and `database_guid`.
Follow the [discovery and rotation runbook](../source-sink/postgres-to-mssql.md#signed-sql-server-database-authority),
then rerun `dpone check --connections`.

Do not point staging back to target merely to bypass this error; use the actual
deployment topology and grant the runtime principal only the required staging
schema rights.
