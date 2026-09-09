# `DPONE_MSSQL_TARGET_DATABASE_AUTHORITY_REQUIRED`

`dpone check --connections` found a governed MSSQL route whose target database
has no signed physical identity in the environment connection registry.

## Fix

Discover the database from the same SQL Server instance and runtime principal
used by the deployment, then add the exact canonical database name under
`connection.database_authorities`. Record `database_id`, the style-126
`create_token`, and `database_guid`; do not copy values from another environment
or let runtime code learn them automatically. Use the discovery query and
rotation procedure in the [PostgreSQL→MSSQL guide](../source-sink/postgres-to-mssql.md#signed-sql-server-database-authority).

Re-run:

```bash
dpone check <pipeline> --connections --environment <environment> --format json
```

A same-name database restore or recreate is an authority rotation, not a retry:
review and publish new registry bytes before starting a new invocation.
