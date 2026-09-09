# `DPONE_MSSQL_STATE_DATABASE_AUTHORITY_REQUIRED`

The target-atomic MSSQL state connection has no signed pin for its configured
database. Without that pin, a same-name state database replacement could accept
catalog writes or old invocation receipts under the wrong authority.

## Fix

Discover and author the state database under the state connection's
`connection.database_authorities`. The pin must contain the canonical name,
`database_id`, style-126 `create_token`, and `database_guid`. Install the exact
external state catalog separately; database authority never provisions DDL.

See [Signed SQL Server database authority](../source-sink/postgres-to-mssql.md#signed-sql-server-database-authority)
for the query, least-privilege requirements, and reviewed rotation procedure.
