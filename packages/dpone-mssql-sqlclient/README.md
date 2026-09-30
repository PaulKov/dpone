# dpone-mssql-sqlclient

Optional Linux x86-64 companion for the dpone ClickHouse → Microsoft SQL Server
native route. It streams a sealed dpone native file through
`Microsoft.Data.SqlClient.SqlBulkCopy` while holding a session application lock
on the same non-pooled connection.

The package requires `Microsoft.NETCore.App 10.x`. An explicit
`mssql_sqlclient` selector activates discovery and fail-closed runtime admission.
Each deployment still requires evidence-bound route qualification; package
metadata does not enforce or claim that qualification.
Credentials cross an anonymous inherited pipe and never enter argv,
environment variables, stdout, stderr, journals, or evidence.

Install and diagnose through the version-matched dpone extra after activation:

```bash
pip install "dpone[clickhouse,mssql-sqlclient]==0.88.0"
dpone runtime mssql-sqlclient doctor --format json
```

BCP remains the compatible default. There is no automatic fallback between
writers after a grant has been issued.
