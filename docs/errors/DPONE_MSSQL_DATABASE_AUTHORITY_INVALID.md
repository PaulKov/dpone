# `DPONE_MSSQL_DATABASE_AUTHORITY_INVALID`

The environment registry contains a malformed or open SQL Server database pin.
The identity document is deliberately closed: extra fields, booleans in place
of IDs, non-canonical GUIDs, and non-style-126 timestamps are rejected.

## Required shape

```yaml
database_authorities:
  DWH:
    database_id: 7
    create_token: "2026-08-16T09:14:22.1233333"
    database_guid: 01234567-89ab-cdef-0123-456789abcdef
```

Generate the values with the documented read-only discovery query; never edit a
digest or normalize a database name by hand. See the
[PostgreSQL→MSSQL authority runbook](../source-sink/postgres-to-mssql.md#signed-sql-server-database-authority).
