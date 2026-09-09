"""SQL Server shadow-swap helpers built on normalized table identity."""

from __future__ import annotations

from dpone.contracts.mssql_object_name import MSSQLObjectName, mssql_sp_rename_statement, quote_mssql_identifier

_COPY_TABLE_PERMISSIONS_SQL = """
DECLARE @src_id INT = OBJECT_ID(@source_object);
DECLARE @dst_id INT = OBJECT_ID(@target_object);
IF @src_id IS NULL OR @dst_id IS NULL
    RETURN;
DECLARE @grant_sql NVARCHAR(MAX) = N'';
SELECT @grant_sql = @grant_sql
    + CASE perm.state_desc
        WHEN 'DENY' THEN N'DENY '
        ELSE N'GRANT '
      END
    + perm.permission_name
    + N' ON '
    + QUOTENAME(OBJECT_SCHEMA_NAME(@dst_id))
    + N'.'
    + QUOTENAME(OBJECT_NAME(@dst_id))
    + N' TO '
    + QUOTENAME(grantee.name)
    + CASE
        WHEN perm.state_desc = 'GRANT_WITH_GRANT_OPTION' THEN N' WITH GRANT OPTION'
        ELSE N''
      END
    + N';'
FROM sys.database_permissions AS perm
INNER JOIN sys.database_principals AS grantee
    ON perm.grantee_principal_id = grantee.principal_id
WHERE perm.major_id = @src_id
  AND perm.minor_id = 0
  AND perm.grantee_principal_id > 4
  AND grantee.type IN ('S', 'U', 'G', 'R', 'A');
IF LEN(@grant_sql) > 0
    EXEC sp_executesql @grant_sql;
"""


def mssql_copy_table_permissions_statement(source: MSSQLObjectName, target: MSSQLObjectName) -> str:
    """Copy table-level GRANT/DENY metadata from ``source`` to ``target`` inside one database."""

    _assert_same_database(source, target)
    source_object = _quote_sql_literal(source.sp_rename_object_name)
    target_object = _quote_sql_literal(target.sp_rename_object_name)
    batch = _quote_sql_literal(_COPY_TABLE_PERMISSIONS_SQL.strip())
    if source.database:
        db_sql = quote_mssql_identifier(source.database)
        return (
            f"EXEC {db_sql}.sys.sp_executesql "
            f"N'{batch}', "
            f"N'@source_object nvarchar(776), @target_object nvarchar(776)', "
            f"N'{source_object}', N'{target_object}'"
        )
    return (
        f"EXEC sp_executesql N'{batch}', "
        f"N'@source_object nvarchar(776), @target_object nvarchar(776)', "
        f"N'{source_object}', N'{target_object}'"
    )


def mssql_shadow_swap_statements(
    *,
    target: MSSQLObjectName,
    shadow_table: str,
    backup_table: str,
    target_existed: bool,
) -> tuple[str, ...]:
    """Ordered SQL statements for shadow cutover with permission preservation."""

    shadow = target.with_table(shadow_table)
    backup = target.with_table(backup_table)
    statements: list[str] = []
    if target_existed:
        statements.append(mssql_sp_rename_statement(target, backup_table))
    statements.append(mssql_sp_rename_statement(shadow, target.table))
    if target_existed:
        statements.append(mssql_copy_table_permissions_statement(backup, target))
        statements.append(f"DROP TABLE IF EXISTS {backup.quoted()}")
    return tuple(statements)


def _assert_same_database(left: MSSQLObjectName, right: MSSQLObjectName) -> None:
    if left.database != right.database or left.schema != right.schema:
        raise ValueError("MSSQL table swap permissions require the same database and schema")


def _quote_sql_literal(value: str) -> str:
    return str(value).replace("'", "''")


__all__ = [
    "mssql_copy_table_permissions_statement",
    "mssql_shadow_swap_statements",
]
