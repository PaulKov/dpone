from __future__ import annotations

from dpone.contracts.mssql_object_name import MSSQLObjectName, mssql_sp_rename_statement
from dpone.contracts.mssql_table_swap import mssql_copy_table_permissions_statement, mssql_shadow_swap_statements


def test_mssql_sp_rename_uses_schema_table_in_target_database_scope() -> None:
    name = MSSQLObjectName.from_parts(
        schema="dwh_example.marketing",
        table="sample_web_sync__dpone_shadow_ab12cd34",
        database="dwh_example",
    )

    statement = mssql_sp_rename_statement(name, "sample_web_sync")

    assert "EXEC [dwh_example].sys.sp_executesql" in statement
    assert "marketing.sample_web_sync__dpone_shadow_ab12cd34" in statement
    assert "dwh_example.marketing.sample_web_sync__dpone_shadow_ab12cd34" not in statement


def test_mssql_shadow_swap_copies_permissions_before_backup_drop() -> None:
    target = MSSQLObjectName.from_parts(
        schema="dwh_example.marketing",
        table="sample_web_sync",
        database="dwh_example",
    )

    statements = mssql_shadow_swap_statements(
        target=target,
        shadow_table="sample_web_sync__dpone_shadow_ab12cd34",
        backup_table="sample_web_sync__dpone_backup_ab12cd34",
        target_existed=True,
    )

    assert len(statements) == 4
    assert "sp_rename" in statements[0]
    assert "sp_rename" in statements[1]
    assert "sys.database_permissions" in statements[2]
    assert statements[3].startswith(
        "DROP TABLE IF EXISTS [dwh_example].[marketing].[sample_web_sync__dpone_backup_ab12cd34]"
    )


def test_mssql_shadow_swap_skips_permission_copy_for_first_load() -> None:
    target = MSSQLObjectName.from_parts(schema="landing", table="orders")

    statements = mssql_shadow_swap_statements(
        target=target,
        shadow_table="orders__dpone_shadow_ab12cd34",
        backup_table="orders__dpone_backup_ab12cd34",
        target_existed=False,
    )

    assert statements == (mssql_sp_rename_statement(target.with_table("orders__dpone_shadow_ab12cd34"), "orders"),)


def test_mssql_copy_table_permissions_requires_same_database_and_schema() -> None:
    source = MSSQLObjectName.from_parts(schema="landing", table="orders__dpone_backup_ab12cd34")
    target = MSSQLObjectName.from_parts(schema="archive", table="orders")

    try:
        mssql_copy_table_permissions_statement(source, target)
    except ValueError as exc:
        assert "same database and schema" in str(exc)
    else:
        raise AssertionError("expected ValueError")
