"""Companion-table protocol tests, without SQL Server certification."""

import pytest

from dpone.adapters.dbt_mssql_physical_catalog_binding_schema import binding_table_sql, verify_binding_table_sql


def test_companion_is_four_columns_without_a_foreign_key():
    sql = binding_table_sql("runtime_local")
    assert "physical_catalog_bindings_v1" in sql
    for field in (
        "registration_id uniqueidentifier NOT NULL",
        "registration_digest varbinary(71) NOT NULL",
        "payload varbinary(max) NOT NULL",
        "binding_digest varbinary(71) NOT NULL",
    ):
        assert field in sql
    assert "PRIMARY KEY (registration_id)" in sql
    assert "DATALENGTH(payload)<=1048576" in sql
    assert "FOREIGN KEY" not in sql
    assert "ALTER TABLE" not in sql


def test_schema_verifier_rejects_hidden_behavior_and_foreign_keys():
    sql = verify_binding_table_sql("runtime_local")
    for guard in (
        "sys.foreign_keys",
        "referenced_object_id=@object",
        "sys.triggers",
        "is_computed",
        "is_identity",
        "is_hidden",
        "is_sparse",
        "is_disabled=0",
        "is_not_trusted=0",
        "column_id",
        "binding_digest",
        "EXCEPT",
        "principal_id=1",
    ):
        assert guard in sql


@pytest.mark.parametrize("name", ["bad]name", "x.y", "", "schema;DROP TABLE x"])
def test_invalid_schema_rejected(name):
    with pytest.raises(ValueError):
        binding_table_sql(name)
    with pytest.raises(ValueError):
        verify_binding_table_sql(name)
