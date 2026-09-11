"""The shared SQL layout is external storage, not a changed execution codec."""

import pytest

from dpone.adapters.composition_mssql_schema import (
    COMPOSITION_MSSQL_LEDGER_LOCK,
    COMPOSITION_MSSQL_SCHEMA_VERSION,
    render_composition_mssql_schema,
)


def test_shared_layout_replaces_both_legacy_journals_without_a_second_lock():
    sql = render_composition_mssql_schema()
    assert COMPOSITION_MSSQL_SCHEMA_VERSION == 2
    assert COMPOSITION_MSSQL_LEDGER_LOCK == "dpone:composition-control:v1"
    assert sql.count("CREATE TABLE ") == 8
    for name in ("owners", "owner_domains", "operations", "operation_domains"):
        assert f"CREATE TABLE [dpone_control].[composition_{name}]" in sql
    for name in ("activations", "activation_domains", "attempts", "attempt_domains"):
        assert f"CREATE TABLE [dpone_control].[composition_{name}]" not in sql
    assert "HASHBYTES" not in sql
    assert "CREATE VIEW" not in sql


def test_original_family_and_epoch_are_bound_by_composite_foreign_keys():
    sql = render_composition_mssql_schema()
    assert "FOREIGN KEY ([owner_key], [operation_family], [owner_subject_sha256])" in sql
    assert "REFERENCES [dpone_control].[composition_owners] ([owner_key], [owner_kind], [subject_sha256])" in sql
    assert "FOREIGN KEY ([owner_key], [guard_id], [fencing_epoch])" in sql
    assert "REFERENCES [dpone_control].[composition_owner_domains] ([owner_key], [guard_id], [fencing_epoch])" in sql
    assert "UNIQUE NONCLUSTERED ([operation_family], [replay_key])" in sql
    assert "UNIQUE NONCLUSTERED ([guard_id], [fencing_epoch])" in sql


def test_postgres_is_a_physical_domain_not_an_execution_principal_or_proof():
    from dpone.adapters.composition_mssql_layout import COMPOSITION_TABLES

    tables = {table.name: table for table in COMPOSITION_TABLES}
    assert "'postgres'" in " ".join(check.expression for check in tables["domains"].checks)
    assert "'postgres'" not in " ".join(check.expression for check in tables["issued_authorities"].checks)
    assert "'qualification'" not in " ".join(check.expression for check in tables["proofs"].checks)
    assert "'execution'" in " ".join(check.expression for check in tables["proofs"].checks)


@pytest.mark.parametrize("schema", ["", "x.y", "x]; DROP TABLE x;--", "x" * 129])
def test_invalid_schema_never_renders_a_partial_batch(schema):
    with pytest.raises(ValueError):
        render_composition_mssql_schema(schema)
