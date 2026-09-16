"""Frozen SQL projection artifact and deterministic schema verification."""

from dataclasses import replace
from pathlib import Path

import pytest

from dpone.adapters.dbt_mssql_physical_registration_schema import (
    COLUMNS,
    registration_table_sql,
    verify_registration_table_sql,
)
from dpone.adapters.dbt_mssql_physical_registration_store import registration_columns
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_values import SharedObserver
from tests.support.dbt_mssql_physical_registration import registration_inputs


def test_package_table_is_exact_frozen_projection():
    path = Path(__file__).parents[1] / "packages/dbt-dpone/control/sqlserver/physical-v1/schema.sql"
    assert path.read_text() == registration_table_sql("dpone_physical")
    columns = registration_columns(MssqlPhysicalRuntimeRegistration(**registration_inputs()))
    assert tuple(columns) == tuple(name for name, _, _ in COLUMNS)
    assert len(columns) == 53
    assert columns["model_database_create_token"] == "2024-02-29T12:00:00.1234567"
    assert columns["observer_permission_contract_sha256"] == b""


def test_verification_covers_hidden_behavior_and_exact_scale():
    sql = verify_registration_table_sql("dpone_physical")
    for guard in (
        "sys.triggers",
        "sys.foreign_keys",
        "is_nullable",
        "is_computed",
        "is_identity",
        "default_object_id",
        "user_type_id<>system_type_id",
        "generated_always_type",
        "encryption_type",
        "scale<>7",
        "is_disabled",
        "ignore_dup_key",
        "is_not_trusted",
    ):
        assert guard in sql
    assert "DROP " not in sql and "ALTER " not in sql


@pytest.mark.parametrize("mode,role", [("SHARE_METADATA", "metadata"), ("SHARE_BUILD", "build")])
def test_shared_observer_projection_retains_inherited_identity_and_contract(mode, role):
    value = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    digest = "sha256:" + "1" * 64
    value = replace(value, principals=replace(value.principals, observer=SharedObserver(mode, digest)))
    columns = registration_columns(value)
    assert columns["observer_mode"] == mode.encode()
    assert columns["observer_permission_contract_sha256"] == digest.encode()
    for namespace in ("control", "model"):
        for suffix in ("principal_id", "sid"):
            assert columns[f"observer_{namespace}_{suffix}"] == columns[f"{role}_{namespace}_{suffix}"]
