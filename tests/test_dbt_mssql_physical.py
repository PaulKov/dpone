"""Pure physical-plan primitives cannot establish runtime admission."""

import hashlib
import json

import pytest

from dpone.contracts.dbt_mssql_physical_validation import (
    PhysicalPlanError,
    physical_object_name,
    require_physical_identifier,
    require_physical_timestamp,
    require_physical_uuid,
    require_sql_positive_integer,
)

GENERATION = "10000000-0000-0000-0000-000000000001"


@pytest.mark.parametrize("role,prefix", [("CANDIDATE", "c"), ("HELPER", "h"), ("BACKUP", "b"), ("CCI", "i")])
def test_object_names_use_exact_domain_separated_canonical_bytes(role, prefix):
    payload = {
        "schema": "dpone.mssql-physical-object-name.v1",
        "generation_id": GENERATION,
        "model_unique_id": "model.example.orders",
        "role": role,
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert physical_object_name(GENERATION, "model.example.orders", role) == f"dpone_{prefix}_{expected}"


def test_names_preserve_unicode_identity_and_change_with_generation():
    name = physical_object_name(GENERATION, "model.example.é", "HELPER")
    assert name != physical_object_name(GENERATION, "model.example.e\u0301", "HELPER")
    assert name != physical_object_name("20000000-0000-0000-0000-000000000001", "model.example.é", "HELPER")


@pytest.mark.parametrize("value", ["", "x" * 129, "😀" * 65, "a\x00b", "a\x85b", "\ud800", 12, True])
def test_identifier_rejects_invalid_exact_sql_names(value):
    with pytest.raises(PhysicalPlanError):
        require_physical_identifier(value, "table")


@pytest.mark.parametrize("value", ["x" * 128, "😀" * 64, "a]b", "table with spaces", "é", "e\u0301"])
def test_identifier_retains_valid_spelling(value):
    assert require_physical_identifier(value, "table") == value


@pytest.mark.parametrize(
    "value",
    [GENERATION.replace("-", ""), "{10000000-0000-0000-0000-000000000001}", "A0000000-0000-0000-0000-000000000001", 1],
)
def test_uuid_rejects_noncanonical_wire_values(value):
    with pytest.raises(PhysicalPlanError):
        require_physical_uuid(value, "generation_id")


@pytest.mark.parametrize("value", [True, 0, -1, 2147483648, 1.0, "1"])
def test_sql_int_rejects_coercion_and_overflow(value):
    with pytest.raises(PhysicalPlanError):
        require_sql_positive_integer(value, "database_id")


def test_sql_int_and_bigint_upper_boundaries():
    assert require_sql_positive_integer(2147483647, "database_id") == 2147483647
    assert require_sql_positive_integer(9223372036854775807, "fencing_epoch", bigint=True) == 9223372036854775807
    with pytest.raises(PhysicalPlanError):
        require_sql_positive_integer(9223372036854775808, "fencing_epoch", bigint=True)


@pytest.mark.parametrize(
    "value",
    [
        "2026-02-29T12:00:00.0000000",
        "2024-02-29T24:00:00.0000000",
        "2024-02-29T12:00:00.000000",
        "2024-02-29T12:00:00.00000000",
        "２０２４-02-29T12:00:00.0000000",
        None,
    ],
)
def test_timestamp_rejects_invalid_calendar_or_noncanonical_text(value):
    with pytest.raises(PhysicalPlanError):
        require_physical_timestamp(value, "create_token")


def test_timestamp_retains_seventh_fractional_digit():
    value = "2024-02-29T12:00:00.1234567"
    assert require_physical_timestamp(value, "create_token") == value


@pytest.mark.parametrize(
    "model,role", [("", "HELPER"), ("x" * 4097, "HELPER"), ("m", "helper"), ("m", "OTHER"), ("\ud800", "CCI")]
)
def test_naming_rejects_invalid_selectors_without_echo(model, role):
    with pytest.raises(PhysicalPlanError) as error:
        physical_object_name(GENERATION, model, role)
    assert "OTHER" not in str(error.value)


def test_model_spec_and_plan_derive_digests_and_conditional_names():
    from dpone.contracts.dbt_mssql_physical import (
        AbsentPredecessor,
        PhysicalFilegroup,
        PhysicalModelPlan,
        PhysicalModelSpec,
        PhysicalRelation,
    )
    from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
    from dpone.contracts.native_identity import OriginalRef

    spec = PhysicalModelSpec(
        model_unique_id="model.example.orders",
        source_graph_sha256="sha256:" + "a" * 64,
        relation=PhysicalRelation("example", "models", "orders"),
        columns=(MssqlCatalogColumn("id", "int", False),),
        layout="rowstore_page",
        filegroup=PhysicalFilegroup(1, "PRIMARY"),
        resource_bounds=OriginalRef("bounds/one", "sha256:" + "b" * 64),
    )
    unsigned = spec.to_dict()
    digest = unsigned.pop("model_spec_sha256")
    assert (
        digest
        == "sha256:" + hashlib.sha256(json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    )
    plan = PhysicalModelPlan(GENERATION, spec, AbsentPredecessor())
    assert plan.backup_name is None
    assert plan.columnstore_index_name is None
    assert plan.candidate_name == physical_object_name(GENERATION, spec.model_unique_id, "CANDIDATE")
    unsigned_plan = plan.to_dict()
    digest = unsigned_plan.pop("model_plan_sha256")
    assert unsigned_plan["spec"]["model_spec_sha256"] == spec.model_spec_sha256
    assert (
        digest
        == "sha256:"
        + hashlib.sha256(json.dumps(unsigned_plan, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    )
