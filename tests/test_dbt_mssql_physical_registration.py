"""Registration representation tests do not grant SQL privileges."""

from dataclasses import FrozenInstanceError, replace
from uuid import UUID

import pytest

from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_values import (
    DatabasePrincipal,
    DatabaseRoleMapping,
    DedicatedObserver,
    RegisteredLimits,
    RegisteredPrincipals,
    SharedObserver,
)
from tests.support.dbt_mssql_physical_registration import registration_inputs


@pytest.mark.parametrize("principal_id", [True, 0, 4, 2147483648, "5", 5.0])
def test_principal_id_is_exact_sql_user_range(principal_id):
    with pytest.raises(ValueError):
        DatabasePrincipal(principal_id, "aa")


@pytest.mark.parametrize("sid", ["", "a", "AAA0", "0xaa", "gg", "aa" * 86, "éé", None])
def test_sid_is_exact_bounded_lowercase_bytes(sid):
    with pytest.raises(ValueError):
        DatabasePrincipal(5, sid)


def test_principal_is_frozen_and_boundary_bytes_are_retained():
    principal = DatabasePrincipal(2147483647, "aa" * 85)
    assert principal.to_dict() == {"principal_id": 2147483647, "sid_hex": "aa" * 85}
    with pytest.raises(FrozenInstanceError):
        principal.principal_id = 6


def mapping(control_id, control_sid, model_id, model_sid):
    return DatabaseRoleMapping(DatabasePrincipal(control_id, control_sid), DatabasePrincipal(model_id, model_sid))


def test_roles_compare_within_each_database_not_across_namespaces():
    metadata = mapping(5, "aa", 6, "bb")
    build = mapping(6, "bb", 5, "aa")
    roles = RegisteredPrincipals(metadata, build, DedicatedObserver(mapping(7, "cc", 7, "cc")))
    assert roles.to_dict()["observer"]["mode"] == "DEDICATED"


@pytest.mark.parametrize("control", [(5, "bb"), (6, "aa")])
def test_metadata_and_build_require_both_id_and_sid_separation(control):
    with pytest.raises(ValueError):
        RegisteredPrincipals(
            mapping(5, "aa", 5, "aa"), mapping(*control, 6, "bb"), DedicatedObserver(mapping(7, "cc", 7, "cc"))
        )


@pytest.mark.parametrize("mode", ["SHARE_METADATA", "SHARE_BUILD"])
def test_sharing_represents_permission_digest_without_claiming_authentication(mode):
    observer = SharedObserver(mode, "sha256:" + "a" * 64)
    assert observer.to_dict() == {"mode": mode, "permission_contract_sha256": "sha256:" + "a" * 64}
    RegisteredPrincipals(mapping(5, "aa", 5, "aa"), mapping(6, "bb", 6, "bb"), observer)


@pytest.mark.parametrize("mode", ["DEDICATED", "SHARED", "share_build", None])
def test_sharing_is_closed(mode):
    with pytest.raises(ValueError):
        SharedObserver(mode, "sha256:" + "a" * 64)


def test_definition_budget_is_independent_utf16_sql_bound():
    limits = RegisteredLimits(1, 9223372036854775807, 10, 2147483647, 10, 256)
    assert limits.to_dict()["max_definition_utf16_bytes"] == 2147483647
    assert limits.max_metadata_bytes == 1


@pytest.mark.parametrize(
    "index,value",
    [
        (0, 0),
        (0, 1048577),
        (1, 9223372036854775808),
        (2, 2147483648),
        (3, 0),
        (3, 2147483648),
        (4, 11),
        (5, 255),
        (5, True),
    ],
)
def test_registered_limits_reject_overflow_wrong_ceiling_and_coercion(index, value):
    values = [1024, 4096, 10, 2048, 10, 256]
    values[index] = value
    with pytest.raises(ValueError):
        RegisteredLimits(*values)


def test_registration_has_detached_complete_projection_without_self_digest():
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    payload = registration.to_dict()
    assert len(payload) == 18
    assert payload["schema"] == "dpone.mssql-physical-runtime-registration.v1"
    assert "registration_digest" not in payload
    payload["principals"]["metadata"]["control"]["principal_id"] = 99
    assert registration.principals.metadata.control.principal_id == 5


@pytest.mark.parametrize("field", ["database_name", "database_id", "database_guid", "create_token"])
def test_partially_matching_database_identity_rejects(field):
    values = registration_inputs()
    changes = {
        "database_name": "another",
        "database_id": 6,
        "database_guid": UUID("30000000-0000-0000-0000-000000000001"),
        "create_token": "2024-02-29T12:00:00.1234568",
    }
    values["model_database"] = replace(values["model_database"], **{field: changes[field]})
    with pytest.raises(ValueError):
        MssqlPhysicalRuntimeRegistration(**values)


def test_same_database_requires_equal_role_mappings_but_distinct_databases_do_not():
    values = registration_inputs()
    values["principals"] = RegisteredPrincipals(
        mapping(5, "aa", 6, "bb"),
        mapping(6, "bb", 5, "aa"),
        DedicatedObserver(mapping(7, "cc", 7, "cc")),
    )
    with pytest.raises(ValueError):
        MssqlPhysicalRuntimeRegistration(**values)
    values["model_database"] = replace(
        values["model_database"],
        database_name="another",
        database_id=6,
        database_guid=UUID("30000000-0000-0000-0000-000000000001"),
    )
    MssqlPhysicalRuntimeRegistration(**values)


@pytest.mark.parametrize("field", ["qualification_policy_id", "control_connection_ref", "model_connection_ref"])
@pytest.mark.parametrize("value", ["", " \t", "x" * 257, None, True])
def test_policy_labels_preserve_existing_nonblank_token_contract(field, value):
    values = registration_inputs()
    values[field] = value
    with pytest.raises(ValueError):
        MssqlPhysicalRuntimeRegistration(**values)
