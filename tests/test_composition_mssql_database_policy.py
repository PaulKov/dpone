"""Catalog refusal diagnostics preserve every existing writer-policy predicate."""

import pytest

from dpone.adapters.composition_mssql_issuance import MssqlEnrollment, _require_database_policy
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_control import CompositionAdmissionError
from tests.composition_mssql_gate_helpers import Cursor


def observe(result):
    permissions = [(0, 0, None, name, "G") for name in ("CREATE TABLE", "CREATE VIEW")]
    permissions += [
        (3, 1, "dbo", name, "G")
        for name in ("SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "ALTER", "VIEW DEFINITION")
    ]
    cursor = Cursor([("SELECT p.class", permissions), ("SELECT schema_id", [(1, "dbo")]), ("DECLARE @role", result)])
    enrollment = MssqlEnrollment("sha256:" + "a" * 64, "Target", 8, "unused", "unused", "writer", ("dbo",))
    _require_database_policy(CompositionMssqlLedger(cursor, "dpone_control"), enrollment)
    assert not cursor.steps


def test_complete_policy_observation_with_no_failed_predicate_is_accepted():
    observe([(0,)])


@pytest.mark.parametrize(
    "code,reason",
    enumerate(
        (
            "role_owner",
            "additional_role",
            "object_type",
            "object_owner",
            "unmanaged_object",
            "assembly",
            "user_authentication",
            "ambient_permission",
            "role_membership",
            "schema_owner",
            "server_principal",
            "public_server_permission",
        ),
        1,
    ),
)
def test_catalog_violation_has_its_exact_sanitized_reason(code, reason):
    with pytest.raises(CompositionAdmissionError, match="login_database_policy_" + reason):
        observe([(code,)])


@pytest.mark.parametrize("result", [[], [(None,)], [(True,)], [(False,)], [(13,)], [(-1,)], [(0, 0)]])
def test_missing_or_invalid_policy_observation_never_becomes_success(result):
    with pytest.raises(CompositionAdmissionError, match="login_database_policy_observation"):
        observe(result)
