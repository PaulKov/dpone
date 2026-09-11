"""Actual enrollment, installed barrier and observer privilege policy regressions."""

import pytest

from dpone.adapters.composition_mssql_gate_proofs import observe_quiescence
from dpone.adapters.composition_mssql_issuance import require_gate_policy
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.composition_mssql_gate_helpers import SERVICE, Cursor
from tests.composition_mssql_gate_helpers import issuing as issuing
from tests.composition_mssql_gate_helpers import offline_catalog as offline_catalog


def enrollment_steps(enrollment):
    pins = (
        enrollment.guard_id,
        enrollment.database,
        enrollment.database_id,
        enrollment.database_guid,
        enrollment.create_token,
        enrollment.writer_role,
        enrollment.database_id,
        enrollment.database_guid,
        enrollment.create_token,
        0,
        0,
        0,
        0,
        SERVICE,
        6,
        b"controller-sid",
        b"controller-sid",
    )
    permissions: list[tuple[object, ...]] = [(0, 0, None, name, "G") for name in ("CREATE TABLE", "CREATE VIEW")]
    permissions += [
        (3, 1, "dbo", name, "G")
        for name in ("SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "ALTER", "VIEW DEFINITION")
    ]
    return [
        ("a.guard_id, e.database_name", [pins]),
        ("schema_name FROM", [("dbo",)]),
        ("SELECT p.class", permissions),
        ("SELECT schema_id", [(1, "dbo")]),
        ("DECLARE @role", [(0,)]),
    ]


def test_enrollment_observes_actual_database_pins_and_exact_role_permissions(issuing):
    from dpone.adapters.composition_mssql_issuance import require_enrollments

    value, _, enrollment = issuing
    cursor = Cursor(enrollment_steps(enrollment))
    assert require_enrollments(CompositionMssqlLedger(cursor, "dpone_control"), value, SERVICE) == (enrollment,)
    assert not cursor.steps
    sql = cursor.calls[-1][0]
    assert "authentication_type<>1" in sql and "sys.server_role_members" in sql
    assert "sys.database_permissions" in sql and "sys.database_role_members" in sql
    assert "owning_principal_id=1" in sql and "principal_id<>1" in sql
    assert "m.guard_id=@guard" in sql


@pytest.mark.parametrize(
    "column,replacement",
    [
        (6, 99),
        (7, "different-guid"),
        (8, "different-create-token"),
        (9, 1),
        (10, 1),
        (11, 1),
        (12, 1),
        (13, "foreign-service"),
        (14, 8),
        (15, b"foreign-owner-sid"),
    ],
)
def test_enrollment_rejects_database_identity_elevation_or_same_control_database(issuing, column, replacement):
    from dpone.adapters.composition_mssql_issuance import require_enrollments

    value, _, enrollment = issuing
    steps = enrollment_steps(enrollment)
    pins = list(steps[0][1][0])
    pins[column] = replacement
    cursor = Cursor([(steps[0][0], [tuple(pins)])])
    with pytest.raises(CompositionAdmissionError, match="login_database_enrollment"):
        require_enrollments(CompositionMssqlLedger(cursor, "dpone_control"), value, SERVICE)
    assert len(cursor.calls) == 1


def test_extra_role_permission_blocks_issuance(issuing):
    from dpone.adapters.composition_mssql_issuance import require_enrollments

    value, _, enrollment = issuing
    steps = enrollment_steps(enrollment)
    steps[2][1].append((0, 0, None, "CONTROL", "G"))
    cursor = Cursor(steps[:4])
    with pytest.raises(CompositionAdmissionError, match="login_role_permissions"):
        require_enrollments(CompositionMssqlLedger(cursor, "dpone_control"), value, SERVICE)


def policy_steps():
    from dpone.adapters.composition_mssql_gate_schema import (
        GATE_READER,
        login_trigger_sql,
        module_sha256,
        monotonic_trigger_sql,
    )

    return [
        (
            "SELECT DB_NAME()",
            [
                (
                    "Control",
                    False,
                    module_sha256(login_trigger_sql("Control", "dpone_control")),
                    GATE_READER,
                    "S",
                    True,
                    1,
                    1,
                    1,
                    1,
                )
            ],
        ),
        ("SELECT t.is_disabled", [(False, module_sha256(monotonic_trigger_sql("dpone_control")))]),
        ("SELECT COUNT(*) FROM sys.databases", [(1,)]),
        ("EXECUTE AS LOGIN", [(1, 1, 1, 1)]),
    ]


def test_installed_gate_policy_matches_exact_bytes_disabled_reader_and_visibility():
    cursor = Cursor(policy_steps())
    require_gate_policy(CompositionMssqlLedger(cursor, "dpone_control"), "Control")
    assert not cursor.steps


@pytest.mark.parametrize("context", ["controller", "reader"])
def test_server_permission_checks_use_the_sql_server_securable_contract(context):
    """An invalid SERVER class yields NULL, even when the principal has rights."""

    class ServerPermissionCursor(Cursor):
        def execute(self, sql, *parameters):
            result = super().execute(sql, *parameters)
            selected = sql.startswith("SELECT DB_NAME()") if context == "controller" else "EXECUTE AS LOGIN" in sql
            if selected and "HAS_PERMS_BY_NAME(NULL, 'SERVER'," in sql:
                values = list(self.rows[0])
                start = 7 if context == "controller" else 0
                values[start : start + 3] = [None, None, None]
                self.rows = [tuple(values)]
            return result

    cursor = ServerPermissionCursor(policy_steps())
    require_gate_policy(CompositionMssqlLedger(cursor, "dpone_control"), "Control")
    assert not cursor.steps


@pytest.mark.parametrize(
    "column,replacement", [(1, True), (2, b"foreign-trigger"), (5, False), (6, 2), (7, None), (8, 0), (9, 0)]
)
def test_changed_disabled_logon_or_missing_complete_visibility_blocks(column, replacement):
    steps = policy_steps()
    values = list(steps[0][1][0])
    values[column] = replacement
    with pytest.raises(CompositionAdmissionError, match="login_gate_policy"):
        require_gate_policy(
            CompositionMssqlLedger(Cursor([(steps[0][0], [tuple(values)])]), "dpone_control"), "Control"
        )


def test_reader_impersonation_must_prove_gate_select_and_full_session_visibility():
    steps = policy_steps()
    steps[-1] = (steps[-1][0], [(1, 1, None, 1)])
    with pytest.raises(CompositionAdmissionError, match="login_gate_reader_visibility"):
        require_gate_policy(CompositionMssqlLedger(Cursor(steps), "dpone_control"), "Control")


def test_reader_probe_restores_controller_before_emitting_the_result_set():
    """A caller may close/cancel after its first row; REVERT must already run."""
    cursor = Cursor(policy_steps())
    require_gate_policy(CompositionMssqlLedger(cursor, "dpone_control"), "Control")
    probe = cursor.calls[-1][0]
    assert probe.index("REVERT;") < probe.rindex("SELECT ")


@pytest.mark.parametrize("observer", [[], [(None,)], [(True,)], [(0,)], [(123,), (456,)]])
def test_quiescence_requires_exact_observer_transaction_before_any_exclusion(issuing, observer):
    _, credentials, enrollment = issuing
    cursor = Cursor(
        [
            ("original_security_id", [(0,)]),
            ("dm_tran_session_transactions", [(0,)]),
            ("dm_tran_current_transaction", observer),
        ]
    )
    with pytest.raises(CompositionAdmissionError, match="login_observer_transaction"):
        observe_quiescence(CompositionMssqlLedger(cursor, "dpone_control"), credentials.login_sid, (enrollment,))
    assert not any("dm_tran_database_transactions" in sql for sql, _ in cursor.calls)
