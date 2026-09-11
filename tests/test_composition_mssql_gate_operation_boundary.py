"""Gate phase failures cannot turn existing-operation readback into new authority."""

import pytest

from dpone.adapters import composition_mssql_attempts as attempts_module
from dpone.adapters import composition_mssql_login_gate as gate_module
from dpone.adapters.composition_mssql_issuance import MssqlEnrollment, MssqlIssuedCredentials
from dpone.adapters.composition_mssql_login_gate import MssqlCompositionLoginGate
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_attempt import CompositionAttemptReceipt
from tests.composition_mssql_gate_helpers import SERVICE, Connection, Cursor, attempt, begin_steps, occurrence
from tests.composition_mssql_gate_helpers import offline_catalog as offline_catalog
from tests.test_composition_mssql_login_gate import issue_steps


@pytest.mark.parametrize("parent_state", ["PREPARED", "ACTIVE", "RETIRING", "RETIRED"])
@pytest.mark.parametrize("operation_state", ["RUNNING", "COMMIT_UNKNOWN", "SUCCEEDED", "FAILED"])
def test_issuance_requires_both_actual_active_owner_and_running_operation(monkeypatch, parent_state, operation_state):
    ledger = CompositionMssqlLedger(Cursor(begin_steps()), "dpone_control")
    ledger.begin(SERVICE)
    value = attempt()
    hashes = ("sha256:" + "a" * 64,) * 3 if operation_state in {"SUCCEEDED", "FAILED"} else (None,) * 3
    receipt = CompositionAttemptReceipt(value, operation_state, *hashes)
    observed = []

    def current(context, selected, *, expected_service_id, terminal_validator):
        assert context is ledger and selected == value and expected_service_id == SERVICE
        assert callable(terminal_validator)
        observed.append(True)
        return occurrence(parent_state), receipt

    monkeypatch.setattr(gate_module, "require_existing_execution_in", current)
    if (parent_state, operation_state) == ("ACTIVE", "RUNNING"):
        MssqlCompositionLoginGate._require_running(ledger, value)
    else:
        with pytest.raises(CompositionAdmissionError):
            MssqlCompositionLoginGate._require_running(ledger, value)
    assert observed == [True]


@pytest.mark.parametrize("failed_read", [1, 2, 3, 4, 5])
def test_existing_original_refusal_in_every_issue_phase_returns_no_credential(monkeypatch, failed_read):
    value = attempt()
    credentials = MssqlIssuedCredentials("dpone_v3_" + value.attempt_sha256[7:], b"a" * 16, "offline-secret")
    enrollment = MssqlEnrollment(
        value.guard_epochs[0][0],
        "SyntheticTarget",
        8,
        "10000000-0000-4000-8000-000000000009",
        "2026-09-10T00:00:00",
        "bounded_writer",
        ("dbo",),
    )
    cursors = [Cursor(steps) for steps in issue_steps(credentials)]
    connections = iter(Connection(cursor) for cursor in cursors)
    reads, closures = [], []

    def observed(context, selected, *, expected_service_id, terminal_validator):
        assert selected == value and expected_service_id == SERVICE
        reads.append(context)
        if len(reads) == failed_read:
            raise CompositionAdmissionError("original_operation_refused")
        return occurrence(), CompositionAttemptReceipt(value, "RUNNING")

    # Only the already observed operation and physical enrollment are supplied
    # by this layer's double. All five real gate call sites must inspect them.
    monkeypatch.setattr(gate_module, "require_existing_execution_in", observed)
    monkeypatch.setattr(attempts_module, "require_existing_execution_in", observed)
    monkeypatch.setattr(gate_module, "new_credentials", lambda _: credentials)
    monkeypatch.setattr(gate_module, "require_gate_policy", lambda *_: None)
    monkeypatch.setattr(gate_module, "require_enrollments", lambda *_: (enrollment,))
    monkeypatch.setattr(MssqlCompositionLoginGate, "close", lambda _, selected: closures.append(selected))
    gate = MssqlCompositionLoginGate(lambda: next(connections), expected_service_id=SERVICE, control_database="Control")
    with pytest.raises(CompositionAdmissionError, match="original_operation_refused"):
        gate.issue_once(value)
    assert len(reads) == failed_read
    assert closures == ([] if failed_read == 1 else [value])
    login_created = any("CREATE LOGIN" in sql for cursor in cursors for sql, _ in cursor.calls)
    assert login_created is (failed_read > 3)
