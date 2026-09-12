"""Offline worker boundary checks; actual issued-login SQL campaign is separate."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_mssql_transaction_fence import MssqlCompositionTransactionFence
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_mssql_binding import CompositionMssqlOperationBinding
from dpone.contracts.composition_persistence import encode_activation_request
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from tests.composition_mssql_gate_helpers import SERVICE, attempt, occurrence
from tests.test_mssql_generic_transaction_governance import _operation, _receipt


def binding():
    original = _operation()
    request = replace(original.attempt.request, strategy="full_refresh")
    operation = replace(original, attempt=replace(original.attempt, request=request))
    write = DbtRelationWrite(
        "p",
        "w",
        "t",
        "transfer",
        "mssql",
        "target",
        request.target_database,
        request.target_schema,
        request.target_table,
    )
    return CompositionMssqlOperationBinding(
        attempt(),
        operation,
        write,
        b"p" * 32,
        b"s" * 16,
        SERVICE,
        "Control",
        encode_activation_request(occurrence().request),
    )


class Cursor:
    def __init__(self, value):
        self.binding = value
        self.statements = []
        self.state = (value.operation.attempt.request.target_database, 1, 1, 42, value.issued_sid)
        self.result = ((42, value.digest),)
        self.closed = False
        self.after = None

    def execute(self, sql, *parameters):
        self.statements.append((sql, parameters))
        self.rows = self.result if sql.startswith("EXEC") else (self.state,)
        if sql.startswith("EXEC") and self.after is not None:
            self.state = self.after

    def fetchall(self):
        return self.rows

    def close(self):
        self.closed = True


def setup():
    value = binding()
    cursor = Cursor(value)
    connection = SimpleNamespace(autocommit=False, cursor=lambda: cursor)
    return value, cursor, SimpleNamespace(connection=connection)


def require(fence, connector, operation):
    return fence.require_current(connector, operation, mutation_plan_sha256=b"p" * 32)


def test_cross_database_procedure_runs_on_same_connection_transaction():
    value, cursor, connector = setup()
    fence = MssqlCompositionTransactionFence(value)
    assert require(fence, connector, value.operation) == 42
    sql, parameters = cursor.statements[1]
    assert sql.startswith("EXEC [Control].[dpone_control].[composition_require_transfer]")
    assert parameters[:3] == (value.digest, value.document, 42)
    assert len(cursor.statements) == 3
    assert cursor.closed
    assert not any("FROM [Control]" in statement for statement, _ in cursor.statements)


@pytest.mark.parametrize(
    "field,value",
    [
        ("target_table", "foreign"),
        ("target_schema", "foreign"),
        ("target_database", "foreign"),
        ("strategy", "incremental_append"),
        ("load_id", "foreign"),
    ],
)
def test_foreign_same_database_operation_rejected_before_sql(field, value):
    original, cursor, connector = setup()
    request = replace(original.operation.attempt.request, **{field: value})
    operation = replace(original.operation, attempt=replace(original.operation.attempt, request=request))
    with pytest.raises(CompositionAdmissionError):
        require(MssqlCompositionTransactionFence(original), connector, operation)
    assert cursor.statements == []


def test_changed_invocation_and_epoch_rejected_before_sql():
    value, cursor, connector = setup()
    request = value.operation.attempt.request
    changed = replace(request, invocation=replace(request.invocation, run_id="foreign"))
    for operation in (
        replace(value.operation, epoch=99),
        replace(value.operation, attempt=replace(value.operation.attempt, request=changed)),
    ):
        with pytest.raises(CompositionAdmissionError):
            require(MssqlCompositionTransactionFence(value), connector, operation)
    assert cursor.statements == []


@pytest.mark.parametrize(
    "state",
    [
        ("other", 1, 1, 42, b"s" * 16),
        ("DWH", 0, 1, 42, b"s" * 16),
        ("DWH", 1, -1, 42, b"s" * 16),
        ("DWH", 1, 1, 42, b"x" * 16),
    ],
)
def test_actual_session_rejects_before_module(state):
    value, cursor, connector = setup()
    cursor.state = state
    with pytest.raises(CompositionAdmissionError):
        require(MssqlCompositionTransactionFence(value), connector, value.operation)
    assert len(cursor.statements) == 1
    assert cursor.closed


def test_changed_transaction_after_module_rejects():
    value, cursor, connector = setup()
    cursor.after = (*cursor.state[:3], 43, cursor.state[4])
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        require(MssqlCompositionTransactionFence(value), connector, value.operation)


def test_foreign_module_ack_rejects():
    value, cursor, connector = setup()
    cursor.result = ((42, b"x" * 32),)
    with pytest.raises(CompositionAdmissionError, match="transfer_module_result"):
        require(MssqlCompositionTransactionFence(value), connector, value.operation)


def test_missing_or_wrong_plan_rejects_before_sql():
    value, cursor, connector = setup()
    for plan in (None, b"x" * 32):
        with pytest.raises(CompositionAdmissionError):
            MssqlCompositionTransactionFence(value).require_current(
                connector, value.operation, mutation_plan_sha256=plan
            )
    assert cursor.statements == []


def test_foreign_receipt_and_unbound_replay_reject_before_sql():
    value, cursor, connector = setup()
    with pytest.raises(CompositionAdmissionError):
        MssqlCompositionTransactionFence(value).require_current(connector, receipt=_receipt(_operation()))
    with pytest.raises(CompositionAdmissionError):
        MssqlCompositionTransactionFence(value).require_current(connector)
    assert cursor.statements == []


def test_exact_receipt_replay_keeps_live_composition_check():
    value, cursor, connector = setup()
    receipt = replace(_receipt(value.operation), mutation_plan_sha256=value.mutation_plan_sha256)
    assert MssqlCompositionTransactionFence(value).require_current(connector, receipt=receipt) == 42
    assert len(cursor.statements) == 3
    for field, changed in (("owner_digest", b"x" * 32), ("operation_epoch", 99), ("mutation_plan_sha256", b"x" * 32)):
        cursor.statements.clear()
        with pytest.raises(CompositionAdmissionError):
            MssqlCompositionTransactionFence(value).require_current(
                connector, receipt=replace(receipt, **{field: changed})
            )
        assert cursor.statements == []
