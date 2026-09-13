"""Exercise production transfer wiring; no live SQL certification."""

from contextlib import ExitStack, contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import dpone.adapters.composition_mssql_login_gate as login
from dpone.adapters.composition_mssql_transfer_access import MssqlCompositionTransferAccess
from dpone.app.composition_dbt_execution_factory import CompositionDbtControlAuthority
from dpone.app.composition_transfer_execution_factory import build_composition_transfer_execution_dependencies
from tests.composition_mssql_gate_helpers import SERVICE, attempt
from tests.test_mssql_composition_transaction_fence import binding


def _dependencies():
    def forbidden_connection():
        raise AssertionError("offline wiring test must not connect")

    return build_composition_transfer_execution_dependencies(
        control=CompositionDbtControlAuthority(forbidden_connection, SERVICE, "Control", "custom_control"),
        read_active=lambda: None,
        sink_target=None,
        state_target=None,
        read_plan=lambda value: None,
        verify_operation=lambda *args, **kwargs: None,
    )


def test_production_gate_grants_and_verifies_transfer_access():
    deps = _dependencies()
    ledger = SimpleNamespace(
        cursor=SimpleNamespace(execute=lambda *args: None),
        table=lambda name: "[custom_control].[composition_" + name + "]",
    )

    @contextmanager
    def transaction():
        yield ledger

    with ExitStack() as stack:
        stack.enter_context(patch.object(deps.gate, "_transaction", transaction))
        for name in ("_require_running", "_require_gate", "_transition"):
            stack.enter_context(patch.object(deps.gate, name, lambda *args, **kwargs: None))
        for name in (
            "require_gate_policy",
            "require_enrollments",
            "create_login",
            "require_worker_users",
            "require_login",
        ):
            stack.enter_context(patch.object(login, name, lambda *args, **kwargs: None))
        stack.enter_context(patch.object(login, "row", lambda value: None))
        grant = stack.enter_context(patch.object(MssqlCompositionTransferAccess, "grant"))
        require = stack.enter_context(patch.object(MssqlCompositionTransferAccess, "require"))
        credentials = deps.gate.issue_once(attempt())
        grant.assert_called_once_with(ledger, credentials)
        require.assert_called_once_with(ledger, credentials)


def test_production_registrar_retains_custom_control_schema():
    from dpone.adapters.composition_mssql_transaction_binding import MssqlCompositionTransactionBindings

    value = binding()
    with patch.object(MssqlCompositionTransactionBindings, "bind", return_value=value):
        deps = _dependencies()
        fence = deps.operation_registrar(value.attempt, value.operation, value.write, value.mutation_plan_sha256)
    assert fence.binding == value
    assert fence.control_schema == "custom_control"


def test_production_outcome_resolves_protected_attempt_sid():
    from dpone.adapters.composition_mssql_transfer_outcome import CompositionTransferObservation
    from dpone.app import composition_transfer_execution_factory as factory

    value = binding()
    writes = []
    ledger = SimpleNamespace(
        cursor=SimpleNamespace(execute=lambda *args: None, fetchall=lambda: [(value.issued_sid,)]),
        table=lambda name: "[custom_control].[composition_" + name + "]",
    )

    @contextmanager
    def transaction(*args):
        yield ledger

    with (
        patch.object(factory, "composition_control_transaction", transaction),
        patch.object(factory, "persist_execution_proof", lambda *args, **kwargs: writes.append(args)),
    ):
        deps = _dependencies()
        deps.outcome_observer._observe = lambda attempt: CompositionTransferObservation(True, True, True, False, False)
        proof = deps.outcome_observer.observe(value.attempt)
    assert proof.authorities[0].principal_id == "mssql-sid:" + value.issued_sid.hex()
    assert len(writes) == 2


def test_protected_binding_decoder_roundtrips_exact_original():
    from dpone.app.composition_transfer_observation_factory import decode_transfer_binding

    value = binding()
    assert decode_transfer_binding(value.document, value.attempt) == value


def test_binding_decoder_rejects_changed_or_noncanonical_original():
    import json

    import pytest

    from dpone.app.composition_transfer_observation_factory import decode_transfer_binding
    from dpone.contracts.composition_activation import CompositionAdmissionError

    value = binding()
    body = json.loads(value.document)
    body["unexpected"] = True
    with pytest.raises(CompositionAdmissionError, match="transfer_binding_original"):
        decode_transfer_binding(json.dumps(body).encode(), value.attempt)


def test_fresh_receipt_reader_uses_same_connection_and_actual_generic_join():
    from dpone.app.composition_transfer_observation_factory import _ReadSession
    from dpone.runtime.state.mssql_generic_operation_state import MssqlGenericOperationState
    from tests.test_mssql_generic_transaction_governance import _receipt, _receipt_row

    receipt = _receipt(binding().operation)
    row = _receipt_row(receipt)
    queries = []
    closed = []
    cursor = SimpleNamespace(
        description=[(key,) for key in row],
        execute=lambda *args: queries.append(args),
        fetchall=lambda: [tuple(row.values())],
        close=lambda: closed.append(True),
    )
    state = MssqlGenericOperationState(
        _ReadSession(SimpleNamespace(cursor=lambda: cursor)), database="State", schema="etl"
    )
    assert state.receipt_by_key(receipt.operation_key) == receipt
    assert "[State].[etl]" in queries[0][0] and "INNER JOIN" in queries[0][0]
    assert queries[0][1] == receipt.operation_key
    assert closed == [True]


def test_protected_original_reopened_and_reverified_for_observation(monkeypatch):
    from dpone.adapters import composition_mssql_transaction_binding as factory
    from tests.composition_mssql_gate_helpers import occurrence

    value = binding()
    current = occurrence()
    queries, verified = [], []

    class Cursor:
        def execute(self, sql, *args):
            queries.append(sql)
            if "DB_NAME" in sql:
                self.rows = [("Control",)]
            elif "g.login_sid" in sql:
                self.rows = [(value.issued_sid,)]
            else:
                self.rows = [(value.digest, value.document)]

        def fetchall(self):
            return self.rows

    @contextmanager
    def transaction(*args):
        yield SimpleNamespace(
            cursor=Cursor(), terminal_validator=None, table=lambda name: "[dpone_control].[composition_" + name + "]"
        )

    monkeypatch.setattr(factory, "composition_control_transaction", transaction)
    monkeypatch.setattr(
        factory, "require_existing_execution_in", lambda *args, **kwargs: (current, SimpleNamespace(state="RUNNING"))
    )
    control = SimpleNamespace(
        connection_factory=lambda: None,
        control_schema="dpone_control",
        expected_service_id=SERVICE,
        control_database="Control",
    )
    plan = SimpleNamespace(
        sources=SimpleNamespace(subject_sha256=current.request.source_subject_sha256),
        workloads=current.request.workloads,
        writes=(value.write,),
    )
    repository = factory.MssqlCompositionTransactionBindings(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        control_database=control.control_database,
        control_schema=control.control_schema,
        read_plan=lambda attempt: plan,
        verify_operation=lambda *args: verified.append(args),
    )
    actual = repository.read_closed(value.attempt)
    assert actual.document == value.document
    assert verified == [(value.attempt, actual.operation, value.write, value.mutation_plan_sha256, plan)]
    assert any("transfer_bindings" in query for query in queries)
