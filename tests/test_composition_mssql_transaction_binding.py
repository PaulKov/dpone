"""Offline identity and provisioning contracts; no live worker certification."""

from dataclasses import replace

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_mssql_binding import stable_operation_document
from tests.test_mssql_generic_transaction_governance import _operation


def test_stable_operation_binds_full_target_and_invocation():
    operation = _operation()
    original = stable_operation_document(operation)
    for field, value in (("target_table", "foreign"), ("target_schema", "foreign"), ("strategy", "full_refresh")):
        request = replace(operation.attempt.request, **{field: value})
        changed = replace(operation, attempt=replace(operation.attempt, request=request))
        assert stable_operation_document(changed) != original


def test_stable_operation_excludes_renewable_lease_only():
    operation = _operation()
    assert stable_operation_document(replace(operation, lease_expires_at_utc=None)) == stable_operation_document(
        operation
    )
    assert stable_operation_document(replace(operation, epoch=operation.epoch + 1)) != stable_operation_document(
        operation
    )


def test_invalid_operation_rejects():
    with pytest.raises(CompositionAdmissionError):
        stable_operation_document(None)


def test_renderer_is_external_cross_database_owner_module():
    from dpone.adapters.composition_mssql_transaction_fence_schema import (
        render_composition_mssql_transaction_fence,
        transfer_procedure_sql,
    )

    sql = render_composition_mssql_transaction_fence(control_database="Control")
    module = transfer_procedure_sql()
    assert "USE [Control]" in sql
    assert "WITH EXECUTE AS OWNER" in module
    assert "sys.sp_getapplock" in module and "@LockOwner=N'Transaction'" in module
    for authority in (
        "authority",
        "owners",
        "operations",
        "operation_domains",
        "owner_domains",
        "domains",
        "issued_authorities",
        "login_gates",
    ):
        assert "[composition_" + authority + "]" in module
    assert "SUSER_SID(ORIGINAL_LOGIN())" in module
    assert "HASHBYTES('SHA2_256',binding_document)" in module
    assert "UPDATE " not in module and "INSERT " not in module and "DELETE " not in module
    assert "COMMIT " not in module and "ROLLBACK " not in module and "EXEC(" not in module


def test_exact_module_catalog_rejects_changed_hash(monkeypatch):
    import dpone.adapters.composition_mssql_transaction_fence_schema as schema
    from tests.composition_mssql_gate_helpers import Cursor

    monkeypatch.setattr(schema, "inspect_composition_table", lambda *args: None)
    row = ("P", 1, 1, -2, schema.module_sha256(schema.transfer_procedure_sql()), 0, 0, None, 1)
    schema.require_transaction_fence_schema(Cursor((("sys.procedures", (row,)),)))
    for index, value in ((4, b"x" * 32), (3, None), (8, 2), (5, 1)):
        changed = (*row[:index], value, *row[index + 1 :])
        with pytest.raises(CompositionAdmissionError, match="transfer_module_schema"):
            schema.require_transaction_fence_schema(Cursor((("sys.procedures", (changed,)),)))


def test_binding_rejects_changed_original_parent_and_sid():
    from tests.test_mssql_composition_transaction_fence import binding

    value = binding()
    for fields in (
        {"parent_document": b"{}"},
        {"issued_sid": b"short"},
        {"control_database": value.write.database},
        {"write": replace(value.write, relation="foreign")},
    ):
        with pytest.raises(CompositionAdmissionError):
            replace(value, **fields)


def registration_case(monkeypatch, *, wrong_source=False, missing_sid=False, changed_readback=False):
    """Explicit ledger/history/source doubles isolate registration ordering."""
    from contextlib import contextmanager
    from types import SimpleNamespace

    import dpone.adapters.composition_mssql_transaction_binding as adapter
    from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionActivationReceipt
    from dpone.contracts.composition_execution import CompositionExecutionPlan
    from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject
    from tests.composition_mssql_gate_helpers import SERVICE, occurrence
    from tests.test_mssql_composition_transaction_fence import binding

    original = binding()
    parent = occurrence().request
    write = original.write
    native_write = replace(write, resource_id="native", kind="model")
    workloads = (
        replace(parent.workloads[0], write_subjects=(dbt_relation_write_subject(native_write),)),
        replace(parent.workloads[1], write_subjects=(dbt_relation_write_subject(write),)),
    )
    parent = replace(
        parent,
        workloads=workloads,
        resources=(
            replace(
                parent.resources[0],
                write_subjects=tuple(
                    sorted((dbt_relation_write_subject(native_write), dbt_relation_write_subject(write)))
                ),
            ),
        ),
    )
    epochs = ((parent.resources[0].guard_id, 1),)
    occurrence_value = CompositionActivationOccurrence(
        parent, CompositionActivationReceipt(parent.request_sha256, "ACTIVE", epochs)
    )
    identity = replace(
        original.attempt,
        activation_request_sha256=parent.request_sha256,
        workload_id=workloads[1].workload_id,
        constituent_id=workloads[1].constituent_id,
        pack_sha256=workloads[1].pack_sha256,
        guard_epochs=epochs,
    )
    sources = SimpleNamespace(
        __post_init__=lambda: None,
        workload_pins=tuple((row.workload_id, row.pack_sha256) for row in workloads),
        relation_writes=(native_write, write),
        subject_sha256="wrong" if wrong_source else parent.source_subject_sha256,
    )
    plan = CompositionExecutionPlan(sources, workloads, sources.relation_writes)
    events = []

    class Cursor:
        stored = None
        rows = ()

        def execute(self, sql, *parameters):
            events.append(sql)
            if sql == "SELECT DB_NAME();":
                self.rows = (("Control",),)
            elif "g.login_sid FROM" in sql:
                self.rows = () if missing_sid else ((b"s" * 16,),)
            elif "SELECT TOP (2) binding_document" in sql:
                self.rows = () if self.stored is None else ((self.stored[1],),)
            elif sql.startswith("INSERT"):
                self.stored = parameters
            else:
                self.rows = (("corrupt",),) if changed_readback else (self.stored,)

        def fetchall(self):
            return self.rows

    cursor = Cursor()
    ledger = SimpleNamespace(
        cursor=cursor, terminal_validator=object(), table=lambda name: "[dpone_control].[composition_" + name + "]"
    )

    @contextmanager
    def transaction(*args):
        events.append("begin")
        try:
            yield ledger
        except Exception:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    monkeypatch.setattr(adapter, "composition_control_transaction", transaction)
    monkeypatch.setattr(adapter, "require_transaction_fence_schema", lambda *args: events.append("schema"))

    def history(*args, **kwargs):
        events.append("history")
        return occurrence_value, SimpleNamespace(state="RUNNING")

    monkeypatch.setattr(adapter, "require_existing_execution_in", history)

    def verify(*args):
        events.append("verify")
        assert args == (identity, original.operation, write, original.mutation_plan_sha256, plan)

    registrar = adapter.MssqlCompositionTransactionBindings(
        lambda: None,
        expected_service_id=SERVICE,
        control_database="Control",
        read_plan=lambda identity: plan,
        verify_operation=verify,
    )
    return registrar, (identity, original.operation, write, original.mutation_plan_sha256), events


def test_registration_audits_originals_and_commits_before_worker_binding(monkeypatch):
    registrar, arguments, events = registration_case(monkeypatch)
    first = registrar.bind(*arguments)
    assert events.index("verify") < events.index("history")
    assert events.index("history") < next(index for index, event in enumerate(events) if event.startswith("INSERT"))
    assert events[-1] == "commit"
    assert registrar.bind(*arguments) == first
    assert sum(event.startswith("INSERT") for event in events) == 1


@pytest.mark.parametrize("fault", ["wrong_source", "missing_sid", "changed_readback"])
def test_registration_fault_cannot_return_authority(monkeypatch, fault):
    registrar, arguments, events = registration_case(monkeypatch, **{fault: True})
    with pytest.raises(CompositionAdmissionError):
        registrar.bind(*arguments)
    assert events[-1] == "rollback"
    assert "commit" not in events
