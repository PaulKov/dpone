"""Actual issued-login SQL fence components, not signed route certification.

The support fixture supplies synthetic source/plan metadata. Every authority,
permission, transaction and row observation below uses the disposable SQL server.
"""

import pytest
from tests.integration.composition.mssql_transfer_fence_live_support import (
    observe_original_transaction,
    observe_worker_transaction,
)

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]
pytest_plugins = [
    "tests.integration.composition.mssql_gate_live_support",
    "tests.integration.composition.mssql_transfer_fence_live_support",
]


def test_transfer_exact_binding_commits_actual_target_rows(transfer_case):
    case = transfer_case
    case.bind_worker()
    worker = case.worker()
    transaction = case.begin_fence(worker)
    case.worker_sql(worker, "INSERT INTO [managed].[rows] VALUES (1,'component');")
    assert case.require_fence(worker, transaction_id=transaction) == transaction
    before = observe_worker_transaction(worker)
    case.record("transfer_before_commit", before)
    assert before["transaction_count"] == 1 and before["transaction_state"] == 1
    original = observe_original_transaction(case, transaction)
    case.record("transfer_original_before_commit", original)
    assert original["active_session_count"] == 1
    worker.commit()
    original = observe_original_transaction(case, transaction)
    case.record("transfer_original_after_commit", original)
    assert original["active_session_count"] == 0
    assert case.target_sql("SELECT row_id,value FROM [managed].[rows];") == ((1, "component"),)
    after = observe_worker_transaction(worker)
    case.record("transfer_after_commit", after)
    assert case.target != case.environment.database.database
    assert case.binding.issued_sid != case.environment.controller_sid
    case.record(
        "transfer_commit",
        {
            "transaction_id": transaction,
            "target_rows": 1,
            "binding_sha256": case.binding.digest.hex(),
            "route_certification": False,
        },
    )


def test_transfer_rollback_releases_transaction_without_committing_rows(transfer_case):
    case = transfer_case
    case.bind_worker()
    worker = case.worker()
    transaction = case.begin_fence(worker)
    case.worker_sql(worker, "INSERT INTO [managed].[rows] VALUES (1,'component');")
    assert case.require_fence(worker, transaction_id=transaction) == transaction
    before = observe_worker_transaction(worker)
    case.record("transfer_before_rollback", before)
    assert before["transaction_count"] == 1 and before["transaction_state"] == 1
    original = observe_original_transaction(case, transaction)
    case.record("transfer_original_before_rollback", original)
    assert original["active_session_count"] == 1
    worker.rollback()
    original = observe_original_transaction(case, transaction)
    case.record("transfer_original_after_rollback", original)
    assert original["active_session_count"] == 0
    assert case.target_sql("SELECT COUNT(*) FROM [managed].[rows];") == ((0,),)
    after = observe_worker_transaction(worker)
    case.record("transfer_after_rollback", after)


def test_transfer_foreign_operation_and_receipt_cannot_mutate(transfer_case):
    from dataclasses import replace
    from types import SimpleNamespace

    from tests.test_mssql_generic_transaction_governance import _operation, _receipt

    from dpone.contracts.composition_activation import CompositionAdmissionError

    case = transfer_case
    case.bind_worker()
    worker = case.worker()
    case.begin_worker(worker)
    connector = SimpleNamespace(connection=worker)
    for field, value in (
        ("target_schema", "unmanaged"),
        ("target_table", "foreign"),
        ("strategy", "incremental_append"),
    ):
        request = replace(case.operation.attempt.request, **{field: value})
        operation = replace(case.operation, attempt=replace(case.operation.attempt, request=request))
        with pytest.raises(CompositionAdmissionError, match="transfer_operation_binding"):
            case.fence.require_current(connector, operation, mutation_plan_sha256=case.mutation_digest)
    request = case.operation.attempt.request
    invocation = replace(request.invocation, run_id="foreign")
    operation = replace(
        case.operation, attempt=replace(case.operation.attempt, request=replace(request, invocation=invocation))
    )
    with pytest.raises(CompositionAdmissionError, match="transfer_operation_binding"):
        case.fence.require_current(connector, operation, mutation_plan_sha256=case.mutation_digest)
    # A foreign unit-fixture envelope is rejection input, never committed evidence.
    with pytest.raises(CompositionAdmissionError, match="transfer_receipt_binding"):
        case.fence.require_current(connector, receipt=_receipt(_operation()))
    worker.rollback()
    assert case.target_sql("SELECT COUNT(*) FROM [managed].[rows];") == ((0,),)


def test_transfer_server_rejects_foreign_binding_digest_and_bytes(transfer_case):
    from tests.integration.composition.mssql_store_live_support import SqlFailure

    case = transfer_case
    case.bind_worker()
    worker = case.worker()
    for changed in ({"digest": b"x" * 32}, {"document": case.binding.document + b" "}):
        case.begin_worker(worker)
        with pytest.raises(SqlFailure) as caught:
            case.direct_fence(worker, **changed)
        assert caught.value.code == 51000
        worker.rollback()
    assert case.target_sql("SELECT COUNT(*) FROM [managed].[rows];") == ((0,),)


def test_transfer_server_rejects_stale_domain_epoch(transfer_case):
    from tests.integration.composition.mssql_store_live_support import SqlFailure, invariant_fault

    case = transfer_case
    case.bind_worker()
    guard, epoch = case.attempt.guard_epochs[0]

    def change(value):
        with invariant_fault(case.sql, case.environment.schema, "domains"):
            case.sql(f"UPDATE {case.table('domains')} SET fencing_epoch=? WHERE guard_id=?;", value, guard)

    worker = case.worker()
    try:
        change(epoch + 1)
        case.begin_worker(worker)
        with pytest.raises(SqlFailure) as caught:
            case.direct_fence(worker)
        assert caught.value.code == 51000
        worker.rollback()
    finally:
        worker.rollback()
        change(epoch)
    assert case.target_sql("SELECT COUNT(*) FROM [managed].[rows];") == ((0,),)


def test_transfer_server_rejects_retiring_parent(transfer_case):
    from tests.integration.composition.mssql_store_live_support import SqlFailure

    case = transfer_case
    case.bind_worker()
    worker = case.worker()
    case.store.begin_retirement(case.request)
    case.begin_worker(worker)
    with pytest.raises(SqlFailure) as caught:
        case.direct_fence(worker)
    assert caught.value.code == 51000
    worker.rollback()
    assert case.target_sql("SELECT COUNT(*) FROM [managed].[rows];") == ((0,),)


def test_transfer_closed_gate_denies_existing_worker_transaction(transfer_case):
    from tests.integration.composition.mssql_gate_live_support import require_denied
    from tests.integration.composition.mssql_store_live_support import SqlFailure

    case = transfer_case
    case.bind_worker()
    worker = case.worker()
    case.gate.close(case.attempt)
    assert case.gate_row()[2] == "CLOSED"
    require_denied(lambda: case.worker())
    case.begin_worker(worker)
    with pytest.raises(SqlFailure) as caught:
        case.direct_fence(worker)
    assert caught.value.code == 51000
    worker.rollback()
    assert case.target_sql("SELECT COUNT(*) FROM [managed].[rows];") == ((0,),)


def test_transfer_worker_has_only_control_procedure_permission(transfer_case):
    from tests.integration.composition.mssql_gate_live_support import require_denied

    case = transfer_case
    case.bind_worker()
    worker = case.worker()
    control = case.environment.database.database
    name = case.credentials.login_name
    for statement in (
        f"SELECT * FROM [{control}].{case.table('owners')};",
        f"UPDATE [{control}].{case.table('login_gates')} SET gate_state='CLOSING';",
        f"ALTER SERVER ROLE [sysadmin] ADD MEMBER [{name}];",
        "EXECUTE AS LOGIN='sa';",
    ):
        require_denied(lambda statement=statement: case.worker_sql(worker, statement))
    permissions = case.sql(
        "SELECT x.permission_name,x.class,x.major_id FROM sys.database_permissions x "
        "JOIN sys.database_principals p ON p.principal_id=x.grantee_principal_id "
        "WHERE p.sid=? ORDER BY x.permission_name;",
        case.credentials.login_sid,
    )
    procedure = case.sql("SELECT OBJECT_ID(?);", f"[{case.environment.schema}].[composition_require_transfer]")[0][0]
    assert permissions in ((("EXECUTE", 1, procedure),), (("CONNECT", 0, 0), ("EXECUTE", 1, procedure)))
    assert case.sql(
        "SELECT COUNT(*) FROM sys.database_role_members r JOIN sys.database_principals p "
        "ON p.principal_id=r.member_principal_id WHERE p.sid=?;",
        case.credentials.login_sid,
    ) == ((0,),)
    assert case.gate_row()[2] == "READY"
    assert case.begin_fence(worker) > 0
    worker.rollback()


def test_transfer_public_permission_drift_prevents_ready_gate(transfer_case):
    from dpone.contracts.composition_activation import CompositionAdmissionError

    case = transfer_case
    case.attempts.admit_once(case.attempt)
    try:
        case.sql(f"GRANT SELECT ON OBJECT::{case.table('owners')} TO [public];")
        with pytest.raises(CompositionAdmissionError, match="transfer_control_user_policy"):
            case.gate.issue_once(case.attempt)
        assert case.gate_row()[2] != "READY"
    finally:
        case.sql(f"REVOKE SELECT ON OBJECT::{case.table('owners')} FROM [public];")
    assert case.target_sql("SELECT COUNT(*) FROM [managed].[rows];") == ((0,),)
