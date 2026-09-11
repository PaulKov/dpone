"""Real SQL crash/race/recovery cases; fault injection never substitutes for SQL.

Commit wrappers delegate to the real DBAPI before losing a response. Gate and
quiescence documents come only from the gate producer. Test-only OUTCOME records
retain actual client events and independently reopened synthetic row observations.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from threading import Event

import pytest
from tests.integration.composition.mssql_gate_live_outcomes import AcknowledgementLost, OutcomeProducer
from tests.integration.composition.mssql_gate_live_provisioning import SqlFailure, execute
from tests.integration.composition.mssql_gate_live_support import commit_factory, require_denied, wait_until
from tests.integration.composition.mssql_store_live_support import invariant_fault, owner_key

from dpone.contracts.composition_control import CompositionAdmissionError

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]
pytest_plugins = ["tests.integration.composition.mssql_gate_live_support"]


def lost_ack():
    raise AcknowledgementLost("injected_loss_after_real_commit")


def _record_lock_snapshot(case, locker, locker_spid):
    """Observe only this fixture session's control-table locks before waiting.

    Catalog IDs and transaction state explain missing locks; they neither relax
    the exact key-lock assertion nor prove authentication rejection.
    """
    table = case.table("login_gates")
    context = execute(locker, "SELECT @@SPID,DB_ID(),@@TRANCOUNT,XACT_STATE();")
    transactions = case.sql(
        "SELECT TOP (17) t.transaction_id,a.transaction_type,a.transaction_state,t.is_user_transaction "
        "FROM sys.dm_tran_session_transactions t JOIN sys.dm_tran_active_transactions a "
        "ON a.transaction_id=t.transaction_id WHERE t.session_id=? ORDER BY t.transaction_id;",
        locker_spid,
    )
    table_identity = case.sql("SELECT DB_ID(),OBJECT_ID(?);", table)
    indexes = case.sql(
        "SELECT TOP (17) p.hobt_id,p.index_id,p.partition_number,i.type,i.is_unique,i.is_primary_key "
        "FROM sys.partitions p JOIN sys.indexes i ON i.object_id=p.object_id AND i.index_id=p.index_id "
        "WHERE p.object_id=OBJECT_ID(?) ORDER BY p.index_id,p.partition_number;",
        table,
    )
    locks = case.sql(
        "SELECT TOP (65) l.resource_type,l.request_mode,l.request_status,l.resource_database_id,"
        "l.resource_associated_entity_id,p.index_id,l.request_owner_type,l.request_owner_id,l.request_session_id "
        "FROM sys.dm_tran_locks l LEFT JOIN sys.partitions p ON p.hobt_id=l.resource_associated_entity_id "
        "WHERE l.request_session_id=? AND l.resource_database_id=DB_ID() AND "
        "((l.resource_type IN ('KEY','RID','PAGE','HOBT') AND p.object_id=OBJECT_ID(?)) OR "
        "(l.resource_type='OBJECT' AND l.resource_associated_entity_id=OBJECT_ID(?))) "
        "ORDER BY l.resource_type,l.resource_associated_entity_id,l.request_mode,l.request_owner_id;",
        locker_spid,
        table,
        table,
    )
    case.record(
        "row_lock_snapshot",
        {
            "locker_spid": locker_spid,
            "driver_autocommit": locker.autocommit,
            "locker_spid_dbid_trancount_xactstate": context,
            "transaction_id_type_state_user": transactions,
            "control_database_and_table_ids": table_identity,
            "index_hobt_id_partition_type_unique_primary": indexes,
            "lock_type_mode_status_dbid_entity_index_owner_kind_owner_id_spid": locks,
        },
    )


def _exclusive_gate_key_lock(case, locker_spid):
    """Require this session's granted exclusive key lock on the control table.

    SQL Server RangeX-X holds both an exclusive range and exclusive resource;
    it conflicts with the LOGON reader's shared and serializable shared locks.
    """
    return (
        case.sql(
            "SELECT COUNT(*) FROM sys.dm_tran_locks WHERE request_session_id=? AND resource_database_id=DB_ID() "
            "AND resource_type='KEY' AND request_mode IN ('X','RangeX-X') AND request_status='GRANT' "
            "AND resource_associated_entity_id IN (SELECT hobt_id FROM sys.partitions WHERE object_id=OBJECT_ID(?));",
            locker_spid,
            case.table("login_gates"),
        )[0][0]
        > 0
    )


def test_lost_admission_ack_never_authorizes_replay(gate_case):
    case = gate_case
    factory, opened = commit_factory(case.environment, 1, lost_ack)
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        case.attempt_store(factory).admit_once(case.attempt)
    assert len(opened) == 1
    assert case.attempts.read_exact(case.attempt).state == "RUNNING"
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        case.attempts.admit_once(case.attempt)
    case.no_issuance()


def test_lost_journal_ack_never_creates_or_reissues_login(gate_case):
    case = gate_case
    case.attempts.admit_once(case.attempt)
    factory, _ = commit_factory(case.environment, 1, lost_ack)
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        case.login_gate(factory).issue_once(case.attempt)
    original = case.gate_row()
    assert original[2] == "CLOSING" and original[3] is None
    assert case.sql("SELECT COUNT(*) FROM sys.server_principals WHERE name=? OR sid=?;", original[1], original[0]) == (
        (0,),
    )
    with pytest.raises(CompositionAdmissionError, match="login_issuance_replay"):
        case.gate.issue_once(case.attempt)
    assert case.gate_row() == original
    assert case.sql(
        f"SELECT COUNT(*) FROM {case.table('proofs')} WHERE operation_key=?;", case.attempt.attempt_sha256
    ) == ((0,),)
    case.record(
        "lost_journal_ack", {"issued_sid": original[0].hex(), "gate_state": original[2], "principal_created": False}
    )


def test_lost_ready_ack_closes_the_real_issued_login(gate_case):
    case = gate_case
    case.attempts.admit_once(case.attempt)
    factory, opened = commit_factory(case.environment, 2, lost_ack)
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        case.login_gate(factory).issue_once(case.attempt)
    original = case.gate_row()
    assert original[2] == "CLOSED"
    assert len(opened) > 2
    assert case.sql("SELECT name,sid,is_disabled FROM sys.server_principals WHERE sid=?;", original[0]) == (
        (original[1], original[0], True),
    )
    with pytest.raises(CompositionAdmissionError, match="login_issuance_replay"):
        case.gate.issue_once(case.attempt)
    assert case.gate_row() == original
    quiet = case.gate.prove_quiescence(case.attempt)
    case.record(
        "lost_ready_ack", {"issued_sid": original[0].hex(), "gate_state": original[2], "quiescence": quiet.to_dict()}
    )


def test_connection_attempts_racing_close_cannot_reconnect(gate_case):
    case = gate_case
    credentials = case.issue()
    case.worker()  # An admitted, retained session must remain visible after close.
    # Keep the exact READY row locked while the principal remains enabled.
    # The real lock catalog and trigger-specific authentication error establish
    # NOWAIT behavior; a generic disabled-login refusal cannot satisfy this.
    with closing(case.environment.connect()) as locker:
        locker.autocommit = False
        locker_spid = execute(locker, "SELECT @@SPID;")[0][0]
        execute(
            locker,
            f"SELECT login_sid FROM {case.table('login_gates')} WITH (XLOCK,HOLDLOCK,ROWLOCK) WHERE operation_key=?;",
            case.attempt.attempt_sha256,
        )
        try:
            _record_lock_snapshot(case, locker, locker_spid)
            wait_until(lambda: _exclusive_gate_key_lock(case, locker_spid))
            assert case.sql("SELECT is_disabled FROM sys.server_principals WHERE sid=?;", credentials.login_sid) == (
                (False,),
            )
            with pytest.raises(SqlFailure) as locked:
                case.worker()
            assert locked.value.code == 17892
        finally:
            locker.rollback()
    closing_committed, release_disable = Event(), Event()

    def before_disable():
        closing_committed.set()
        assert release_disable.wait(50), "closing_interlock_timeout"

    factory, _ = commit_factory(case.environment, 1, before_disable)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(case.login_gate(factory).close, case.attempt)
        try:
            assert closing_committed.wait(10), "closing_commit_not_observed"
            assert case.gate_row()[2] == "CLOSING"
            assert case.sql("SELECT is_disabled FROM sys.server_principals WHERE sid=?;", credentials.login_sid) == (
                (False,),
            )
            results = []
            for _ in range(8):
                with pytest.raises(SqlFailure) as closing_denial:
                    case.worker()
                assert closing_denial.value.code == 17892
                results.append(closing_denial.value.code)
        finally:
            release_disable.set()
        future.result(timeout=20)
    assert case.gate_row()[2] == "CLOSED"
    require_denied(lambda: case.worker())
    with pytest.raises(CompositionAdmissionError, match="login_sessions_not_quiescent"):
        case.gate.prove_quiescence(case.attempt)
    case.record(
        "connection_race",
        {
            "attempted": 8,
            "row_xlock_observed": True,
            "ready_but_locked_error": locked.value.code,
            "closing_while_enabled_error_codes": results,
            "post_closure_reconnect_denied": True,
        },
    )
    case.close_and_prove()


def test_inflight_command_and_open_transaction_block_quiescence(gate_case):
    case = gate_case
    credentials = case.issue()
    worker = case.worker()
    worker.autocommit = False
    spid = execute(worker, "SELECT @@SPID;")[0][0]
    execute(worker, "INSERT INTO [managed].[rows] VALUES (1,'uncommitted');")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(execute, worker, "WAITFOR DELAY '00:00:05';")
        wait_until(
            lambda: (
                case.sql("SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id=? AND command='WAITFOR';", spid)
                == ((1,),)
            )
        )
        case.gate.close(case.attempt)
        require_denied(lambda: case.worker())
        with pytest.raises(CompositionAdmissionError, match="login_sessions_not_quiescent"):
            case.gate.prove_quiescence(case.attempt)
        future.result(timeout=10)
    assert case.sql("SELECT COUNT(*) FROM sys.dm_tran_session_transactions WHERE session_id=?;", spid)[0][0] >= 1
    worker.rollback()
    with pytest.raises(CompositionAdmissionError, match="login_sessions_not_quiescent"):
        case.gate.prove_quiescence(case.attempt)
    case.close_and_prove()
    assert case.target_sql("SELECT COUNT(*) FROM [managed].[rows];") == ((0,),)
    case.record(
        "drain",
        {
            "issued_sid": credentials.login_sid.hex(),
            "active_request_observed": True,
            "open_transaction_observed": True,
            "rows_after_rollback": 0,
        },
    )


def test_foreign_transaction_blocks_but_observer_does_not(gate_case):
    case = gate_case
    case.issue()
    _, before = case.close_and_prove()
    with closing(case.environment.connect(database=case.target)) as foreign:
        foreign.autocommit = False
        execute(foreign, "INSERT INTO [managed].[rows] VALUES (7,'foreign transaction');")
        try:
            with pytest.raises(CompositionAdmissionError, match="login_unattributed_transactions"):
                case.gate.prove_quiescence(case.attempt)
        finally:
            foreign.rollback()
    assert case.gate.prove_quiescence(case.attempt) == before
    case.record(
        "observer_exclusion", {"own_catalog_transaction_accepted": True, "other_target_transaction_rejected": True}
    )


def test_missing_terminal_proof_blocks_overlapping_reuse(gate_case):
    case = gate_case
    case.issue()
    execute(case.worker(), "INSERT INTO [managed].[rows] VALUES (1,'terminal');")
    closed, _ = case.close_and_prove()
    outcome = OutcomeProducer(case).reconcile(((1, "terminal"),))
    assert (
        case.attempts.finalize(case.attempt, state="SUCCEEDED", outcome_evidence_sha256=outcome.proof_sha256).state
        == "SUCCEEDED"
    )
    try:
        with invariant_fault(case.environment.recover, case.environment.schema, "proofs"):
            case.sql(
                f"DELETE FROM {case.table('proofs')} WHERE operation_key=? AND kind='CLOSED_GATES' AND proof_sha256=?;",
                case.attempt.attempt_sha256,
                closed.proof_sha256,
            )
        with pytest.raises(CompositionAdmissionError, match="terminal_proof_identity"):
            case.attempts.admit_once(replace(case.attempt, try_number=2))
        assert case.sql(
            f"SELECT COUNT(*) FROM {case.table('operations')} WHERE owner_key=?;", owner_key(case.request.activation_id)
        ) == ((1,),)
    finally:
        # Restore through fresh exact SID observations and the actual producer,
        # never by pasting saved proof bytes into protected evidence storage.
        assert case.gate.close(case.attempt) == closed
    assert case.attempts.read_exact(case.attempt).state == "SUCCEEDED"


def test_real_commit_unknown_requires_explicit_reconciliation(gate_case):
    case = gate_case
    case.issue()
    worker = case.worker()
    worker.autocommit = False
    execute(worker, "INSERT INTO [managed].[rows] VALUES (1,'actually committed');")
    producer = OutcomeProducer(case)
    with pytest.raises(AcknowledgementLost):
        producer.lose_commit_ack(worker)
    case.close_and_prove()
    unknown = producer.record_unknown()
    assert (
        case.attempts.finalize(case.attempt, state="COMMIT_UNKNOWN", outcome_evidence_sha256=unknown.proof_sha256).state
        == "COMMIT_UNKNOWN"
    )
    with pytest.raises(CompositionAdmissionError, match="attempt_conflict"):
        case.attempts.admit_once(replace(case.attempt, try_number=2))
    case.store.begin_retirement(case.request)
    with pytest.raises(CompositionAdmissionError, match="unresolved_attempt"):
        case.store.finalize_retirement(case.request)
    resolved = producer.reconcile(((1, "actually committed"),))
    assert resolved.proof_sha256 != unknown.proof_sha256
    with pytest.raises(CompositionAdmissionError, match="attempt_terminal_replay"):
        case.attempts.finalize(case.attempt, state="SUCCEEDED", outcome_evidence_sha256=resolved.proof_sha256)
    receipt = case.attempts.reconcile_unknown(
        case.attempt, state="SUCCEEDED", outcome_evidence_sha256=resolved.proof_sha256
    )
    assert receipt.state == "SUCCEEDED" and case.attempts.read_exact(case.attempt) == receipt
    assert case.store.finalize_retirement(case.request).receipt.state == "RETIRED"
    assert case.target_sql("SELECT row_id,value FROM [managed].[rows];") == ((1, "actually committed"),)
    assert case.sql(
        f"SELECT COUNT(*) FROM {case.table('proofs')} WHERE operation_key=? AND kind='OUTCOME';",
        case.attempt.attempt_sha256,
    ) == ((2,),)
