"""Real control-ledger SQL tests; no routes, ClickHouse service or LOGON proof.

Risk matrix: lifecycle/metadata closure (positive and compatibility), immutable
retry (idempotency), conflict/injected epoch corruption (negative), concurrent
prepare (isolation), committed ACK loss (recovery), corruption/foreign authority
(fail closed), unresolved attempts (retirement safety). All SQL uses the newly
disposable runner database; synthetic identities are never claimed as live gates.
"""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
from uuid import uuid4

import pytest
from tests.integration.composition import mssql_store_live_support as support

from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_persistence import encode_activation_request, encode_attempt_identity

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]
sql_case = support.sql_case


def successor(request):
    return replace(request, context=replace(request.context, activation_id=str(uuid4())))


def test_external_ddl_and_mixed_engine_lifecycle(sql_case, record_property):
    case = sql_case
    store = case.store()
    with pytest.raises(CompositionAdmissionError):
        store.prepare(case.request)
    assert case.sql("SELECT COUNT(*) FROM sys.schemas WHERE name = ?", case.schema) == ((0,),)
    case.install(record_property)
    request = case.request
    prepared = store.prepare(request)
    assert prepared.receipt.state == "PREPARED"
    assert store.prepare(request) == prepared
    assert store.read(request.activation_id) == prepared
    rows = case.domains()
    assert tuple(row[:4] for row in rows) == tuple(
        (r.guard_id, r.connector, r.service_id, r.physical_subject_sha256) for r in request.resources
    )
    assert all(row[4:] == (1, support.owner_key(request.activation_id)) for row in rows)
    assert case.sql(f"SELECT owner_kind,subject_document FROM {case.table('owners')}") == (
        ("execution", encode_activation_request(request)),
    )
    for operation, state in (
        (store.activate, "ACTIVE"),
        (store.begin_retirement, "RETIRING"),
        (store.finalize_retirement, "RETIRED"),
    ):
        result = operation(request)
        assert result.receipt.state == state
        assert result.request == request
        assert result.receipt.guard_epochs == prepared.receipt.guard_epochs
        assert operation(request) == result
        assert case.sql(f"SELECT state FROM {case.table('owners')}") == ((state,),)
    assert all(row[4:] == (1, None) for row in case.domains())
    next_request = successor(request)
    next_occurrence = store.prepare(next_request)
    assert all(epoch == 2 for _, epoch in next_occurrence.receipt.guard_epochs)
    assert all(row[4:] == (2, support.owner_key(next_request.activation_id)) for row in case.domains())
    assert store.read(request.activation_id).receipt.state == "RETIRED"


def test_conflict_and_epoch_ceiling_leave_no_partial_mutation(sql_case, record_property):
    """Injected max is invalid retained history, not a valid-history overflow proof."""
    case = sql_case
    case.install()
    store = case.store()
    store.prepare(case.request)
    before = case.snapshot()
    with pytest.raises(CompositionAdmissionError, match="guard_conflict"):
        store.prepare(successor(case.request))
    assert case.snapshot() == before
    for operation in (store.activate, store.begin_retirement, store.finalize_retirement):
        operation(case.request)
    with support.invariant_fault(case.sql, case.schema, "domains"):
        case.sql(
            f"UPDATE {case.table('domains')} SET fencing_epoch = 9223372036854775807 WHERE guard_id = ?",
            case.request.resources[-1].guard_id,
        )
    before = case.snapshot()
    with pytest.raises(CompositionAdmissionError) as rejected:
        store.prepare(successor(case.request))
    assert rejected.value.reason == "guard_readback"
    assert case.snapshot() == before
    record_property(
        "dpone.composition.injected_max_epoch",
        json.dumps({"reason": "guard_readback", "valid_history_overflow": False}),
    )


def test_concurrent_same_domain_prepare_has_one_winner(sql_case):
    case = sql_case
    case.install()
    barrier = Barrier(2)
    requests = (case.request, successor(case.request))

    def prepare(request):
        barrier.wait(timeout=15)
        try:
            return case.store().prepare(request)
        except CompositionAdmissionError as error:
            return error.reason

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(prepare, requests))
    winners = [result for result in results if not isinstance(result, str)]
    assert len(winners) <= 1
    assert all(
        result in {"ledger_lock", "guard_conflict", "commit_unknown"} for result in results if isinstance(result, str)
    )
    # A contender can temporarily own the global lock during postcommit
    # readback. Reconcile any uncertain acknowledgement using independent SQL;
    # exactly one durable owner must exist and no mutation is replayed.
    durable = case.sql(
        f"SELECT LOWER(CONVERT(char(36), owner_id)) FROM {case.table('owners')} WHERE owner_kind='execution'"
    )
    assert len(durable) == 1
    winner_id = durable[0][0]
    assert winner_id in {request.activation_id for request in requests}
    assert all(result.request.activation_id == winner_id for result in winners)
    assert case.store().read(winner_id).receipt.state == "PREPARED"
    assert all(row[4:] == (1, support.owner_key(winner_id)) for row in case.domains())
    assert case.sql(f"SELECT COUNT(*) FROM {case.table('owner_domains')}") == ((len(case.request.resources),),)


def test_lost_commit_ack_uses_independent_sql_readback(sql_case):
    case = sql_case
    case.install()
    connections, commits = [], []

    class LostAcknowledgement:
        """Delegate real DBAPI SQL/commit, then lose only the client response."""

        def __init__(self, connection):
            self.connection = connection

        @property
        def autocommit(self):
            return self.connection.autocommit

        @autocommit.setter
        def autocommit(self, value):
            self.connection.autocommit = value

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def commit(self):
            self.connection.commit()
            commits.append("actual_server_commit_completed")
            raise ConnectionError("deliberately_lost_commit_ack")

    def factory():
        connection = case.database.connect()
        connections.append(connection)
        return LostAcknowledgement(connection) if len(connections) == 1 else connection

    result = case.store(factory).prepare(case.request)
    assert len(connections) == 2 and connections[0] is not connections[1]
    assert commits == ["actual_server_commit_completed"]
    assert result == case.store().read(case.request.activation_id)
    assert case.sql(f"SELECT COUNT(*) FROM {case.table('owners')}") == ((1,),)
    assert all(row[4:] == (1, support.owner_key(case.request.activation_id)) for row in case.domains())


def test_foreign_control_identity_and_corrupt_request_fail_closed(sql_case):
    case = sql_case
    case.install()
    before = case.snapshot()
    with pytest.raises(CompositionAdmissionError, match="control_authority"):
        case.store(service_id=str(uuid4())).prepare(case.request)
    assert case.snapshot() == before
    store = case.store()
    store.prepare(case.request)
    with support.invariant_fault(case.sql, case.schema, "owners"):
        case.sql(
            f"UPDATE {case.table('owners')} SET subject_document = ?", encode_activation_request(case.request) + b" "
        )
    before = case.snapshot()
    with pytest.raises(CompositionAdmissionError, match="persistence_readback"):
        store.read(case.request.activation_id)
    with pytest.raises(CompositionAdmissionError, match="persistence_readback"):
        store.activate(case.request)
    assert case.snapshot() == before


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN"])
def test_unresolved_attempt_blocks_retirement(sql_case, state):
    case = sql_case
    case.install()
    store = case.store()
    store.prepare(case.request)
    active = store.activate(case.request)
    workload = case.request.workloads[0]
    guards = {
        resource.guard_id
        for resource in case.request.resources
        if set(resource.write_subjects).intersection(workload.write_subjects)
    }
    attempt = CompositionAttemptIdentity(
        case.request.request_sha256,
        workload.workload_id,
        workload.constituent_id,
        workload.pack_sha256,
        support.digest("synthetic plan"),
        "synthetic component run",
        "task",
        1,
        -1,
        tuple(pair for pair in active.receipt.guard_epochs if pair[0] in guards),
    )
    seed_unresolved(case, attempt, state)
    retiring = store.begin_retirement(case.request)
    before = case.snapshot()
    with pytest.raises(CompositionAdmissionError, match="unresolved_attempt"):
        store.finalize_retirement(case.request)
    assert store.read(case.request.activation_id) == retiring
    assert case.snapshot() == before
    assert case.sql(
        f"SELECT state, closed_gates_sha256, quiescence_sha256, outcome_evidence_sha256 FROM {case.table('operations')}"
    ) == ((state, None, None, None),)


def seed_unresolved(case, attempt, state):
    """Insert complete original scope under the real shared transaction lock."""
    owner = support.owner_key(case.request.activation_id)
    with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
        ledger.cursor.execute(
            f"INSERT INTO {ledger.table('operations')} "
            "(operation_key,operation_family,owner_key,owner_subject_sha256,replay_key,operation_document,state) "
            "VALUES (?,'execution',?,?,?,?,'RUNNING');",
            attempt.attempt_sha256,
            owner,
            case.request.request_sha256,
            attempt.attempt_sha256,
            encode_attempt_identity(attempt),
        )
        support.drain_results(ledger.cursor)
        for guard, epoch in attempt.guard_epochs:
            ledger.cursor.execute(
                f"INSERT INTO {ledger.table('operation_domains')} (operation_key,owner_key,guard_id,fencing_epoch) VALUES (?,?,?,?);",
                attempt.attempt_sha256,
                owner,
                guard,
                epoch,
            )
            support.drain_results(ledger.cursor)
        if state == "COMMIT_UNKNOWN":
            ledger.cursor.execute(
                f"UPDATE {ledger.table('operations')} SET state='COMMIT_UNKNOWN' WHERE operation_key=?;",
                attempt.attempt_sha256,
            )
            support.drain_results(ledger.cursor)
