"""Parent-authority tests for spawned MSSQL backfill process lanes."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from multiprocessing import get_context
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.backfill import normalize_backfill_execution_policy, plan_chunks, plan_hash
from dpone.backfill.process_lane_contracts import (
    ProcessLaneBinding,
    ProcessLaneDispatch,
    ProcessLaneOperationLeaseReply,
    ProcessLaneOperationLeaseRequest,
)
from dpone.backfill.process_lane_operation import ParentOperationLeaseCoordinator
from dpone.backfill.runtime_execution import BackfillChunkExecutor
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_transaction_governance import (
    MssqlGenericCommitReceipt,
    MssqlPayloadCommitEvidence,
    MssqlReceiptMetrics,
    MssqlSourceLifecycleEvidence,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)
from dpone.contracts.run_context import RunContext
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.runtime.etl import backfill_process_runtime as process_runtime
from dpone.runtime.etl.backfill_process_runtime import _identity_config, _ParentMssqlLaneAuthority
from dpone.runtime.etl.mssql_transaction_identity import (
    build_mssql_attempt_request,
    invocation_identity,
    operation_request,
)


class _StateStorage:
    atomicity = "target_atomic"
    database = "DWH"
    schema = "audit"

    def __init__(self) -> None:
        self.connector = object()
        self.binding_checks = 0
        self.authority_checks = 0

    def require_database_authority_binding(self) -> None:
        self.binding_checks += 1

    def verify_database_authority(self, connector: Any) -> None:
        assert connector is not None
        self.authority_checks += 1


def _authority_case() -> tuple[_ParentMssqlLaneAuthority, ProcessLaneDispatch, MssqlTransactionOperation, datetime]:
    expiry = datetime.now(UTC) + timedelta(hours=1)
    config = LoadConfig(
        source_conn_id="source-ref",
        target_conn_id="target-ref",
        source_database="SourceDB",
        source_schema="dbo",
        source_table="orders",
        target_database="DWH",
        target_schema="dbo",
        target_table="orders_shadow",
        load_strategy=LoadStrategy.BACKFILL,
        options={
            "source_type": "mssql",
            "sink_type": "mssql",
            "backfill": {
                "inner_mode": "partition_replace",
                "chunk": {"column": "id", "from": 0, "to": 20, "step": 10},
            },
        },
    )
    policy = normalize_backfill_execution_policy(config.options["backfill"])
    assert policy.chunk is not None
    chunks = plan_chunks(policy.chunk, run_key="campaign-a")
    chunk = chunks[0]
    chunk_config = BackfillChunkExecutor.build_chunk_load_config(
        config,
        chunk,
        run_key="campaign-a",
        plan_hash=plan_hash(chunks, execution_policy=policy),
        owner="owner-1",
        lease_expires_at_utc=expiry,
    )
    run_context = RunContext(
        "scheduler-run",
        config={"process": "orders", "pipeline_id": "sales", "task_id": "initial"},
    )
    storage = _StateStorage()
    sink = SimpleNamespace(connector=object(), state_storage=storage)
    physical = SimpleNamespace(
        digest=b"t" * 32,
        database_name="DWH",
        schema_name="dbo",
        table_name="orders_shadow",
    )
    source_identity = SourcePhysicalIdentity(
        dialect="mssql",
        cluster_identifier="source-cluster",
        database="SourceDB",
        effective_principal="reader",
        session_principal="reader",
    )
    target_calls: list[tuple[str, str, str]] = []
    source_calls: list[LoadConfig] = []

    def target_resolver(_target, _state, *, database, schema, table):
        target_calls.append((database, schema, table))
        return physical

    def source_resolver(_source, load_config):
        source_calls.append(load_config)
        return source_identity

    authority = _ParentMssqlLaneAuthority(
        storage,
        source=object(),
        sink=sink,
        run_context=run_context,
        dag_id="DAG__sales__orders__initial",
        target_resolver=target_resolver,
        source_identity_resolver=source_resolver,
    )
    authority._test_calls = (target_calls, source_calls, storage)  # type: ignore[attr-defined]
    binding = ProcessLaneBinding("campaign-a", chunk.index, "owner-1")
    dispatch = ProcessLaneDispatch("command-1", chunk_config, binding, parent_context=object())
    identity_config = _identity_config(chunk_config)
    invocation = invocation_identity(run_context, identity_config, dag_id="DAG__sales__orders__initial")
    request = build_mssql_attempt_request(
        identity_config,
        invocation=invocation,
        target_identity=physical.digest,
        source_identity=source_identity,
        load_id="ephemeral-child-load-id",
        request_coordinates=("DWH", "dbo", "orders_shadow"),
    )
    attempt = MssqlTransactionAttempt(request, generation=3)
    requested_operation = operation_request(identity_config, invocation)
    operation = MssqlTransactionOperation(
        attempt=attempt,
        operation_key=requested_operation.operation_key(attempt),
        scope_hash=requested_operation.scope_hash,
        owner_digest=requested_operation.owner_digest,
        epoch=5,
        lease_expires_at_utc=requested_operation.lease_expires_at_utc,
    )
    return authority, dispatch, operation, expiry


def _replace_attempt_request(
    operation: MssqlTransactionOperation,
    **changes: Any,
) -> MssqlTransactionOperation:
    request = replace(operation.attempt.request, **changes)
    attempt = replace(operation.attempt, request=request)
    return replace(operation, attempt=attempt)


def _receipt_for(attempt, requested_operation) -> MssqlGenericCommitReceipt:
    now = datetime.now(UTC)
    operation_key = requested_operation.operation_key(attempt)
    return MssqlGenericCommitReceipt(
        receipt_id=f"mssql-generic-v1:{operation_key.hex()}",
        operation_key=operation_key,
        attempt_key=attempt.attempt_key,
        target_identity=attempt.target_identity,
        generation=1,
        scope_hash=requested_operation.scope_hash,
        operation_epoch=1,
        owner_digest=requested_operation.owner_digest,
        route_fingerprint=attempt.route_fingerprint,
        load_id=attempt.load_id,
        strategy=attempt.strategy,
        mutation_plan_sha256=b"m" * 32,
        target_before_sha256=b"b" * 32,
        target_after_sha256=b"a" * 32,
        loaded_at_utc=now,
        committed_at_utc=now,
        payload_evidence=MssqlPayloadCommitEvidence(b"p" * 32, 3, 3, 3, b"n" * 32),
        source_lifecycle=MssqlSourceLifecycleEvidence(now, now, "test-clock"),
        metrics=MssqlReceiptMetrics(
            inserted_rows=2,
            updated_rows=1,
            total_rows=10,
            staging_rows=3,
            unchanged_rows=7,
        ),
    )


@pytest.mark.parametrize("tamper", ["process", "task", "target", "route", "expiry", "current"])
def test_parent_authority_rejects_child_derived_identity_fields(tamper: str) -> None:
    authority, dispatch, operation, _expiry = _authority_case()

    if tamper in {"process", "task"}:
        invocation = operation.attempt.request.invocation
        changed = replace(
            invocation,
            process="forged-process" if tamper == "process" else invocation.process,
            task_partition="forged:task" if tamper == "task" else invocation.task_partition,
        )
        operation = _replace_attempt_request(operation, invocation=changed)
    elif tamper == "target":
        operation = _replace_attempt_request(operation, target_identity=b"x" * 32)
    elif tamper == "route":
        operation = _replace_attempt_request(operation, route_fingerprint=b"x" * 32)
    elif tamper == "expiry":
        operation = replace(operation, lease_expires_at_utc=operation.lease_expires_at_utc + timedelta(minutes=5))
    else:
        operation = replace(operation, attempt=replace(operation.attempt, is_current_generation=False))

    assert authority.validate_operation_binding(dispatch, operation, None) is False


def test_parent_authority_accepts_exact_binding_and_caches_physical_authority() -> None:
    authority, dispatch, operation, _expiry = _authority_case()

    assert authority.validate_operation_binding(dispatch, operation, None) is True
    assert authority.validate_operation_binding(dispatch, operation, None) is True

    target_calls, source_calls, storage = authority._test_calls  # type: ignore[attr-defined]
    assert target_calls == [("DWH", "dbo", "orders_shadow")]
    assert source_calls == [_identity_config(dispatch.load_config)]
    assert storage.binding_checks == storage.authority_checks == 1


def test_parent_receipt_authority_is_warm_before_child_probe() -> None:
    authority, dispatch, _operation, _expiry = _authority_case()

    assert authority.prepare_dispatch(dispatch) is dispatch
    target_calls, source_calls, storage = authority._test_calls  # type: ignore[attr-defined]
    assert target_calls == [("DWH", "dbo", "orders_shadow")]
    assert source_calls == [_identity_config(dispatch.load_config)]
    assert storage.binding_checks == storage.authority_checks == 1

    assert authority.issue_receipt_probe(dispatch, "replay-load", None) is not None
    assert target_calls == [("DWH", "dbo", "orders_shadow")]
    assert source_calls == [_identity_config(dispatch.load_config)]
    assert storage.binding_checks == storage.authority_checks == 1


def test_runtime_composes_dispatch_preflight_before_receipt_authority_warm(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class DispatchAuthority:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def prepare_dispatch(self, dispatch: ProcessLaneDispatch) -> ProcessLaneDispatch:
            calls.append("dispatch")
            return dispatch

    class ReceiptAuthority:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.receipt_recovery = object()

        def prepare_dispatch(self, dispatch: ProcessLaneDispatch) -> ProcessLaneDispatch:
            calls.append("receipt")
            return dispatch

        def renew_operation_lease(self, *_args: object) -> bool:
            return True

        def validate_operation_binding(self, *_args: object) -> bool:
            return True

        def issue_receipt_probe(self, *_args: object) -> None:
            return None

        def validate_replay_result(self, *_args: object) -> bool:
            return True

    monkeypatch.setattr(process_runtime, "_ParentDispatchAuthority", DispatchAuthority)
    monkeypatch.setattr(process_runtime, "_ParentMssqlLaneAuthority", ReceiptAuthority)
    monkeypatch.setattr(process_runtime, "PortableScopeColumnResolver", lambda **_kwargs: object())
    monkeypatch.setattr(
        process_runtime,
        "build_mssql_backfill_portable_scope_history_proof",
        lambda **_kwargs: object(),
    )
    config = SimpleNamespace(
        raw_config={"pipeline": "orders"},
        source_obj=object(),
        sink_obj=SimpleNamespace(state_storage=object()),
    )
    runtime = process_runtime.build_backfill_process_lane_runtime(
        config,
        run_context=RunContext("scheduler-run"),
        dag_id="DAG__sales__orders__initial",
        execution_date=None,
    )
    dispatch = ProcessLaneDispatch(
        "command-1",
        object(),
        ProcessLaneBinding("campaign-a", 1, "owner-1"),
        parent_context=object(),
    )

    assert runtime.prepare_dispatch is not None
    assert runtime.prepare_dispatch(dispatch) is dispatch
    assert calls == ["dispatch", "receipt"]


def test_parent_issues_exact_replay_authority_and_fresh_validates_receipt_metrics() -> None:
    authority, dispatch, _operation, _expiry = _authority_case()
    issued = authority.issue_receipt_probe(dispatch, "replay-load", None)
    assert issued is not None
    attempt, requested_operation = issued
    receipt = _receipt_for(attempt, requested_operation)
    probes: list[tuple[object, object]] = []
    authority._state = SimpleNamespace(  # type: ignore[assignment]
        replay_if_committed=lambda candidate, operation: (
            probes.append((candidate, operation)) or MssqlTransactionAdmission(replay_receipt=receipt)
        )
    )
    result = {
        "status": "success",
        "errors": [],
        "load_id": "replay-load",
        "extracted_rows": 3,
        "loaded_rows": 2,
        "inserted_rows": 2,
        "updated_rows": 1,
        "total_rows": 10,
        "final_rows": 10,
        "staging_rows": 3,
        "soft_deleted_rows": 0,
        "reactivated_rows": 0,
        "unchanged_rows": 7,
        "hard_deleted_rows": 0,
        "active_rows": None,
        "commit_receipt_id": receipt.receipt_id,
        "commit_outcome": "replay_suppressed",
        "replaced_rows": 0,
        "deleted_lookback_rows": 0,
        "reconciliation_metrics": {"mssql_transaction_replay_suppressed": True},
    }

    assert authority.validate_replay_result(dispatch, attempt, requested_operation, result) is True
    assert probes == [(attempt, requested_operation)]
    assert (
        authority.validate_replay_result(
            dispatch,
            attempt,
            requested_operation,
            {**result, "commit_receipt_id": "forged"},
        )
        is False
    )


def _registration(
    dispatch: ProcessLaneDispatch,
    operation: MssqlTransactionOperation,
    *,
    request_id: str = "register-1",
) -> ProcessLaneOperationLeaseRequest:
    return ProcessLaneOperationLeaseRequest(
        request_id=request_id,
        action="register",
        worker_id=0,
        command_id=dispatch.command_id,
        binding=dispatch.binding,
        operation_key=operation.operation_key,
        owner_digest=operation.owner_digest,
        epoch=operation.epoch,
        operation=operation,
    )


def test_parent_registration_proves_current_sql_state_before_active_ack() -> None:
    authority, dispatch, operation, expiry = _authority_case()
    calls: list[tuple[MssqlTransactionOperation, datetime]] = []
    coordinator = ParentOperationLeaseCoordinator(
        renew=lambda current, deadline: calls.append((current, deadline)) is None or True,
        validate_binding=authority.validate_operation_binding,
    )
    parent, child = get_context("spawn").Pipe(duplex=True)

    assert coordinator.handle(
        parent, _registration(dispatch, operation), worker_id=0, dispatch=dispatch, control_error=None
    )
    reply = child.recv()

    assert calls == [(operation, expiry)]
    assert isinstance(reply, ProcessLaneOperationLeaseReply)
    assert reply.active is True
    assert reply.lease_expires_at_utc == expiry
    parent.close()
    child.close()


def test_parent_control_failure_rejects_before_validation_or_sql_renew() -> None:
    _authority, dispatch, operation, _expiry = _authority_case()
    renewals: list[object] = []
    validations: list[object] = []
    coordinator = ParentOperationLeaseCoordinator(
        renew=lambda *_args: renewals.append(object()) is None,
        validate_binding=lambda *_args: validations.append(object()) is None,
    )
    parent, child = get_context("spawn").Pipe(duplex=True)

    active = coordinator.handle(
        parent,
        _registration(dispatch, operation),
        worker_id=0,
        dispatch=dispatch,
        control_error="DPONE_BACKFILL_CHUNK_LEASE_HEARTBEAT_LOST",
    )
    reply = child.recv()

    assert active is False
    assert reply.active is False
    assert renewals == validations == []
    parent.close()
    child.close()


def test_cached_active_registration_cannot_replay_after_unregister() -> None:
    _authority, dispatch, operation, _expiry = _authority_case()
    coordinator = ParentOperationLeaseCoordinator(
        renew=lambda *_args: True,
        validate_binding=lambda *_args: True,
    )
    parent, child = get_context("spawn").Pipe(duplex=True)
    registration = _registration(dispatch, operation)
    assert coordinator.handle(parent, registration, worker_id=0, dispatch=dispatch, control_error=None)
    assert child.recv().active is True
    unregister = replace(registration, request_id="unregister-1", action="unregister", operation=None)
    assert coordinator.handle(parent, unregister, worker_id=0, dispatch=dispatch, control_error=None)
    assert child.recv().active is True

    assert not coordinator.handle(parent, registration, worker_id=0, dispatch=dispatch, control_error=None)
    assert child.recv().active is False
    parent.close()
    child.close()


@pytest.mark.parametrize("tamper", ["expiry", "generation", "epoch"])
def test_parent_registration_rejects_stale_or_extended_child_authority(tamper: str) -> None:
    _authority, dispatch, operation, _expiry = _authority_case()
    expected_generation = operation.attempt.generation
    expected_epoch = operation.epoch
    renewals: list[MssqlTransactionOperation] = []

    if tamper == "expiry":
        operation = replace(operation, lease_expires_at_utc=operation.lease_expires_at_utc + timedelta(hours=1))
    elif tamper == "generation":
        operation = replace(operation, attempt=replace(operation.attempt, generation=expected_generation + 1))
    else:
        operation = replace(operation, epoch=expected_epoch + 1)

    def state_current(candidate: MssqlTransactionOperation, _deadline: datetime) -> bool:
        renewals.append(candidate)
        return candidate.attempt.generation == expected_generation and candidate.epoch == expected_epoch

    coordinator = ParentOperationLeaseCoordinator(renew=state_current, validate_binding=lambda *_args: True)
    parent, child = get_context("spawn").Pipe(duplex=True)

    active = coordinator.handle(
        parent,
        _registration(dispatch, operation),
        worker_id=0,
        dispatch=dispatch,
        control_error=None,
    )
    reply = child.recv()

    assert active is False
    assert isinstance(reply, ProcessLaneOperationLeaseReply)
    assert reply.active is False
    if tamper == "expiry":
        assert renewals == []
    else:
        assert renewals == [operation]
    parent.close()
    child.close()
