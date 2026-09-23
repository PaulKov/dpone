"""Exact failed-attempt authority cannot be forged or crossed between attempts."""

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from time import monotonic
from uuid import UUID

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_sqlclient_failed_attempt_settlement import (
    WindowStoreSqlClientFailedSettlementJournal,
)
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.app.mssql_sqlclient_failed_attempt_composition import (
    SqlClientFailedAttemptDeployment,
    compose_sqlclient_failed_attempt_settlement,
)
from dpone.app.mssql_tds_attempt_composition import create_tds_attempt
from dpone.contracts.mssql_sqlclient_attempt import (
    SqlClientAttemptEvidence,
    SqlClientCredentialIntent,
    SqlClientGrantIntent,
    SqlClientWriterObserved,
)
from dpone.contracts.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedAttemptDisposition,
    SqlClientFailedAttemptSettlementReceipt,
    SqlClientFailedRetirementAuthorization,
    SqlClientFailedRetirementReceipt,
    SqlClientFailedRetirementRequest,
    SqlClientFailedRetirementSubject,
    issue_failed_retirement_authorization,
)
from dpone.contracts.mssql_sqlclient_stage_identity import SqlClientStageIdentity
from dpone.contracts.mssql_tds_create import TdsCreateObservedColumn, TdsCreateType
from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorDirectory,
    TdsDirectoryLimits,
    TdsDirectorySnapshot,
)
from dpone.contracts.mssql_tds_worker import (
    Contained,
    ContainmentRequired,
    Exited,
    LaunchIntent,
    Prepared,
    ProcessRegistered,
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsAttemptState,
    TdsObjectIdentity,
    TdsProcessIdentity,
)
from dpone.services.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedAttemptSettlementService,
    failed_settlement_operation_key,
)


def _subject() -> SqlClientFailedRetirementSubject:
    attempt = TdsAttemptIdentity(
        target_key="target",
        run_id="run",
        ordinal=0,
        attempt=0,
        plan_sha256="1" * 64,
        policy_sha256="2" * 64,
        implementation_sha256="3" * 64,
        file_sha256="4" * 64,
        database="db",
        schema="stage",
        table="chunk",
        owner_binding="5" * 64,
    )
    stage = SqlClientStageIdentity(
        database_guid=UUID(int=1),
        database_id=5,
        database_name="db",
        schema_id=6,
        schema_name="stage",
        table_name="chunk",
        object_id=7,
        create_date=datetime(2026, 1, 1),
        owner_binding="5" * 64,
        object_nonce=UUID(int=2),
        columns=(TdsCreateObservedColumn(1, "id", TdsCreateType.BIGINT, False, 8, 19, 0, None),),
    )
    return SqlClientFailedRetirementSubject.bind(
        attempt=attempt,
        stage=stage,
        object_identity=TdsObjectIdentity(7, "6" * 64),
        expected_object_nonce=UUID(int=2),
        input_custody_sha256="7" * 64,
        error=TdsAttemptError.CONNECTION,
        observation_sha256="8" * 64,
        lifecycle_revision=9,
        lifecycle_state_sha256="9" * 64,
        directory_key="directory",
        directory_revision=10,
        directory_state_sha256="a" * 64,
        lease_owner="owner",
        lease_fence=11,
    )


def test_exact_subject_authority_and_terminal_receipt_are_self_binding() -> None:
    subject = _subject()
    authorization = issue_failed_retirement_authorization(subject)
    request = SqlClientFailedRetirementRequest(subject, authorization)
    receipt = SqlClientFailedAttemptSettlementReceipt.bind(
        request_sha256=request.request_sha256,
        retirement_receipt_sha256="b" * 64,
        operation_key="c" * 64,
        disposition=SqlClientFailedAttemptDisposition.RETRY_READY,
    )

    subject.__post_init__()
    receipt.__post_init__()
    assert receipt.request_sha256 == request.request_sha256


def test_authorization_cannot_be_forged_or_crossed() -> None:
    subject = _subject()
    with pytest.raises(ValueError, match="failed_settlement_invalid"):
        SqlClientFailedRetirementAuthorization(subject.subject_sha256, "0" * 64)

    with pytest.raises(ValueError, match="failed_settlement_invalid"):
        replace(subject, lease_fence=12)

    authorization = issue_failed_retirement_authorization(subject)
    with pytest.raises(ValueError, match="failed_settlement_invalid"):
        crossed = object.__new__(SqlClientFailedRetirementSubject)
        for name in subject.__dataclass_fields__:
            object.__setattr__(crossed, name, "f" * 64 if name == "subject_sha256" else getattr(subject, name))
        SqlClientFailedRetirementRequest(crossed, authorization)


def test_subject_rejects_stage_and_lifecycle_tampering() -> None:
    subject = _subject()
    for changes in (
        {"lifecycle_revision": subject.lifecycle_revision + 1},
        {"directory_state_sha256": "f" * 64},
        {"object_identity": TdsObjectIdentity(8, "6" * 64)},
    ):
        with pytest.raises(ValueError, match="failed_settlement_invalid"):
            replace(subject, **changes)


def test_subject_represents_authoritative_never_created_attempt_without_fake_stage() -> None:
    subject = _subject()
    rebound = SqlClientFailedRetirementSubject.bind(
        **{
            name: None if name in {"stage", "object_identity"} else getattr(subject, name)
            for name in subject.__dataclass_fields__
            if name not in {"schema", "subject_sha256"}
        }
    )

    assert rebound.stage is None
    assert rebound.object_identity is None


def _retirement(request: SqlClientFailedRetirementRequest) -> SqlClientFailedRetirementReceipt:
    return SqlClientFailedRetirementReceipt.bind(
        request_sha256=request.request_sha256,
        object_incarnation_sha256="1" * 64,
        drop_settlement_sha256="2" * 64,
        absence_sha256="3" * 64,
        terminal_sha256="4" * 64,
        directory_sha256="5" * 64,
        capacity_sha256="6" * 64,
    )


def test_nominal_service_persists_intent_before_retirement_and_replays_receipt() -> None:
    request = SqlClientFailedRetirementRequest(_subject(), issue_failed_retirement_authorization(_subject()))
    events = []
    durable = {}

    class Observer:
        def operation_key(self, plan, attempt_id, lease):
            return "c" * 64

        def observe(self, plan, attempt_id, lease):
            events.append("observe")
            return request

    class Retirement:
        def observe_or_retire(self, candidate):
            events.append("retire")
            assert candidate == request
            return _retirement(candidate)

    class Reservation:
        def reserve(self, candidate):
            events.append("reserve")
            assert candidate == request

    class Journal:
        def observe_or_resume(self, operation_key, observe, resume):
            assert operation_key == "c" * 64
            if operation_key not in durable:
                candidate = observe()
                events.append("intent")
                durable[operation_key] = resume(candidate)
                events.append("receipt")
            return durable[operation_key]

    service = SqlClientFailedAttemptSettlementService(Observer(), Reservation(), Retirement(), Journal())
    first = service.settle(object(), "attempt", object())
    second = service.settle(object(), "attempt", object())

    assert first is second
    assert events == ["observe", "intent", "reserve", "retire", "receipt"]


def test_window_store_journal_replays_receipt_without_resuming_retirement(tmp_path: Path) -> None:
    store = SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60.0)
    journal = WindowStoreSqlClientFailedSettlementJournal(store, lease)
    request = SqlClientFailedRetirementRequest(_subject(), issue_failed_retirement_authorization(_subject()))
    operation_key = failed_settlement_operation_key(request)
    calls = []

    def resume(candidate):
        calls.append("resume")
        assert candidate == request
        retirement = _retirement(request)
        return SqlClientFailedAttemptSettlementReceipt.bind(
            request_sha256=request.request_sha256,
            retirement_receipt_sha256=retirement.receipt_sha256,
            operation_key=operation_key,
            disposition=SqlClientFailedAttemptDisposition.RETRY_READY,
        )

    first = journal.observe_or_resume(operation_key, lambda: request, resume)
    second = journal.observe_or_resume(operation_key, lambda: pytest.fail("observer replayed"), resume)

    assert first == second
    assert calls == ["resume"]


def test_production_observer_rejects_non_retryable_failure_before_retirement(tmp_path: Path) -> None:
    subject = _subject()
    subject = SqlClientFailedRetirementSubject.bind(
        **{
            name: (
                TdsAttemptError.CONSTRAINT
                if name == "error"
                else 1
                if name == "lease_fence"
                else getattr(subject, name)
            )
            for name in subject.__dataclass_fields__
            if name not in {"schema", "subject_sha256"}
        }
    )
    ownership = TdsAttemptOwnership(subject.lease_owner, subject.lease_fence, str(UUID(int=3)))
    state = TdsAttemptState(
        identity=subject.attempt,
        ownership=ownership,
        phase=TdsAttemptPhase.CONTAINED,
        sequence=9,
        object_identity=subject.object_identity,
        process=TdsProcessIdentity("b" * 64, str(UUID(int=4)), 123, 456),
        exit_code=1,
        result_sha256="c" * 64,
        error=subject.error,
        observation_sha256=subject.observation_sha256,
        schema_version=2,
        backend="mssql_sqlclient",
        sqlclient=SqlClientAttemptEvidence("d" * 64, "e" * 64, False, "f" * 64, "0" * 64),
    )
    snapshot = type("Snapshot", (), {"state": state, "revision": subject.lifecycle_revision})()
    directory_state = TdsCoordinatorDirectory(
        parent=subject.attempt,
        limits=TdsDirectoryLimits(8, 3, 50_000, 10_000),
        work_sealed=False,
        retirement_authority=None,
        sequence=0,
        schema_version=2,
    )
    directory_snapshot = TdsDirectorySnapshot(directory_state, ownership, subject.directory_revision)
    store = SQLiteWindowStore(tmp_path / "terminal.sqlite", clock=lambda: 1.0)
    lease = store.acquire(subject.attempt.target_key, subject.lease_owner, 60.0)
    service = compose_sqlclient_failed_attempt_settlement(
        SqlClientFailedAttemptDeployment(
            store=store,
            lease=lease,
            lifecycle_observer=type("Lifecycle", (), {"read": lambda *_: snapshot})(),
            directory_observer=type("Directory", (), {"read": lambda *_: directory_snapshot})(),
            directory_limits=directory_state.limits,
            resolve_attempt=lambda *_: subject.attempt,
            observe_stage=lambda *_: subject.stage,
            observe_input_custody=lambda *_: subject.input_custody_sha256,
            reservation=type("Reservation", (), {"reserve": lambda *_: pytest.fail("terminal reserved")})(),
            retirement=type("Retirement", (), {"observe_or_retire": lambda *_: pytest.fail("terminal retired")})(),
        )
    )

    with pytest.raises(RuntimeError, match="terminal_input_or_policy"):
        service.settle(object(), "run-0-0", lease)


def test_real_attempt_reservation_precedes_terminal_and_replay_skips_observer(tmp_path: Path) -> None:
    subject = _subject()
    store = SQLiteWindowStore(tmp_path / "real.sqlite", clock=lambda: 1.0)
    lease = store.acquire(subject.attempt.target_key, subject.lease_owner, 60.0)
    limits = TdsDirectoryLimits(8, 3, 50_000, 10_000)
    pool = TdsActorPool(capacity=2)

    @contextmanager
    def factory():
        yield store

    deadline = monotonic() + 5
    attempt = create_tds_attempt(
        pool,
        factory,
        subject.attempt,
        limits,
        lease,
        supervisor_token=str(UUID(int=30)),
        deadline=deadline,
        backend="mssql_sqlclient",
    )
    lifecycle = attempt._lifecycle
    for event in (
        Prepared(subject.object_identity, "1" * 64),
        LaunchIntent("2" * 64),
        ProcessRegistered(TdsProcessIdentity("3" * 64, str(UUID(int=31)), 42, 1)),
        SqlClientCredentialIntent(SqlClientAttemptEvidence("4" * 64, "5" * 64, False)),
        SqlClientWriterObserved("6" * 64),
        SqlClientGrantIntent("7" * 64),
        Exited(1, "8" * 64),
        ContainmentRequired(TdsAttemptError.CONNECTION),
        Contained("9" * 64),
    ):
        lifecycle.advance(event, expected_phase=lifecycle.snapshot.state.phase, deadline=deadline)

    attempts = TdsAttemptJournal(store, backend="mssql_sqlclient")
    directories = TdsCoordinatorDirectoryJournal(store, parent_observer=attempts)
    observations = []

    class ObservedAttempts:
        def read(self, identity):
            observations.append(identity)
            return attempts.read(identity)

    class Reservation:
        def reserve(self, request):
            current = attempts.read(subject.attempt)
            assert current is not None
            if current.state.phase is TdsAttemptPhase.CONTAINED:
                attempt.reserve_retirement(UUID(int=32), request.request_sha256, deadline=deadline)

    class Retirement:
        calls = 0

        def observe_or_retire(self, request):
            self.calls += 1
            current = attempts.read(subject.attempt)
            directory = directories.read(subject.attempt, limits)
            assert current is not None and current.state.phase is TdsAttemptPhase.RETIREMENT_REQUIRED
            assert directory is not None and directory.state.retirement_authority == current.state
            if self.calls == 1:
                raise RuntimeError("injected_after_reservation")
            return _retirement(request)

    retirement = Retirement()
    service = compose_sqlclient_failed_attempt_settlement(
        SqlClientFailedAttemptDeployment(
            store=store,
            lease=lease,
            lifecycle_observer=ObservedAttempts(),
            directory_observer=directories,
            directory_limits=limits,
            resolve_attempt=lambda *_: subject.attempt,
            observe_stage=lambda *_: subject.stage,
            observe_input_custody=lambda *_: subject.input_custody_sha256,
            reservation=Reservation(),
            retirement=retirement,
        )
    )
    with pytest.raises(RuntimeError, match="injected_after_reservation"):
        service.settle(object(), "run-0-0", lease)
    first = service.settle(object(), "run-0-0", lease)
    second = service.settle(object(), "run-0-0", lease)
    assert first == second
    assert len(observations) == 1
    assert retirement.calls == 2
    attempt.close(deadline=deadline)
    pool.close(deadline=deadline)
