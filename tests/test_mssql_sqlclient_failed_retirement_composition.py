"""Production failed P10g uses real attempt state and restart-safe SQL ordering."""

from __future__ import annotations

from contextlib import contextmanager
from time import monotonic
from uuid import UUID

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_sqlclient_failed_attempt_settlement import (
    WindowStoreSqlClientFailedRetirementProgress,
)
from dpone.adapters.mssql_sqlclient_failed_retirement_effects import ProductionSqlClientFailedRetirementEffects
from dpone.adapters.mssql_sqlclient_failed_retirement_operator import (
    SqlClientFailedRetirementProgress,
    SqlClientFailedRetirementTerminal,
)
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.app.mssql_sqlclient_failed_retirement_composition import SqlClientFailedRetirementReservation
from dpone.app.mssql_tds_attempt_composition import create_tds_attempt, resume_tds_attempt
from dpone.contracts.mssql_sqlclient_attempt import (
    SqlClientAttemptEvidence,
    SqlClientCredentialIntent,
    SqlClientGrantIntent,
    SqlClientWriterObserved,
)
from dpone.contracts.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedRetirementRequest,
    SqlClientFailedRetirementSubject,
    issue_failed_retirement_authorization,
)
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.contracts.mssql_tds_worker import (
    Contained,
    ContainmentRequired,
    Exited,
    LaunchIntent,
    Prepared,
    ProcessRegistered,
    TdsAttemptError,
    TdsAttemptPhase,
    TdsProcessIdentity,
)
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody
from tests.test_mssql_sqlclient_failed_attempt_settlement import _subject
from tests.test_mssql_sqlclient_native_retirement_operator import _admission, _row, _Session


class Store:
    def __init__(self, request: SqlClientFailedRetirementRequest, *, fail_after: int | None = None) -> None:
        self.value = SqlClientFailedRetirementProgress(request.request_sha256)
        self.events: list[str] = []
        self.fail_after = fail_after

    def observe(self, request_sha256: str) -> SqlClientFailedRetirementProgress:
        assert request_sha256 == self.value.request_sha256
        return self.value

    def record(self, before, after):
        assert before == self.value
        self.value = after
        self.events.append(
            next(name for name in after.__dataclass_fields__ if getattr(before, name) != getattr(after, name))
        )
        if len(self.events) == self.fail_after:
            self.fail_after = None
            raise RuntimeError("injected_after_durable_record")
        return after


class Effects:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.terminal = None

    def observe_exact_incarnation(self, request):
        self.calls.append("incarnation")
        return self.terminal._incarnation(request)

    def drop_exact(self, request, intent_sha256):
        self.calls.append("drop")
        return "succeeded"

    def reconcile_drop(self, request, intent_sha256):
        self.calls.append("reconcile")
        return "no_effect"

    def observe_absence(self, request, drop_sha256):
        self.calls.append("absence")
        return "a" * 64

    def observe_capacity_release(self, request, directory_sha256):
        self.calls.append("capacity")
        return "b" * 64


def _real_attempt(tmp_path):
    subject = _subject()
    subject = SqlClientFailedRetirementSubject.bind(
        **{
            name: 1 if name == "lease_fence" else getattr(subject, name)
            for name in subject.__dataclass_fields__
            if name not in {"schema", "subject_sha256"}
        }
    )
    request = SqlClientFailedRetirementRequest(subject, issue_failed_retirement_authorization(subject))
    store = SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1.0)
    lease = store.acquire(subject.attempt.target_key, subject.lease_owner, 60.0)
    pool = TdsActorPool(capacity=2)

    @contextmanager
    def factory():
        yield store

    deadline = monotonic() + 10
    attempt = create_tds_attempt(
        pool,
        factory,
        subject.attempt,
        TdsDirectoryLimits(8, 3, 50_000, 10_000),
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
    return request, attempt, pool, factory, lease, deadline


def test_real_attempt_reaches_retired_closed_and_replay_has_no_second_drop(tmp_path) -> None:
    request, attempt, pool, _factory, _lease, deadline = _real_attempt(tmp_path)
    reservation = SqlClientFailedRetirementReservation(lambda _: attempt, monotonic=lambda: deadline - 1)
    reservation.reserve(request)
    progress = Store(request)
    effects = Effects()
    terminal = SqlClientFailedRetirementTerminal(reservation, progress, effects, deadline=lambda: deadline)
    effects.terminal = terminal

    first = terminal.observe_or_retire(request)
    second = terminal.observe_or_retire(request)

    assert first == second
    assert attempt.lifecycle.state.phase is TdsAttemptPhase.RETIRED
    assert attempt.directory.state.admission_closed
    assert effects.calls == ["incarnation", "drop", "absence", "capacity"]
    assert progress.events == [
        "intent_sha256",
        "drop_sha256",
        "absence_sha256",
        "terminal_sha256",
        "directory_sha256",
        "capacity_sha256",
    ]
    attempt.close(deadline=deadline)
    pool.close(deadline=deadline)


def test_replay_of_durable_intent_reconciles_before_one_safe_drop(tmp_path) -> None:
    request, attempt, pool, _factory, _lease, deadline = _real_attempt(tmp_path)
    reservation = SqlClientFailedRetirementReservation(lambda _: attempt, monotonic=lambda: deadline - 1)
    reservation.reserve(request)
    progress = Store(request)
    progress.value = SqlClientFailedRetirementProgress(request.request_sha256, intent_sha256="c" * 64)
    effects = Effects()
    terminal = SqlClientFailedRetirementTerminal(reservation, progress, effects, deadline=lambda: deadline)
    effects.terminal = terminal

    terminal.observe_or_retire(request)

    assert effects.calls[:3] == ["reconcile", "incarnation", "drop"]
    assert effects.calls.count("drop") == 1
    assert progress.value.effect_attempt == 1
    attempt.close(deadline=deadline)
    pool.close(deadline=deadline)


@pytest.mark.parametrize("fail_after", range(1, 7))
def test_every_durable_crash_prefix_resumes_without_repeating_drop(tmp_path, fail_after) -> None:
    request, attempt, pool, _factory, _lease, deadline = _real_attempt(tmp_path)
    reservation = SqlClientFailedRetirementReservation(lambda _: attempt, monotonic=lambda: deadline - 1)
    reservation.reserve(request)
    progress = Store(request, fail_after=fail_after)
    effects = Effects()
    terminal = SqlClientFailedRetirementTerminal(reservation, progress, effects, deadline=lambda: deadline)
    effects.terminal = terminal

    with pytest.raises(RuntimeError, match="injected_after_durable_record"):
        terminal.observe_or_retire(request)
    receipt = terminal.observe_or_retire(request)

    assert receipt.request_sha256 == request.request_sha256
    assert effects.calls.count("drop") == 1
    assert attempt.lifecycle.state.phase is TdsAttemptPhase.RETIRED
    assert attempt.directory.state.admission_closed
    attempt.close(deadline=deadline)
    pool.close(deadline=deadline)


def test_reservation_rejects_crossed_retained_authority(tmp_path) -> None:
    request, attempt, pool, _factory, _lease, deadline = _real_attempt(tmp_path)
    crossed = SqlClientFailedRetirementRequest(
        _subject(),
        issue_failed_retirement_authorization(_subject()),
    )
    crossed = SqlClientFailedRetirementRequest(
        crossed.subject,
        crossed.authorization,
    )
    reservation = SqlClientFailedRetirementReservation(lambda _: attempt, monotonic=lambda: deadline - 1)
    object.__setattr__(crossed.subject, "lease_fence", crossed.subject.lease_fence + 1)

    with pytest.raises((ValueError, RuntimeError), match="failed_retirement"):
        reservation.reserve(crossed)

    attempt.close(deadline=deadline)
    pool.close(deadline=deadline)


def test_capacity_and_containment_proofs_follow_actual_actor_release(tmp_path) -> None:
    request, attempt, pool, factory, lease, deadline = _real_attempt(tmp_path)
    custody = SqlClientAttemptRetirementCustody(
        lambda suspension, end: resume_tds_attempt(pool, factory, suspension, lease, deadline=end)
    )
    custody.retain(attempt, deadline=deadline)
    reservation = SqlClientFailedRetirementReservation(
        lambda value: custody.acquire_failed(value, deadline=deadline),
        lambda value: custody.release_failed(value, deadline=deadline),
        monotonic=lambda: deadline - 1,
    )
    reservation.reserve(request)
    assert pool.live_count == 0
    progress = Store(request)
    effects = Effects()

    def capacity(value, directory_sha256):
        assert custody.failed_released(value)
        assert pool.live_count == 0
        effects.calls.append("capacity")
        return "b" * 64

    effects.observe_capacity_release = capacity
    terminal = SqlClientFailedRetirementTerminal(reservation, progress, effects, deadline=lambda: deadline)
    effects.terminal = terminal
    terminal.observe_or_retire(request)

    resumed = custody.acquire_failed(request, deadline=deadline)
    slot = next(slot for slot in resumed.directory.state.slots if slot.command_sha256 == request.request_sha256)
    assert slot.local_containment is not None
    assert slot.local_containment.process_sha256 == request.subject.observation_sha256
    custody.release_failed(request, deadline=deadline)
    assert custody.failed_released(request)
    assert pool.live_count == 0
    custody.close_all()
    pool.close(deadline=deadline)


def test_process_restart_finishes_capacity_without_reacquiring_lost_custody() -> None:
    request = SqlClientFailedRetirementRequest(_subject(), issue_failed_retirement_authorization(_subject()))
    progress = Store(request)
    progress.value = SqlClientFailedRetirementProgress(
        request.request_sha256,
        intent_sha256="1" * 64,
        drop_sha256="2" * 64,
        drop_outcome="succeeded",
        absence_sha256="3" * 64,
        terminal_sha256="4" * 64,
        directory_sha256="5" * 64,
    )

    class RestartedReservation:
        def acquire_terminal(self, _request):
            raise AssertionError("durably retired attempt reacquired after process restart")

        def release_terminal(self, _request):
            raise AssertionError("empty restarted custody was mutated")

    effects = Effects()
    terminal = SqlClientFailedRetirementTerminal(
        RestartedReservation(), progress, effects, deadline=lambda: monotonic() + 1
    )
    effects.terminal = terminal

    receipt = terminal.observe_or_retire(request)

    assert receipt.capacity_sha256 == "b" * 64
    assert effects.calls == ["capacity"]
    assert progress.events == ["capacity_sha256"]


def test_production_effects_use_lock_exact_sql_commit_absence_and_capacity() -> None:
    subject = _subject()
    request = SqlClientFailedRetirementRequest(subject, issue_failed_retirement_authorization(subject))
    shaped = type("Request", (), {"projection": type("Projection", (), {"stage": subject.stage})()})()
    admission = _admission(shaped)
    sessions = [
        _Session(admission, (_row(subject.stage),)),
        _Session(admission, (_row(subject.stage),)),
        _Session(admission, ()),
    ]

    @contextmanager
    def open_session():
        yield sessions.pop(0)

    effects = ProductionSqlClientFailedRetirementEffects(
        open_management_session=open_session,
        admission=admission,
        expected_server=admission.server,
        expected_database=admission.database,
        assert_current=lambda _: None,
        observe_capacity_release=lambda _: True,
        monotonic=lambda: 1.0,
    )

    incarnation = effects.observe_exact_incarnation(request)
    drop_session = sessions[0]
    assert effects.drop_exact(request, "c" * 64) == "succeeded"
    assert effects.observe_absence(request, "d" * 64)
    assert effects.observe_capacity_release(request, "e" * 64)
    assert incarnation == SqlClientFailedRetirementTerminal._incarnation(request)
    assert [call[0] for call in drop_session.calls] == ["lock", "query", "execute", "commit", "contain"]


def test_window_store_progress_persists_exact_cas_prefix(tmp_path) -> None:
    request = SqlClientFailedRetirementRequest(_subject(), issue_failed_retirement_authorization(_subject()))
    store = SQLiteWindowStore(tmp_path / "progress.sqlite", clock=lambda: 1.0)
    lease = store.acquire(request.subject.attempt.target_key, request.subject.lease_owner, 30.0)
    progress = WindowStoreSqlClientFailedRetirementProgress(store, lease)
    initial = progress.observe(request.request_sha256)
    changed = SqlClientFailedRetirementProgress(request.request_sha256, intent_sha256="a" * 64)

    assert progress.record(initial, changed) == changed
    assert WindowStoreSqlClientFailedRetirementProgress(store, lease).observe(request.request_sha256) == changed
    with pytest.raises(RuntimeError, match="progress_unknown"):
        progress.record(initial, changed)
