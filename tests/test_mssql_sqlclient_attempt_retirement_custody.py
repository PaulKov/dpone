"""Live attempt authority reaches same-fence P10g preparation exactly once."""

from contextlib import contextmanager
from pathlib import Path
from runpy import run_path
from time import monotonic
from uuid import UUID

import pytest

import dpone.contracts.mssql_tds_suspension as suspension_contract
from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.app.mssql_tds_attempt_composition import create_tds_attempt, resume_tds_attempt
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_sqlclient_attempt import (
    SqlClientAttemptEvidence,
    SqlClientCredentialIntent,
    SqlClientGrantIntent,
    SqlClientWriterObserved,
)
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.contracts.mssql_tds_worker import (
    Exited,
    LaunchIntent,
    Prepared,
    ProcessRegistered,
    TdsAttemptPhase,
    TdsProcessIdentity,
    Verified,
)
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody

_FIXTURES = run_path(str(Path(__file__).with_name("test_mssql_native_chunk_retirement.py")))
_request = _FIXTURES["_request"]


def _verified_attempt(tmp_path, request):
    store = SQLiteWindowStore(tmp_path / "custody.sqlite", clock=lambda: 1.0)
    for index in range(2):
        prior = store.acquire(request.projection.attempt.target_key, f"prior-{index}", 30)
        store.release(prior)
    lease = store.acquire(request.projection.attempt.target_key, "owner", 30)
    pool = TdsActorPool(capacity=2)

    @contextmanager
    def factory():
        yield store

    deadline = monotonic() + 5
    attempt = create_tds_attempt(
        pool,
        factory,
        request.projection.attempt,
        TdsDirectoryLimits(8, 3, 50_000, 10_000),
        lease,
        supervisor_token=str(UUID(int=10)),
        deadline=deadline,
        backend="mssql_sqlclient",
    )
    lifecycle = attempt._lifecycle
    for event in (
        Prepared(request.projection.object_identity, "1" * 64),
        LaunchIntent("2" * 64),
        ProcessRegistered(TdsProcessIdentity("3" * 64, str(UUID(int=11)), 42, 1)),
        SqlClientCredentialIntent(SqlClientAttemptEvidence("4" * 64, "5" * 64, False)),
        SqlClientWriterObserved("6" * 64),
        SqlClientGrantIntent("7" * 64),
        Exited(0, "4" * 64),
        Verified(request.projection.lifecycle_verification_sha256),
    ):
        lifecycle.advance(event, expected_phase=lifecycle.snapshot.state.phase, deadline=deadline)
    return attempt, pool, factory, lease, store, deadline


def test_prepares_exact_verified_attempt_under_same_fence(tmp_path):
    request = _request()
    attempt, pool, factory, lease, store, deadline = _verified_attempt(tmp_path, request)
    assert attempt.lifecycle.revision == request.projection.lifecycle_revision
    custody = SqlClientAttemptRetirementCustody(
        lambda suspension, end: resume_tds_attempt(pool, factory, suspension, lease, deadline=end)
    )
    custody.retain(attempt, deadline=deadline)
    assert pool.live_count == 0
    custody.prepare_verified(request, deadline=deadline)
    lifecycle = TdsAttemptJournal(store, backend="mssql_sqlclient").read(request.projection.attempt)
    directory = TdsCoordinatorDirectoryJournal(
        store, parent_observer=TdsAttemptJournal(store, backend="mssql_sqlclient")
    ).read(request.projection.attempt, TdsDirectoryLimits(8, 3, 50_000, 10_000))
    assert lifecycle is not None and directory is not None
    assert lifecycle.state.phase is TdsAttemptPhase.RETIREMENT_REQUIRED
    assert lifecycle.state.error is None
    assert directory.state.retirement_authority == lifecycle.state
    with pytest.raises(LookupError, match="live_attempt_unavailable"):
        custody.prepare_verified(request, deadline=deadline)
    pool.close(deadline=deadline)


def test_suspension_is_one_shot_and_cannot_be_consumed_after_process_loss(tmp_path, monkeypatch):
    request = _request()
    attempt, pool, factory, lease, _store, deadline = _verified_attempt(tmp_path, request)
    suspension = attempt.suspend(deadline=deadline)
    with pytest.raises(WindowContractError, match="suspension_unavailable"):
        attempt.suspend(deadline=deadline)
    resumed = resume_tds_attempt(pool, factory, suspension, lease, deadline=deadline)
    resumed.close(deadline=deadline)
    with pytest.raises(ValueError, match="suspension_invalid"):
        resume_tds_attempt(pool, factory, suspension, lease, deadline=deadline)

    lost_path = tmp_path / "lost"
    lost_path.mkdir()
    attempt, lost_pool, _factory, _lease, _store, _deadline = _verified_attempt(lost_path, request)
    lost = attempt.suspend(deadline=_deadline)
    original_pid = suspension_contract.os.getpid()
    with monkeypatch.context() as patch:
        patch.setattr(suspension_contract.os, "getpid", lambda: original_pid + 1)
        with pytest.raises(ValueError, match="suspension_invalid"):
            lost.consume()
    lost_pool.close(deadline=_deadline)
    pool.close(deadline=deadline)
