from pathlib import Path
from runpy import run_path
from uuid import UUID

import pytest

from dpone.adapters import mssql_sqlclient_native_retirement_window_store as durable_state
from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_sqlclient_native_retirement_window_store import (
    WindowStoreSqlClientNativeRetirementState,
)
from dpone.contracts.bounded_window import WindowContractError, WindowLeaseLost
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits, TdsDirectorySnapshot, initial_directory
from dpone.contracts.mssql_tds_worker import (
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsAttemptState,
    TdsProcessIdentity,
)
from dpone.ports.mssql_native_chunk_retirement import NativeChunkLifecyclePhase

_FIXTURES = run_path(str(Path(__file__).with_name("test_mssql_native_chunk_retirement.py")))
_Effects = _FIXTURES["_Effects"]
_progress = _FIXTURES["_progress"]
_request = _FIXTURES["_request"]
_service = _FIXTURES["_service"]
_State = _FIXTURES["_State"]


_LIMITS = TdsDirectoryLimits(8, 3, 50_000, 10_000)


class _Attempts:
    def __init__(self, request):
        projection = request.projection
        state = TdsAttemptState(
            identity=projection.attempt,
            ownership=TdsAttemptOwnership("owner", 1, str(UUID(int=10))),
            phase=TdsAttemptPhase.VERIFIED,
            sequence=7,
            object_identity=projection.object_identity,
            process=TdsProcessIdentity("1" * 64, str(UUID(int=11)), 42, 1),
            exit_code=0,
            result_sha256="2" * 64,
            verification_sha256=projection.lifecycle_verification_sha256,
            observation_sha256=projection.lifecycle_verification_sha256,
        )
        self.snapshot = TdsAttemptSnapshot(state, projection.lifecycle_revision)

    def read(self, identity):
        return self.snapshot if identity == self.snapshot.state.identity else None


class _Directories:
    def __init__(self, request):
        state = initial_directory(request.projection.attempt, _LIMITS)
        owner = TdsAttemptOwnership("owner", 1, str(UUID(int=10)))
        self.snapshot = TdsDirectorySnapshot(state, owner, 4)

    def read(self, parent, limits):
        if parent == self.snapshot.state.parent and limits == self.snapshot.state.limits:
            return self.snapshot
        return None


def _state(tmp_path, request, *, clock=lambda: 1.0):
    store = SQLiteWindowStore(tmp_path / "retirement.sqlite", clock=clock)
    lease = store.acquire("target", "owner", 30)
    return (
        store,
        lease,
        WindowStoreSqlClientNativeRetirementState(
            store,
            lease,
            attempts=_Attempts(request),
            directories=_Directories(request),
            directory_limits=_LIMITS,
        ),
    )


def _adapter(store, lease, request):
    return WindowStoreSqlClientNativeRetirementState(
        store,
        lease,
        attempts=_Attempts(request),
        directories=_Directories(request),
        directory_limits=_LIMITS,
    )


def test_initializes_verified_progress_and_recovers_exact_state(tmp_path) -> None:
    request = _request()
    store, lease, state = _state(tmp_path, request)

    initial = state.observe(request)
    assert initial.lifecycle[0].phase is NativeChunkLifecyclePhase.VERIFIED
    assert initial.lifecycle[0].directory_revision == 4
    assert initial.lifecycle[0].lifecycle_revision == request.projection.lifecycle_revision
    assert state.seal_work(request).work_sealed

    restarted = _adapter(store, lease, request)
    assert restarted.observe(request).work_sealed


def test_exact_codec_round_trips_complete_retirement_prefix() -> None:
    request = _request()
    memory = _State(_progress(request), [])
    _service(memory, _Effects()).retire(request)

    payload = durable_state._encode(memory.progress)
    assert durable_state._decode(payload) == memory.progress
    with pytest.raises(ValueError, match="noncanonical"):
        durable_state._decode(payload.replace(":", ": ", 1))


def test_initialization_rejects_changed_verified_snapshot(tmp_path) -> None:
    request = _request()
    store = SQLiteWindowStore(tmp_path / "mismatch.sqlite", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 30)
    attempts = _Attempts(request)
    attempts.snapshot = TdsAttemptSnapshot(attempts.snapshot.state, 10)
    state = WindowStoreSqlClientNativeRetirementState(
        store,
        lease,
        attempts=attempts,
        directories=_Directories(request),
        directory_limits=_LIMITS,
    )

    with pytest.raises(ValueError, match="initial_evidence_mismatch"):
        state.observe(request)


def test_rejects_corrupt_or_differently_bound_durable_progress(tmp_path) -> None:
    request = _request()
    store, lease, state = _state(tmp_path, request)
    state.observe(request)
    record = store.load(state.storage_key(request))
    assert record is not None
    store.save(state.storage_key(request), record.revision, "{}", lease)

    with pytest.raises(ValueError, match="retirement_state_record_invalid"):
        state.observe(request)


def test_lost_save_ack_accepts_only_exact_durable_payload(tmp_path) -> None:
    request = _request()
    store, lease, state = _state(tmp_path, request)
    state.observe(request)

    class LostAckStore:
        def __getattr__(self, name):
            return getattr(store, name)

        def save(self, key, expected, payload, current_lease):
            store.save(key, expected, payload, current_lease)
            raise TimeoutError("reply lost")

    recovered = _adapter(LostAckStore(), lease, request)
    assert recovered.seal_work(request).work_sealed


def test_lost_initialization_ack_recovers_only_the_exact_record(tmp_path) -> None:
    request = _request()
    store = SQLiteWindowStore(tmp_path / "lost-init.sqlite", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 30)

    class LostInitialAckStore:
        def __init__(self):
            self.first = True

        def __getattr__(self, name):
            return getattr(store, name)

        def save(self, key, expected, payload, current_lease):
            record = store.save(key, expected, payload, current_lease)
            if self.first:
                self.first = False
                raise TimeoutError("initial reply lost")
            return record

    state = _adapter(LostInitialAckStore(), lease, request)
    assert state.observe(request).lifecycle[0].phase is NativeChunkLifecyclePhase.VERIFIED


def test_contention_reloads_identical_transition_and_rejects_divergence(tmp_path) -> None:
    request = _request()
    store, lease, state = _state(tmp_path, request)
    initial = state.observe(request)

    class ConcurrentStore:
        def __init__(self):
            self.once = True

        def __getattr__(self, name):
            return getattr(store, name)

        def save(self, key, expected, payload, current_lease):
            if self.once and expected is not None:
                self.once = False
                _adapter(store, lease, request).require_containment(request)
                raise WindowContractError("Checkpoint compare-and-swap conflict")
            return store.save(key, expected, payload, current_lease)

    contended = _adapter(ConcurrentStore(), lease, request)
    result = contended.seal_work(request)
    assert result.work_sealed
    assert result.lifecycle[-1].phase is NativeChunkLifecyclePhase.CONTAINMENT_REQUIRED
    assert initial.work_sealed is False


def test_stale_lease_cannot_load_or_advance_after_takeover(tmp_path) -> None:
    now = [1.0]
    request = _request()
    store, lease, state = _state(tmp_path, request, clock=lambda: now[0])
    state.observe(request)
    now[0] = 40.0
    successor = store.acquire("target", "successor", 30)

    with pytest.raises(WindowLeaseLost):
        state.observe(request)
    takeover = _adapter(store, successor, request)
    assert takeover.seal_work(request).work_sealed
