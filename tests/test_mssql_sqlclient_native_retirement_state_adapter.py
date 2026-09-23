import ast
from pathlib import Path
from runpy import run_path

import pytest

from dpone.adapters.mssql_sqlclient_native_retirement_state import (
    CasSqlClientNativeRetirementState,
)
from dpone.adapters.mssql_sqlclient_native_retirement_state import (
    NativeChunkRetirementProgress as StoreRetirementProgress,
)
from dpone.adapters.mssql_sqlclient_native_retirement_state import (
    NativeChunkRetirementRequest as StoreRetirementRequest,
)
from dpone.contracts.mssql_native_chunk_retirement_authority import (
    NativeChunkRetirementRequest as CanonicalRetirementRequest,
)
from dpone.contracts.mssql_native_chunk_retirement_evidence import (
    NativeChunkRetirementProgress as CanonicalRetirementProgress,
)
from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkLifecyclePhase,
    NativeChunkRetirementProgress,
    NativeChunkRetirementRequest,
    NativeChunkRetirementReservation,
)

_FIXTURES = run_path(str(Path(__file__).with_name("test_mssql_native_chunk_retirement.py")))
_Effects = _FIXTURES["_Effects"]
_progress = _FIXTURES["_progress"]
_request = _FIXTURES["_request"]


def test_window_store_depends_on_its_state_owner_instead_of_the_retirement_port() -> None:
    source = (
        Path(__file__).parents[1] / "src" / "dpone" / "adapters" / "mssql_sqlclient_native_retirement_window_store.py"
    )
    imports = {
        node.module
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8")))
        if isinstance(node, ast.ImportFrom)
    }

    assert "dpone.ports.mssql_native_chunk_retirement" not in imports


def test_retirement_port_preserves_canonical_class_identity() -> None:
    assert NativeChunkRetirementRequest is CanonicalRetirementRequest
    assert NativeChunkRetirementProgress is CanonicalRetirementProgress
    assert StoreRetirementRequest is CanonicalRetirementRequest
    assert StoreRetirementProgress is CanonicalRetirementProgress


def test_cas_state_persists_exact_monotonic_lifecycle() -> None:
    request = _request()
    slot = [0, _progress(request)]

    def load(key):
        assert key == request.projection.projection_sha256
        return slot[0], slot[1]

    def cas(key, revision, progress):
        if revision != slot[0]:
            return False
        slot[:] = [revision + 1, progress]
        return True

    state = CasSqlClientNativeRetirementState(load=load, compare_swap=cas)
    assert state.seal_work(request).work_sealed
    required = state.require_containment(request)
    assert required.lifecycle[-1].phase is NativeChunkLifecyclePhase.CONTAINMENT_REQUIRED
    contained = state.record_containment(request, _Effects().prove_containment(request))
    assert contained.lifecycle[-1].phase is NativeChunkLifecyclePhase.CONTAINED
    assert contained.containment is not None
    assert state.record_containment(request, contained.containment) == contained
    reservation = NativeChunkRetirementReservation.deterministic(request)
    reserved = state.reserve_retirement(request, reservation)
    assert reserved.lifecycle[-1].phase is NativeChunkLifecyclePhase.RETIREMENT_REQUIRED
    assert reserved.reservation == reservation
    assert state.reserve_retirement(request, reservation) == reserved


def test_cas_lost_ack_accepts_only_byte_identical_observed_progress() -> None:
    request = _request()
    slot = [0, _progress(request)]

    def cas(key, revision, progress):
        slot[:] = [revision + 1, progress]
        raise TimeoutError

    state = CasSqlClientNativeRetirementState(load=lambda key: (slot[0], slot[1]), compare_swap=cas)
    assert state.seal_work(request).work_sealed

    slot[:] = [0, _progress(request)]

    def unknown(key, revision, progress):
        raise TimeoutError

    state = CasSqlClientNativeRetirementState(load=lambda key: (slot[0], slot[1]), compare_swap=unknown)
    with pytest.raises(RuntimeError, match="outcome_unknown"):
        state.seal_work(request)


def test_cas_contention_reuses_identical_concurrent_transition() -> None:
    request = _request()
    slot = [0, _progress(request)]
    competing = CasSqlClientNativeRetirementState(
        load=lambda key: (slot[0], slot[1]),
        compare_swap=lambda key, revision, progress: False,
    )
    concurrent = competing._with_phase(request, slot[1], NativeChunkLifecyclePhase.CONTAINMENT_REQUIRED)
    first = True

    def cas(key, revision, progress):
        nonlocal first
        if first:
            first = False
            slot[:] = [revision + 1, concurrent]
            return False
        raise AssertionError("identical transition must not be persisted twice")

    state = CasSqlClientNativeRetirementState(load=lambda key: (slot[0], slot[1]), compare_swap=cas)
    result = state.require_containment(request)
    assert [proof.phase for proof in result.lifecycle] == [
        NativeChunkLifecyclePhase.VERIFIED,
        NativeChunkLifecyclePhase.CONTAINMENT_REQUIRED,
    ]
