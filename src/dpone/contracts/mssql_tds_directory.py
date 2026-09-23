"""Public coordinator-directory contracts and pure transitions."""
# ruff: noqa: F403

from dataclasses import replace
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_tds_directory_model import *
from dpone.contracts.mssql_tds_directory_model import (
    TdsCoordinatorCommand,
    TdsCoordinatorDirectory,
    TdsDirectoryLimits,
    TdsDirectorySlot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    _cleanup_flags,
    _int,
    _require,
)
from dpone.contracts.mssql_tds_directory_model import _slot_bound as _slot_bound
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptState


def initial_directory(
    parent: TdsAttemptIdentity, limits: TdsDirectoryLimits, *, schema_version: int = 1
) -> TdsCoordinatorDirectory:
    """Caller must have durably recorded the exact parent before persisting this."""
    return TdsCoordinatorDirectory(parent, limits, schema_version=schema_version)


def _next(state: TdsCoordinatorDirectory, **changes: Any) -> TdsCoordinatorDirectory:
    _require(type(state) is TdsCoordinatorDirectory)
    return replace(state, sequence=state.sequence + 1, **changes)


def reserve_operation(
    state: TdsCoordinatorDirectory,
    *,
    operation_id: UUID,
    command: TdsCoordinatorCommand,
    command_sha256: str,
    owner_fence: int,
) -> TdsCoordinatorDirectory:
    """Reserve once before creating child state; an existing UUID never reopens."""
    _require(not state.admission_closed and all(slot.settled for slot in state.slots))
    _require(command is not TdsCoordinatorCommand.RECONCILE)
    if command is TdsCoordinatorCommand.RETIRE:
        _require(state.work_sealed and state.retirement_authority is not None)
    else:
        _require(not state.work_sealed)
    slot = TdsDirectorySlot(len(state.slots), operation_id, command, command_sha256, owner_fence)
    return _next(state, slots=state.slots + (slot,))


def reserve_reconciliation(
    state: TdsCoordinatorDirectory,
    *,
    operation_id: UUID,
    command_sha256: str,
    owner_fence: int,
    reconciles_slot: int,
    containment: TdsLocalContainment,
) -> TdsCoordinatorDirectory:
    """Append recovery for the unresolved tail; never resend its original command."""
    _int(reconciles_slot)
    _require(not state.admission_closed and reconciles_slot == len(state.slots) - 1)
    original = state.slots[reconciles_slot]
    root = original
    unresolved = not root.settled
    while root.reconciles_slot is not None:
        root = state.slots[root.reconciles_slot]
        unresolved = unresolved or not root.settled
    _require(unresolved)
    _int(owner_fence, 1)
    _require(owner_fence > original.owner_fence)
    if state.work_sealed:
        _require(_cleanup_flags(state.slots)[reconciles_slot])
    observed = record_local_containment(state, reconciles_slot, containment)
    slot = TdsDirectorySlot(
        len(state.slots), operation_id, TdsCoordinatorCommand.RECONCILE, command_sha256, owner_fence, reconciles_slot
    )
    return _next(state, slots=observed.slots + (slot,))


def _observe(state: TdsCoordinatorDirectory, index: int, proof: object, *, local: bool) -> TdsCoordinatorDirectory:
    _int(index)
    _require(not state.admission_closed and index < len(state.slots))
    cls = TdsLocalContainment if local else TdsRemoteSettlement
    _require(type(proof) is cls)
    name = "local_containment" if local else "remote_settlement"
    slot = state.slots[index]
    old = getattr(slot, name)
    _require(old is None or old == proof)
    if old == proof:
        return state
    if type(proof) is TdsLocalContainment:
        changed = replace(slot, local_containment=proof)
    elif type(proof) is TdsRemoteSettlement:
        changed = replace(slot, remote_settlement=proof)
    else:
        raise ValueError("mssql_native.tds_directory_invalid")
    return _next(state, slots=state.slots[:index] + (changed,) + state.slots[index + 1 :])


def record_local_containment(
    state: TdsCoordinatorDirectory, index: int, proof: TdsLocalContainment
) -> TdsCoordinatorDirectory:
    return _observe(state, index, proof, local=True)


def record_remote_settlement(
    state: TdsCoordinatorDirectory, index: int, proof: TdsRemoteSettlement
) -> TdsCoordinatorDirectory:
    return _observe(state, index, proof, local=False)


def seal_work(state: TdsCoordinatorDirectory) -> TdsCoordinatorDirectory:
    """Irreversible barrier required before publication; staging may still exist."""
    if state.work_sealed:
        return state
    _require(not state.admission_closed and not state.work_sealed and all(slot.settled for slot in state.slots))
    return _next(state, work_sealed=True)


def authorize_retirement(state: TdsCoordinatorDirectory, authority: TdsAttemptState) -> TdsCoordinatorDirectory:
    """Bind contained failed-attempt or settled-parent cleanup authority.

    Verified staging requires its existing published/aborted parent evidence;
    an unverified contained attempt can retire without aborting the whole window.
    The caller verifies the durable observation and containment, not this model.
    """
    _require(type(authority) is TdsAttemptState)
    if state.retirement_authority is not None:
        _require(state.retirement_authority == authority)
        return state
    _require(state.work_sealed and not state.admission_closed)
    return _next(state, retirement_authority=authority)


def close_admission(state: TdsCoordinatorDirectory) -> TdsCoordinatorDirectory:
    """Final admission closure, never permission to release object reservations.

    Resource release additionally requires a separately verified exact-object
    absence/never-created retirement receipt. Settled SQL can still leave a table.
    """
    if state.admission_closed:
        return state
    _require(state.work_sealed and all(slot.settled for slot in state.slots))
    return _next(state, admission_closed=True)
