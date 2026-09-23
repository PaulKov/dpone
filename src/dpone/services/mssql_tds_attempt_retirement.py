"""Process ownership and ordered terminal settlement for one TDS attempt."""

from __future__ import annotations

import math
import os
from dataclasses import fields, is_dataclass, replace
from enum import Enum
from threading import current_thread
from typing import Any, Protocol
from uuid import UUID

from dpone.ports.mssql_tds_directory import (
    AuthorizeDirectoryRetirement,
    CloseDirectoryAdmission,
    RecordDirectoryContainment,
    RecordDirectorySettlement,
    ReserveDirectoryOperation,
    SealDirectoryWork,
    TdsLocalContainment,
    TdsRemoteSettlement,
    parent_digest,
)
from dpone.ports.mssql_tds_journal import ParentAuthority, ParentRetirementRequired, Retired
from dpone.ports.mssql_tds_suspension import TdsAttemptSuspension, _issue_tds_attempt_suspension
from dpone.services.mssql_tds_writer_contracts import (
    Contained,
    RetirementRequired,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsAttemptState,
    TdsCoordinatorCommand,
    TdsDirectorySnapshot,
    WindowContractError,
    WindowOutcomeUnknown,
)


class ShutdownCapability(Protocol):
    def close(self, *, deadline: float) -> None: ...


class AttemptDepartureViewMixin:
    """Read-only projections of the retained CREATE settlement."""

    @property
    def _create_departure_outcome(self: Any):
        return self._create_settlement.outcome if self._create_settlement is not None else None

    @property
    def _create_departure_registered(self: Any):
        return self._create_settlement.registered_outcome if self._create_settlement is not None else None

    @property
    def _create_departure_snapshot(self: Any):
        return self._create_settlement.original_outcome if self._create_settlement is not None else None


class TdsAttemptUnknown(WindowOutcomeUnknown):
    def __init__(self, gateways: tuple[ShutdownCapability, ...]) -> None:
        self._gateways = gateways
        self._pid, self._thread = os.getpid(), current_thread()
        super().__init__("mssql_native.tds_attempt_composition_unknown")

    def close(self, *, deadline: float) -> None:
        assert_local_owner(self._pid, self._thread)
        validate_deadline(deadline)
        failed = False
        for gateway in self._gateways:
            try:
                gateway.close(deadline=deadline)
            except BaseException:
                failed = True
        if failed:
            raise self


def assert_local_owner(pid: int, thread: object) -> None:
    """Reject fork/thread misuse before touching inherited synchronization."""
    if os.getpid() != pid or current_thread() is not thread:
        raise WindowContractError("mssql_native.tds_attempt_owner_mismatch")


def validate_deadline(deadline: float) -> None:
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ValueError("mssql_native.tds_attempt_deadline_invalid")


def same_state_tree(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False
    kind = type(left)
    if isinstance(left, Enum) or kind in (type(None), bool, int, float, str, bytes, UUID):
        return left is right if isinstance(left, Enum) else left == right
    if type(left) is tuple and type(right) is tuple:
        return len(left) == len(right) and all(same_state_tree(a, b) for a, b in zip(left, right, strict=True))
    if is_dataclass(kind):
        raw = object.__getattribute__
        return all(same_state_tree(raw(left, f.name), raw(right, f.name)) for f in fields(kind))
    return False


def assert_retirement_descendant(saved: TdsAttemptState, current: TdsAttemptState) -> None:
    """Require current retirement authority to descend only by valid takeover steps."""
    phases = (TdsAttemptPhase.CONTAINED, TdsAttemptPhase.RETIREMENT_REQUIRED)
    if saved.phase not in phases or current.phase not in phases:
        raise WindowContractError("mssql_native.tds_attempt_retirement_evidence_changed")

    phase_steps = phases.index(current.phase) - phases.index(saved.phase)
    takeover_steps = current.sequence - saved.sequence - phase_steps
    fence_steps = current.ownership.fence - saved.ownership.fence
    same_owner = current.ownership == saved.ownership
    allowed_owner = (same_owner and takeover_steps == 0) or (
        fence_steps > 0
        and 1 <= takeover_steps <= fence_steps
        and (takeover_steps >= 2 or current.ownership.supervisor_id != saved.ownership.supervisor_id)
    )
    if (
        phase_steps < 0
        or not allowed_owner
        or replace(saved, ownership=current.ownership, sequence=current.sequence, phase=current.phase) != current
    ):
        raise WindowContractError("mssql_native.tds_attempt_retirement_evidence_changed")


def reserve_tds_attempt_retirement(
    attempt: Any, operation_id: UUID, command_sha256: str, *, deadline: float
) -> TdsDirectorySnapshot:
    """Transfer exact failed-attempt authority into one RETIRE reservation."""
    with attempt._sequence(deadline):
        phase = attempt._lifecycle.snapshot.state.phase
        attempt._require_unpoisoned()
        if phase not in (TdsAttemptPhase.CONTAINED, TdsAttemptPhase.RETIREMENT_REQUIRED):
            raise WindowContractError("mssql_native.tds_attempt_containment_required")
        directory = attempt._directory.observation.snapshot
        attempt._require_unpoisoned()
        if directory is None or directory.state.admission_closed:
            raise WindowContractError("mssql_native.tds_attempt_retirement_admission_closed")
        saved_authority = directory.state.retirement_authority
        if saved_authority is not None:
            assert_retirement_descendant(saved_authority, attempt._lifecycle.snapshot.state)
            attempt._require_unpoisoned()
        attempt._directory.execute(SealDirectoryWork(), deadline=deadline)
        attempt._require_unpoisoned()
        if phase is TdsAttemptPhase.CONTAINED:
            attempt._lifecycle.advance(RetirementRequired(), expected_phase=phase, deadline=deadline)
            attempt._require_unpoisoned()
            attempt._lifecycle.assert_authority(deadline=deadline)
            attempt._require_unpoisoned()
        authority = attempt._lifecycle.snapshot.state
        if saved_authority is None:
            attempt._directory.execute(AuthorizeDirectoryRetirement(authority), deadline=deadline)
            attempt._require_unpoisoned()
        return attempt._directory.execute(
            ReserveDirectoryOperation(operation_id, TdsCoordinatorCommand.RETIRE, command_sha256), deadline=deadline
        )


def contain_tds_verified_parent(
    attempt: Any, authority: ParentAuthority, proof_sha256: str, *, deadline: float
) -> TdsAttemptSnapshot:
    """Bind published authority and durably contain one verified attempt."""
    if type(authority) is not ParentAuthority:
        raise ValueError("mssql_native.tds_parent_retirement_authority_invalid")
    with attempt._sequence(deadline):
        phase = attempt._lifecycle.snapshot.state.phase
        if phase is not TdsAttemptPhase.VERIFIED:
            raise WindowContractError("mssql_native.tds_verified_parent_required")
        attempt._lifecycle.advance(ParentRetirementRequired(authority), expected_phase=phase, deadline=deadline)
        attempt._require_unpoisoned()
        return attempt._lifecycle.advance(
            Contained(proof_sha256),
            expected_phase=TdsAttemptPhase.CONTAINMENT_REQUIRED,
            deadline=deadline,
        )


def suspend_tds_attempt(attempt: Any, *, deadline: float) -> TdsAttemptSuspension:
    """Close both actors and issue one process-local same-fence continuation."""
    attempt._owned()
    if attempt._closed or attempt._busy or attempt._poisoned:
        raise WindowContractError("mssql_native.tds_attempt_suspension_unavailable")
    lifecycle, directory = attempt.lifecycle, attempt.directory
    attempt.close(deadline=deadline)
    return _issue_tds_attempt_suspension(lifecycle, directory)


def seal_tds_attempt_work(attempt: Any, *, deadline: float) -> TdsDirectorySnapshot:
    """Seal the current directory under the attempt sequence lock."""
    with attempt._sequence(deadline):
        return attempt._directory.execute(SealDirectoryWork(), deadline=deadline)


def close_tds_attempt_admission(attempt: Any, *, deadline: float) -> TdsDirectorySnapshot:
    """Close directory admission under the attempt sequence lock."""
    with attempt._sequence(deadline):
        return attempt._directory.execute(CloseDirectoryAdmission(), deadline=deadline)


def complete_tds_attempt_retirement(
    attempt: Any,
    operation_id: UUID,
    process_absence_sha256: str,
    object_absence_sha256: str,
    settlement_authority_sha256: str,
    remote_settlement_sha256: str,
    retired_sha256: str,
    *,
    deadline: float,
) -> tuple[TdsAttemptSnapshot, TdsDirectorySnapshot]:
    """Settle RETIRE evidence and lifecycle inside the attempt's sequence lock."""
    if type(operation_id) is not UUID:
        raise ValueError("mssql_native.tds_attempt_retirement_settlement_invalid")
    with attempt._sequence(deadline):
        lifecycle = attempt._lifecycle.snapshot
        phase = lifecycle.state.phase
        if phase not in (TdsAttemptPhase.RETIREMENT_REQUIRED, TdsAttemptPhase.RETIRED):
            raise WindowContractError("mssql_native.tds_attempt_retirement_required")
        directory = attempt._directory.observation.snapshot
        if directory is None:
            raise WindowContractError("mssql_native.tds_attempt_directory_missing")
        slots = tuple(slot for slot in directory.state.slots if slot.operation_id == operation_id)
        if len(slots) != 1 or slots[0].command is not TdsCoordinatorCommand.RETIRE:
            raise WindowContractError("mssql_native.tds_attempt_retirement_slot_invalid")
        slot = slots[0]
        parent_sha256 = parent_digest(lifecycle.state.identity)
        local = TdsLocalContainment(parent_sha256, operation_id, process_absence_sha256, object_absence_sha256)
        remote = TdsRemoteSettlement(
            parent_sha256,
            operation_id,
            settlement_authority_sha256,
            remote_settlement_sha256,
        )
        if slot.local_containment != local:
            attempt._directory.execute(RecordDirectoryContainment(slot.index, local), deadline=deadline)
        current = attempt._directory.observation.snapshot
        if current is None:
            raise WindowContractError("mssql_native.tds_attempt_directory_missing")
        slot = current.state.slots[slot.index]
        if slot.remote_settlement != remote:
            attempt._directory.execute(RecordDirectorySettlement(slot.index, remote), deadline=deadline)
        if phase is TdsAttemptPhase.RETIREMENT_REQUIRED:
            attempt._lifecycle.advance(Retired(retired_sha256), expected_phase=phase, deadline=deadline)
        lifecycle = attempt._lifecycle.snapshot
        directory = attempt._directory.execute(CloseDirectoryAdmission(), deadline=deadline)
        if lifecycle.state.phase is not TdsAttemptPhase.RETIRED or not directory.state.admission_closed:
            raise WindowContractError("mssql_native.tds_attempt_retirement_incomplete")
        return lifecycle, directory
