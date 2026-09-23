"""Pure reservation binding; original-owner lifecycle effects remain in services."""

from dataclasses import fields, is_dataclass, replace
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorCommand,
    TdsCoordinatorDirectory,
    TdsDirectoryLimits,
    TdsDirectorySlot,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
)
from dpone.contracts.mssql_tds_worker import (
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsAttemptState,
    TdsObjectIdentity,
)


def validate_reservation_binding(
    parent: TdsAttemptSnapshot,
    directory: TdsDirectorySnapshot,
    identity: TdsCoordinatorIdentity,
    command: TdsCoordinatorCommand = TdsCoordinatorCommand.CREATE,
) -> None:
    """Validate original nested values before equality, hashing or serialization."""
    _validate_reservation_binding(parent, directory, identity, command, TdsAttemptPhase.CREATION_INTENT)


def _validate_reservation_binding(
    parent: TdsAttemptSnapshot,
    directory: TdsDirectorySnapshot,
    identity: TdsCoordinatorIdentity,
    command: TdsCoordinatorCommand,
    phase: TdsAttemptPhase,
) -> None:

    def check(value: object, cls: type) -> None:
        if type(value) is not cls:
            raise ValueError("mssql_native.tds_attempt_departure_binding")
        replace(value)  # type: ignore[type-var]

    if type(parent) is not TdsAttemptSnapshot or type(directory) is not TdsDirectorySnapshot:
        raise ValueError("mssql_native.tds_attempt_departure_binding")
    check(identity, TdsCoordinatorIdentity)
    check(identity.parent, TdsAttemptIdentity)
    check(parent.state, TdsAttemptState)
    check(parent.state.identity, TdsAttemptIdentity)
    check(parent.state.ownership, TdsAttemptOwnership)
    check(parent, TdsAttemptSnapshot)
    state = directory.state
    if type(state) is not TdsCoordinatorDirectory:
        raise ValueError("mssql_native.tds_attempt_departure_binding")
    check(state.parent, TdsAttemptIdentity)
    check(state.limits, TdsDirectoryLimits)
    check(directory.ownership, TdsAttemptOwnership)
    if type(state.slots) is not tuple:
        raise ValueError("mssql_native.tds_attempt_departure_binding")
    for slot in state.slots:
        check(slot, TdsDirectorySlot)
        for proof, cls in (
            (slot.local_containment, TdsLocalContainment),
            (slot.remote_settlement, TdsRemoteSettlement),
        ):
            if proof is not None:
                check(proof, cls)
    if state.retirement_authority is not None:
        raise ValueError("mssql_native.tds_attempt_departure_binding")
    check(directory, TdsDirectorySnapshot)
    check(state, TdsCoordinatorDirectory)
    if (
        parent.state.schema_version != 2
        or state.schema_version != 2
        or parent.state.phase is not phase
        or parent.state.backend != "mssql_sqlclient"
        or parent.state.identity != state.parent
        or identity.parent != state.parent
        or parent.state.ownership != directory.ownership
        or state.work_sealed
        or state.admission_closed
        or not state.slots
    ):
        raise ValueError("mssql_native.tds_attempt_departure_binding")
    slot = state.slots[-1]
    if (
        slot.command is not command
        or identity.command is not slot.command
        or slot.index != identity.slot_index
        or slot.operation_id != identity.operation_id
        or slot.command_sha256 != identity.command_sha256
        or slot.owner_fence != identity.original_fence
        or slot.owner_fence != directory.ownership.fence
        or slot.local_containment is not None
        or slot.remote_settlement is not None
    ):
        raise ValueError("mssql_native.tds_attempt_departure_binding")


def validate_prepared_grant_binding(
    parent: TdsAttemptSnapshot, directory: TdsDirectorySnapshot, identity: TdsCoordinatorIdentity
) -> None:
    """Validate a PREPARED GRANT structure, never producer provenance or authority.

    Fixed known constructors validate original leaves before directory hashing.
    No generic dataclass traversal can invoke a foreign nested constructor. The
    legacy CREATE entry retains its historical checks, including error order.
    Valid detached copies remain structures; only the original continuation can
    authorize effects using actual acknowledged observations.
    """

    def typed(value: Any, cls: type) -> None:
        if type(value) is not cls:
            raise ValueError("mssql_native.tds_attempt_departure_binding")

    def leaf(value: Any, cls: type) -> None:
        typed(value, cls)
        replace(value)

    def uuid(value: Any) -> None:
        typed(value, UUID)
        if type(value.int) is not int or not 0 < value.int < 2**128:
            raise ValueError("mssql_native.tds_attempt_departure_binding")

    typed(parent, TdsAttemptSnapshot)
    typed(directory, TdsDirectorySnapshot)
    typed(identity, TdsCoordinatorIdentity)
    typed(parent.state, TdsAttemptState)
    typed(directory.state, TdsCoordinatorDirectory)
    if parent.state.phase is not TdsAttemptPhase.PREPARED or directory.state.retirement_authority is not None:
        raise ValueError("mssql_native.tds_attempt_departure_binding")
    for value in (identity.parent, parent.state.identity, directory.state.parent):
        leaf(value, TdsAttemptIdentity)
    for ownership in (parent.state.ownership, directory.ownership):
        leaf(ownership, TdsAttemptOwnership)
    leaf(parent.state.object_identity, TdsObjectIdentity)
    uuid(identity.operation_id)
    replace(identity)
    limits = directory.state.limits
    typed(limits, TdsDirectoryLimits)
    # Its constructor uses asdict; do not let that traverse unadmitted values.
    if any(type(getattr(limits, field.name)) is not int for field in fields(TdsDirectoryLimits)):
        raise ValueError("mssql_native.tds_attempt_departure_binding")
    replace(limits)
    typed(directory.state.slots, tuple)
    for slot in directory.state.slots:
        typed(slot, TdsDirectorySlot)
        uuid(slot.operation_id)
        for proof, cls in (
            (slot.local_containment, TdsLocalContainment),
            (slot.remote_settlement, TdsRemoteSettlement),
        ):
            if proof is not None:
                typed(proof, cls)
                uuid(proof.operation_id)
                replace(proof)
        replace(slot)
    replace(parent.state)
    replace(parent)
    # Check the opaque revision and ownership before directory reconstruction
    # hashes already validated parent/slot identities.
    replace(directory)
    replace(directory.state)
    _validate_reservation_binding(parent, directory, identity, TdsCoordinatorCommand.GRANT, TdsAttemptPhase.PREPARED)


def validate_prepared_verify_binding(
    parent: TdsAttemptSnapshot, directory: TdsDirectorySnapshot, identity: TdsCoordinatorIdentity
) -> None:
    """Require VERIFY to follow the exact fully settled GRANT predecessor.

    This validates immutable structure only. The application association must
    still prove identity of the original P8 settlement capability before any
    reservation or execution effect.
    """

    _validate_reservation_binding(parent, directory, identity, TdsCoordinatorCommand.VERIFY, TdsAttemptPhase.PREPARED)
    slots = directory.state.slots
    if len(slots) < 2:
        raise ValueError("mssql_native.tds_attempt_departure_binding")
    predecessor = slots[-2]
    if (
        predecessor.command is not TdsCoordinatorCommand.GRANT
        or predecessor.local_containment is None
        or predecessor.remote_settlement is None
        or not all(slot.settled for slot in slots[:-1])
    ):
        raise ValueError("mssql_native.tds_attempt_departure_binding")


def validate_original_record(value: Any) -> None:
    if type(value) is UUID:
        if type(value.int) is not int or not 0 <= value.int < 2**128:
            raise ValueError("mssql_native.original_record_invalid")
    elif is_dataclass(value) and not isinstance(value, type):
        for field in fields(value):
            validate_original_record(getattr(value, field.name))
        replace(value)
    elif type(value) is tuple:
        for item in value:
            validate_original_record(item)
