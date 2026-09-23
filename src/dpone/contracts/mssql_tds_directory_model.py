"""Immutable coordinator discovery and admission, without storage or SQL effects.

Proofs carry caller-observed evidence bindings; constructing them proves no OS or
SQL fact. Persistence must compare the complete parent identity, fenced-CAS every
transition, and exclude late old-fence writes before treating missing children as
unlaunched. The work barrier precedes publication; cleanup has a separate barrier.
"""

from dataclasses import asdict, dataclass
from enum import StrEnum
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_sqlclient_attempt import SqlClientAttemptEvidence
from dpone.contracts.mssql_tds_worker import TdsAttemptState, state_payload
from dpone.contracts.mssql_tds_worker_identity import (
    ParentAuthority,
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsObjectIdentity,
    TdsProcessIdentity,
)
from dpone.contracts.strict_json import canonical_json_bytes

_MAX = 2**63 - 1


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("mssql_native.tds_directory_invalid")


def _int(value: object, minimum: int = 0) -> None:
    _require(type(value) is int and minimum <= value <= _MAX)


def _hash(value: object) -> None:
    _require(type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value))


def _uuid(value: object) -> None:
    _require(type(value) is UUID and value.int != 0)


class TdsCoordinatorCommand(StrEnum):
    CREATE = "create"
    OBSERVE = "observe"
    GRANT = "grant"
    VERIFY = "verify"
    RETIRE = "retire"
    CAPACITY = "capacity"
    RECONCILE = "reconcile"


@dataclass(frozen=True)
class TdsDirectoryLimits:
    """Composition admits finite work and cleanup budgets; there are no defaults."""

    max_entries: int
    reserved_retirement_entries: int
    max_encoded_bytes: int
    reserved_retirement_bytes: int

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            _int(value, 1)
        _require(self.reserved_retirement_entries < self.max_entries)
        _require(self.reserved_retirement_bytes < self.max_encoded_bytes)


@dataclass(frozen=True)
class TdsLocalContainment:
    """Caller-observed exact local incarnation containment, never remote rollback."""

    parent_sha256: str
    operation_id: UUID
    process_sha256: str
    proof_sha256: str

    def __post_init__(self) -> None:
        _uuid(self.operation_id)
        for value in (self.parent_sha256, self.process_sha256, self.proof_sha256):
            _hash(value)


@dataclass(frozen=True)
class TdsRemoteSettlement:
    """Caller-observed remote settlement under independently held SQL authority."""

    parent_sha256: str
    operation_id: UUID
    authority_sha256: str
    proof_sha256: str

    def __post_init__(self) -> None:
        _uuid(self.operation_id)
        for value in (self.parent_sha256, self.authority_sha256, self.proof_sha256):
            _hash(value)


@dataclass(frozen=True)
class TdsDirectorySlot:
    """Append-only operation binding; only its two evidence fields may advance."""

    index: int
    operation_id: UUID
    command: TdsCoordinatorCommand
    command_sha256: str
    owner_fence: int
    reconciles_slot: int | None = None
    local_containment: TdsLocalContainment | None = None
    remote_settlement: TdsRemoteSettlement | None = None

    def __post_init__(self) -> None:
        _int(self.index)
        _uuid(self.operation_id)
        _require(type(self.command) is TdsCoordinatorCommand)
        _hash(self.command_sha256)
        _int(self.owner_fence, 1)
        if self.reconciles_slot is not None:
            _int(self.reconciles_slot)
            _require(self.reconciles_slot < self.index)
        _require((self.command is TdsCoordinatorCommand.RECONCILE) == (self.reconciles_slot is not None))
        for proof, cls in (
            (self.local_containment, TdsLocalContainment),
            (self.remote_settlement, TdsRemoteSettlement),
        ):
            if proof is not None:
                _require(type(proof) is cls and proof.operation_id == self.operation_id)

    @property
    def settled(self) -> bool:
        return self.local_containment is not None and self.remote_settlement is not None


@dataclass(frozen=True)
class TdsCoordinatorDirectory:
    parent: TdsAttemptIdentity
    limits: TdsDirectoryLimits
    slots: tuple[TdsDirectorySlot, ...] = ()
    work_sealed: bool = False
    retirement_authority: TdsAttemptState | None = None
    admission_closed: bool = False
    sequence: int = 0
    schema_version: int = 1

    def __post_init__(self) -> None:
        _require(type(self.parent) is TdsAttemptIdentity and type(self.limits) is TdsDirectoryLimits)
        _require(type(self.slots) is tuple and type(self.work_sealed) is bool and type(self.admission_closed) is bool)
        _require(type(self.schema_version) is int and self.schema_version in (1, 2))
        _int(self.sequence)
        _require(self.sequence >= len(self.slots))
        _require(len(self.slots) <= self.limits.max_entries)
        ids: set[UUID] = set()
        binding = parent_digest(self.parent)
        for index, slot in enumerate(self.slots):
            _require(type(slot) is TdsDirectorySlot and slot.index == index and slot.operation_id not in ids)
            ids.add(slot.operation_id)
            for proof in (slot.local_containment, slot.remote_settlement):
                _require(proof is None or proof.parent_sha256 == binding)
            if slot.reconciles_slot is not None:
                original = self.slots[slot.reconciles_slot]
                _require(slot.reconciles_slot == index - 1 and slot.owner_fence > original.owner_fence)
                _require(original.local_containment is not None)
            elif index:
                _require(all(prior.settled for prior in self.slots[:index]))
                _require(slot.owner_fence >= self.slots[index - 1].owner_fence)
        if self.retirement_authority is not None:
            _require(type(self.retirement_authority) is TdsAttemptState and self.work_sealed)
            _require(self.retirement_authority.identity == self.parent)
            _require(self.retirement_authority.schema_version == self.schema_version)
            _require(
                self.retirement_authority.phase in (TdsAttemptPhase.CONTAINED, TdsAttemptPhase.RETIREMENT_REQUIRED)
            )
        cleanup = _cleanup_flags(self.slots)
        # Sealing precedes the first retirement; ordinary work cannot resume.
        _require(all(not previous or current for previous, current in zip(cleanup, cleanup[1:])))
        _require(not any(cleanup) or (self.work_sealed and self.retirement_authority is not None))
        _require(not self.work_sealed or all(slot.settled for slot, retired in zip(self.slots, cleanup) if not retired))
        _require(not self.admission_closed or (self.work_sealed and all(slot.settled for slot in self.slots)))
        _budget(self, cleanup)


def parent_digest(parent: TdsAttemptIdentity) -> str:
    _require(type(parent) is TdsAttemptIdentity)
    return sha256(canonical_json_bytes(asdict(parent))).hexdigest()


@dataclass(frozen=True)
class TdsDirectorySnapshot:
    """Acknowledged directory and writer ownership at one durable store revision."""

    state: TdsCoordinatorDirectory
    ownership: TdsAttemptOwnership
    revision: int

    def __post_init__(self) -> None:
        _require(type(self.state) is TdsCoordinatorDirectory and type(self.ownership) is TdsAttemptOwnership)
        _int(self.revision, 1)
        _require(all(slot.owner_fence <= self.ownership.fence for slot in self.state.slots))


def directory_key(parent: TdsAttemptIdentity) -> str:
    """Stable locator excludes mutable bindings so recovery detects identity drift."""
    _require(type(parent) is TdsAttemptIdentity)
    return (
        "mssql-tds-directory-v1/"
        + sha256(canonical_json_bytes([parent.target_key, parent.run_id, parent.ordinal, parent.attempt])).hexdigest()
    )


def _cleanup_flags(slots: tuple[TdsDirectorySlot, ...]) -> list[bool]:
    flags: list[bool] = []
    for slot in slots:
        flags.append(
            slot.command is TdsCoordinatorCommand.RETIRE
            or (slot.reconciles_slot is not None and flags[slot.reconciles_slot])
        )
    return flags


def _slot_payload(slot: TdsDirectorySlot) -> dict:
    value = asdict(slot)
    value["operation_id"] = str(slot.operation_id)
    for name in ("local_containment", "remote_settlement"):
        if value[name] is not None:
            value[name]["operation_id"] = str(value[name]["operation_id"])
    return value


def _payload(state: TdsCoordinatorDirectory) -> dict:
    return dict(
        schema=f"dpone.tds.coordinator-directory.v{state.schema_version}",
        parent=asdict(state.parent),
        limits=asdict(state.limits),
        slots=[_slot_payload(slot) for slot in state.slots],
        work_sealed=state.work_sealed,
        retirement_authority=state_payload(state.retirement_authority) if state.retirement_authority else None,
        admission_closed=state.admission_closed,
        sequence=state.sequence,
    )


def encode_directory(state: TdsCoordinatorDirectory) -> bytes:
    """Canonical record for the persistence owner; this module performs no I/O."""
    _require(type(state) is TdsCoordinatorDirectory)
    return canonical_json_bytes(_payload(state))


def _slot_bound() -> int:
    """Maximum closed slot encoding, including both future observations and comma."""
    h, uid = "f" * 64, UUID(int=1)
    slot = TdsDirectorySlot(
        _MAX,
        uid,
        TdsCoordinatorCommand.RECONCILE,
        h,
        _MAX,
        _MAX - 1,
        TdsLocalContainment(h, uid, h, h),
        TdsRemoteSettlement(h, uid, h, h),
    )
    return len(canonical_json_bytes(_slot_payload(slot))) + 1


def _retirement_bound(parent: TdsAttemptIdentity, schema_version: int = 1) -> TdsAttemptState:
    """Worst-size admitted authority shape reserves later cleanup metadata."""
    h, uid = "f" * 64, str(UUID(int=1))
    return TdsAttemptState(
        parent,
        TdsAttemptOwnership("😀" * 256, _MAX, uid),
        TdsAttemptPhase.RETIREMENT_REQUIRED,
        _MAX,
        TdsObjectIdentity(2**31 - 1, h),
        TdsProcessIdentity(h, uid, 2**31 - 1, _MAX),
        0,
        h,
        h,
        max(TdsAttemptError, key=len),
        h,
        ParentAuthority("published", h),
        schema_version=schema_version,
        backend="mssql_sqlclient" if schema_version == 2 else None,
        sqlclient=SqlClientAttemptEvidence(h, h, False, h, h) if schema_version == 2 else None,
    )


def _budget(state: TdsCoordinatorDirectory, cleanup: list[bool]) -> None:
    limits, unit = state.limits, _slot_bound()
    _require(limits.reserved_retirement_bytes >= limits.reserved_retirement_entries * unit)
    ordinary = len(cleanup) - sum(cleanup)
    _require(ordinary <= limits.max_entries - limits.reserved_retirement_entries)
    envelope = _payload(state)
    envelope.update(
        slots=[],
        work_sealed=False,
        admission_closed=False,
        sequence=_MAX,
        retirement_authority=state_payload(_retirement_bound(state.parent, state.schema_version)),
    )
    base = len(canonical_json_bytes(envelope)) + 3  # unverified exit -255 versus verified 0
    # Reserve all future proof fields at admission, so recording settlement never
    # consumes unbudgeted bytes. Cleanup has a separate partition of this budget.
    _require(
        base + ordinary * unit + max(limits.reserved_retirement_bytes, sum(cleanup) * unit) <= limits.max_encoded_bytes
    )
    _require(base + unit + limits.reserved_retirement_bytes <= limits.max_encoded_bytes)
