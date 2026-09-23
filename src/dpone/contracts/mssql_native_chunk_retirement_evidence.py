"""Immutable operation, settlement and progress evidence for P10g retirement."""

# ruff: noqa: F405

from __future__ import annotations

from dataclasses import asdict, dataclass
from uuid import UUID

from dpone.contracts.mssql_native_chunk_retirement_authority import (
    _ERROR,
    NativeChunkContainmentProof,
    NativeChunkDropIntent,
    NativeChunkDropOutcome,
    NativeChunkDropProof,
    NativeChunkLifecycleProof,
    NativeChunkRetirementReservation,
    _bind_simple,
    _digest,
    _simple_digest,
)
from dpone.contracts.mssql_native_parent_journal import canonical_digest


@dataclass(frozen=True, slots=True)
class NativeChunkSettlementProof:
    """Durable local and remote settlement for one known DROP result."""

    projection_sha256: str
    authorization_sha256: str
    operation_id: UUID
    operation_sha256: str
    effect_attempt: int
    drop_proof_sha256: str
    object_incarnation_sha256: str
    directory_key: str
    directory_revision: int
    authority_digest: str
    authority_fence: int
    local_settlement_sha256: str
    remote_settlement_sha256: str
    outcome: NativeChunkDropOutcome = NativeChunkDropOutcome.SUCCEEDED
    proof_sha256: str = ""

    def __post_init__(self) -> None:
        if type(self.operation_id) is not UUID or not self.operation_id.int or type(self.directory_key) is not str:
            raise ValueError(_ERROR)
        if (
            type(self.effect_attempt) is not int
            or self.effect_attempt not in (0, 1)
            or type(self.directory_revision) is not int
            or self.directory_revision < 1
            or type(self.authority_fence) is not int
            or self.authority_fence < 1
        ):
            raise ValueError(_ERROR)
        for value in (
            self.projection_sha256,
            self.authorization_sha256,
            self.operation_sha256,
            self.drop_proof_sha256,
            self.object_incarnation_sha256,
            self.authority_digest,
            self.local_settlement_sha256,
            self.remote_settlement_sha256,
            self.proof_sha256,
        ):
            _digest(value)
        if self.outcome not in (NativeChunkDropOutcome.SUCCEEDED, NativeChunkDropOutcome.FAILED):
            raise ValueError(_ERROR)
        if self.proof_sha256 != _settlement_digest(self):
            raise ValueError(_ERROR)


def _settlement_digest(value: NativeChunkSettlementProof) -> str:
    body = asdict(value)
    body["operation_id"] = str(value.operation_id)
    body["outcome"] = value.outcome.value
    body.pop("proof_sha256")
    return canonical_digest(body)


def bind_native_chunk_settlement(**facts: object) -> NativeChunkSettlementProof:
    provisional = object.__new__(NativeChunkSettlementProof)
    for name, value in facts.items():
        object.__setattr__(provisional, name, value)
    object.__setattr__(provisional, "proof_sha256", "0" * 64)
    return NativeChunkSettlementProof(**facts, proof_sha256=_settlement_digest(provisional))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class NativeChunkAbsenceProof:
    """Exact post-DROP catalog observation; false is never cleanup authority."""

    object_incarnation_sha256: str
    settlement_sha256: str
    directory_revision: int
    lifecycle_revision: int
    proof_sha256: str
    absent: bool

    def __post_init__(self) -> None:
        _digest(self.object_incarnation_sha256)
        _digest(self.settlement_sha256)
        _digest(self.proof_sha256)
        if (
            type(self.directory_revision) is not int
            or self.directory_revision < 1
            or type(self.lifecycle_revision) is not int
            or self.lifecycle_revision < 1
            or type(self.absent) is not bool
        ):
            raise ValueError(_ERROR)
        if self.proof_sha256 != _simple_digest(self):
            raise ValueError(_ERROR)


def bind_native_chunk_absence(**facts: object) -> NativeChunkAbsenceProof:
    return _bind_simple(NativeChunkAbsenceProof, facts)


@dataclass(frozen=True, slots=True)
class NativeChunkCapacityProof:
    """Observed release of the reservation after closed directory admission."""

    proof_sha256: str
    directory_sha256: str
    released: bool

    def __post_init__(self) -> None:
        _digest(self.proof_sha256)
        _digest(self.directory_sha256)
        if type(self.released) is not bool:
            raise ValueError(_ERROR)
        if self.proof_sha256 != _simple_digest(self):
            raise ValueError(_ERROR)


def bind_native_chunk_capacity(**facts: object) -> NativeChunkCapacityProof:
    return _bind_simple(NativeChunkCapacityProof, facts)


@dataclass(frozen=True, slots=True)
class NativeChunkRetiredTerminal:
    """Exact acknowledged RETIRED lifecycle snapshot."""

    projection_sha256: str
    authorization_sha256: str
    parent_authority_digest: str
    operation_sha256: str
    absence_sha256: str
    directory_revision: int
    lifecycle_revision: int
    lifecycle_proof_sha256: str
    proof_sha256: str

    def __post_init__(self) -> None:
        for value in (
            self.projection_sha256,
            self.authorization_sha256,
            self.parent_authority_digest,
            self.operation_sha256,
            self.absence_sha256,
            self.lifecycle_proof_sha256,
            self.proof_sha256,
        ):
            _digest(value)
        if (
            type(self.directory_revision) is not int
            or self.directory_revision < 1
            or type(self.lifecycle_revision) is not int
            or self.lifecycle_revision < 1
            or self.proof_sha256 != _terminal_digest(self)
        ):
            raise ValueError(_ERROR)


def _terminal_digest(value: NativeChunkRetiredTerminal) -> str:
    body = asdict(value)
    body.pop("proof_sha256")
    return canonical_digest(body)


def bind_native_chunk_retired_terminal(**facts: object) -> NativeChunkRetiredTerminal:
    provisional = object.__new__(NativeChunkRetiredTerminal)
    for name, value in facts.items():
        object.__setattr__(provisional, name, value)
    object.__setattr__(provisional, "proof_sha256", "0" * 64)
    return NativeChunkRetiredTerminal(**facts, proof_sha256=_terminal_digest(provisional))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class NativeChunkClosedDirectory:
    """Exact closed-admission directory snapshot after RETIRED."""

    projection_sha256: str
    parent_authority_digest: str
    operation_sha256: str
    terminal_sha256: str
    proof_sha256: str

    def __post_init__(self) -> None:
        for value in (
            self.projection_sha256,
            self.parent_authority_digest,
            self.operation_sha256,
            self.terminal_sha256,
            self.proof_sha256,
        ):
            _digest(value)
        if self.proof_sha256 != _simple_digest(self):
            raise ValueError(_ERROR)


def bind_native_chunk_closed_directory(**facts: object) -> NativeChunkClosedDirectory:
    return _bind_simple(NativeChunkClosedDirectory, facts)


@dataclass(frozen=True, slots=True, kw_only=True)
class NativeChunkRetirementProgress:
    """Acknowledged immutable suffix progress; optional fields form a prefix."""

    projection_sha256: str
    parent_authority_digest: str
    authorization_sha256: str
    lifecycle: tuple[NativeChunkLifecycleProof, ...]
    work_sealed: bool = False
    containment: NativeChunkContainmentProof | None = None
    reservation: NativeChunkRetirementReservation | None = None
    drop_intent: NativeChunkDropIntent | None = None
    drop: NativeChunkDropProof | None = None
    settlement: NativeChunkSettlementProof | None = None
    absence: NativeChunkAbsenceProof | None = None
    terminal: NativeChunkRetiredTerminal | None = None
    admission_closed: bool = False
    directory: NativeChunkClosedDirectory | None = None
    capacity: NativeChunkCapacityProof | None = None

    def __post_init__(self) -> None:
        _digest(self.projection_sha256)
        _digest(self.parent_authority_digest)
        _digest(self.authorization_sha256)
        if (
            type(self.lifecycle) is not tuple
            or not self.lifecycle
            or any(type(value) is not NativeChunkLifecycleProof for value in self.lifecycle)
        ):
            raise ValueError(_ERROR)
        if type(self.work_sealed) is not bool or type(self.admission_closed) is not bool:
            raise ValueError(_ERROR)
        for value, cls in (
            (self.containment, NativeChunkContainmentProof),
            (self.reservation, NativeChunkRetirementReservation),
            (self.drop_intent, NativeChunkDropIntent),
            (self.drop, NativeChunkDropProof),
            (self.settlement, NativeChunkSettlementProof),
            (self.absence, NativeChunkAbsenceProof),
            (self.terminal, NativeChunkRetiredTerminal),
            (self.directory, NativeChunkClosedDirectory),
            (self.capacity, NativeChunkCapacityProof),
        ):
            if value is not None and type(value) is not cls:
                raise ValueError(_ERROR)
        prefix = (
            self.work_sealed,
            self.containment is not None,
            self.reservation is not None,
            self.drop_intent is not None,
            self.drop is not None,
            self.settlement is not None,
            self.absence is not None,
            self.terminal is not None,
            self.admission_closed,
            self.directory is not None,
            self.capacity is not None,
        )
        if any(current and not previous for previous, current in zip(prefix, prefix[1:])):
            raise ValueError(_ERROR)
        if self.absence is not None and not self.absence.absent:
            raise ValueError(_ERROR)
        if self.absence is not None and (
            self.settlement is None or self.settlement.outcome is not NativeChunkDropOutcome.SUCCEEDED
        ):
            raise ValueError(_ERROR)
        if self.capacity is not None and not self.capacity.released:
            raise ValueError(_ERROR)


__all__ = tuple(name for name in globals() if name.startswith("NativeChunk") or name.startswith("bind_native"))
