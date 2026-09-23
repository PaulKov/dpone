"""Closed in-process gateway requests and bounded ownership interfaces."""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from dpone.contracts.mssql_tds_api import TdsAttemptIdentity, TdsAttemptState, WindowLease
from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorCommand,
    TdsDirectoryLimits,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
)
from dpone.contracts.mssql_tds_directory import parent_digest as parent_digest
from dpone.contracts.mssql_tds_suspension import TdsAttemptResumeClaim


@dataclass(frozen=True)
class DirectoryObservation:
    snapshot: TdsDirectorySnapshot | None

    def __post_init__(self) -> None:
        if self.snapshot is not None and type(self.snapshot) is not TdsDirectorySnapshot:
            raise ValueError("mssql_native.tds_directory_observation_invalid")


@dataclass(frozen=True)
class ReadDirectory:
    parent: TdsAttemptIdentity
    limits: TdsDirectoryLimits


@dataclass(frozen=True)
class CreateDirectory:
    parent: TdsAttemptIdentity
    limits: TdsDirectoryLimits
    lease: WindowLease
    supervisor_token: str


@dataclass(frozen=True)
class TakeOverDirectory:
    observed: TdsDirectorySnapshot
    lease: WindowLease
    supervisor_token: str


@dataclass(frozen=True)
class ResumeDirectory:
    """Consume one process-local suspension claim under unchanged ownership."""

    claim: TdsAttemptResumeClaim
    lease: WindowLease


DirectoryInitialization = ReadDirectory | CreateDirectory | TakeOverDirectory | ResumeDirectory


@dataclass(frozen=True)
class AssertDirectoryAuthority:
    pass


@dataclass(frozen=True)
class ReserveDirectoryOperation:
    operation_id: UUID
    command: TdsCoordinatorCommand
    command_sha256: str


@dataclass(frozen=True)
class ReserveDirectoryReconciliation:
    operation_id: UUID
    command_sha256: str
    reconciles_slot: int
    containment: TdsLocalContainment


@dataclass(frozen=True)
class RecordDirectoryContainment:
    index: int
    proof: TdsLocalContainment


@dataclass(frozen=True)
class RecordDirectorySettlement:
    index: int
    proof: TdsRemoteSettlement


@dataclass(frozen=True)
class SealDirectoryWork:
    pass


@dataclass(frozen=True)
class AuthorizeDirectoryRetirement:
    authority: TdsAttemptState


@dataclass(frozen=True)
class CloseDirectoryAdmission:
    pass


DirectoryRequest = (
    AssertDirectoryAuthority
    | ReserveDirectoryOperation
    | ReserveDirectoryReconciliation
    | RecordDirectoryContainment
    | RecordDirectorySettlement
    | SealDirectoryWork
    | AuthorizeDirectoryRetirement
    | CloseDirectoryAdmission
)


class TdsDirectoryWriter(Protocol):
    """Closed transitions on one actor-owned, acknowledged CAS snapshot."""

    @property
    def snapshot(self) -> TdsDirectorySnapshot: ...

    def assert_authority(self) -> None: ...

    def reserve_operation(
        self, *, operation_id: UUID, command: TdsCoordinatorCommand, command_sha256: str
    ) -> TdsDirectorySnapshot: ...

    def reserve_reconciliation(
        self, *, operation_id: UUID, command_sha256: str, reconciles_slot: int, containment: TdsLocalContainment
    ) -> TdsDirectorySnapshot: ...

    def record_local_containment(self, index: int, proof: TdsLocalContainment) -> TdsDirectorySnapshot: ...

    def record_remote_settlement(self, index: int, proof: TdsRemoteSettlement) -> TdsDirectorySnapshot: ...

    def seal_work(self) -> TdsDirectorySnapshot: ...

    def authorize_retirement(self, authority: TdsAttemptState) -> TdsDirectorySnapshot: ...

    def close_admission(self) -> TdsDirectorySnapshot: ...


class TdsDirectoryObserver(Protocol):
    """Read exact directory state without acquiring creation or writer authority."""

    def read(self, parent: TdsAttemptIdentity, limits: TdsDirectoryLimits) -> TdsDirectorySnapshot | None: ...


class TdsDirectoryStore(TdsDirectoryObserver, Protocol):
    """Initialization is performed only inside the actor-owned context."""

    def create(
        self, parent: TdsAttemptIdentity, limits: TdsDirectoryLimits, lease: WindowLease, *, supervisor_token: str
    ) -> TdsDirectoryWriter: ...

    def take_over(
        self, observed: TdsDirectorySnapshot, lease: WindowLease, *, supervisor_token: str
    ) -> TdsDirectoryWriter: ...

    def resume(self, claim: TdsAttemptResumeClaim, lease: WindowLease) -> TdsDirectoryWriter: ...


class TdsDirectoryGateway(Protocol):
    """Supervisor-only interface; timeouts retain capacity and forbid reuse."""

    @property
    def observation(self) -> DirectoryObservation:
        """Last acknowledged immutable observation; absence never creates state."""

    def execute(self, request: DirectoryRequest, *, deadline: float) -> TdsDirectorySnapshot:
        """Await one closed writer command; late results cannot refresh authority."""

    def close(self, *, deadline: float) -> None:
        """Bound actor teardown wait, distinct from durable admission closure."""
