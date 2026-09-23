"""Closed in-process gateway requests and bounded ownership interfaces."""

from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.mssql_tds_api import TdsDirectoryLimits, WindowLease
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorEvent,
    TdsCoordinatorIdentity,
    TdsCoordinatorPhase,
    TdsCoordinatorSnapshot,
)


@dataclass(frozen=True)
class CoordinatorObservation:
    """Acknowledged immutable snapshot or absence; not writer/settlement authority."""

    snapshot: TdsCoordinatorSnapshot | None

    def __post_init__(self) -> None:
        if self.snapshot is not None and type(self.snapshot) is not TdsCoordinatorSnapshot:
            raise ValueError("mssql_native.tds_coordinator_observation_invalid")


@dataclass(frozen=True)
class ReadCoordinator:
    identity: TdsCoordinatorIdentity
    limits: TdsDirectoryLimits


@dataclass(frozen=True)
class CreateCoordinator:
    identity: TdsCoordinatorIdentity
    limits: TdsDirectoryLimits
    lease: WindowLease
    supervisor_token: str


@dataclass(frozen=True)
class TakeOverCoordinator:
    observed: TdsCoordinatorSnapshot
    limits: TdsDirectoryLimits
    lease: WindowLease
    supervisor_token: str


CoordinatorInitialization = ReadCoordinator | CreateCoordinator | TakeOverCoordinator


@dataclass(frozen=True)
class AssertCoordinatorAuthority:
    pass


@dataclass(frozen=True)
class AdvanceCoordinator:
    event: TdsCoordinatorEvent
    expected_phase: TdsCoordinatorPhase


CoordinatorRequest = AssertCoordinatorAuthority | AdvanceCoordinator


class TdsCoordinatorObserver(Protocol):
    """Synchronous actor-owned observation, without writer acquisition."""

    def read(self, identity: TdsCoordinatorIdentity) -> TdsCoordinatorSnapshot | None: ...


class TdsCoordinatorWriter(Protocol):
    """One actor owns exact CAS state; ambiguous calls permanently poison it."""

    @property
    def snapshot(self) -> TdsCoordinatorSnapshot: ...

    def assert_authority(self) -> None: ...

    def advance(self, event: TdsCoordinatorEvent, *, expected_phase: TdsCoordinatorPhase) -> TdsCoordinatorSnapshot: ...


class TdsCoordinatorStore(TdsCoordinatorObserver, Protocol):
    """Creation and takeover happen only inside the actor-owned context."""

    def create(
        self, identity: TdsCoordinatorIdentity, limits: TdsDirectoryLimits, lease: WindowLease, *, supervisor_token: str
    ) -> TdsCoordinatorWriter: ...

    def take_over(
        self, observed: TdsCoordinatorSnapshot, limits: TdsDirectoryLimits, lease: WindowLease, *, supervisor_token: str
    ) -> TdsCoordinatorWriter: ...


class TdsCoordinatorGateway(Protocol):
    """One closed in-flight request with an absolute deadline, never a SQL port."""

    @property
    def observation(self) -> CoordinatorObservation: ...

    def execute(self, request: CoordinatorRequest, *, deadline: float) -> TdsCoordinatorSnapshot: ...

    def close(self, *, deadline: float) -> None:
        """Bound teardown waiting; uncompleted actors continue consuming capacity."""
