"""Read-only observations of retained SQLClient helper resources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, overload

from dpone.adapters.mssql_sqlclient_departure_process import SqlClientDepartureProcess
from dpone.contracts.mssql_sqlclient_departure_models import (
    TdsChildExit,
    TdsCoordinatorStartup,
    TdsProcessIdentity,
)
from dpone.ports.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceGateway
from dpone.ports.mssql_tds_worker import TdsUnresolvedLaunch

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class DepartureCustodySnapshot:
    """One immutable projection of helper custody at an observation boundary."""

    helper_evidence: SqlClientDepartureEvidenceGateway | None
    helper_evidence_closed: bool
    child: SqlClientDepartureProcess | None
    unresolved_launch: TdsUnresolvedLaunch | None
    process: TdsProcessIdentity | None
    startup: TdsCoordinatorStartup | None
    declared_startup: TdsCoordinatorStartup | None
    raw_result: bytes | None
    local_exit: TdsChildExit | None
    child_close_attempted: bool
    child_closed: bool
    unresolved_close_attempted: bool
    unresolved_closed: bool
    containment_deadline: float | None
    containment_budget_captured: bool


class SnapshotProvider(Protocol):
    """Structural source for a fresh immutable custody observation."""

    def snapshot(self) -> DepartureCustodySnapshot: ...


class SnapshotField(Generic[T]):
    """Read-only compatibility descriptor backed by a fresh snapshot."""

    def __init__(self, getter: Callable[[DepartureCustodySnapshot], T]) -> None:
        self._getter = getter

    @overload
    def __get__(self, instance: None, owner: type[Any]) -> SnapshotField[T]: ...

    @overload
    def __get__(self, instance: SnapshotProvider, owner: type[Any]) -> T: ...

    def __get__(self, instance: SnapshotProvider | None, owner: type[Any]) -> SnapshotField[T] | T:
        if instance is None:
            return self
        return self._getter(instance.snapshot())

    def __set__(self, instance: SnapshotProvider, value: T) -> None:
        raise AttributeError("custody observations are read-only")


def snapshot_field(getter: Callable[[DepartureCustodySnapshot], T]) -> SnapshotField[T]:
    """Declare a typed compatibility view without returning mutable custody state."""
    return SnapshotField(getter)
