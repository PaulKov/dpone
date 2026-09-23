"""Closed effect boundary for one SQLClient departure transcript."""

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID

from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1
from dpone.ports.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceGateway


class DepartureLauncher(Protocol):
    """Immutable admitted launcher facts consumed by application policy."""

    @property
    def admission(self) -> bytes: ...
    @property
    def admission_sha256(self) -> str: ...
    @property
    def implementation_sha256(self) -> str: ...
    @property
    def package_root(self) -> Path: ...
    @property
    def max_address_space_bytes(self) -> int: ...


class DepartureProcess(Protocol):
    """One bounded helper process, without a concrete transport dependency."""

    @property
    def identity(self) -> TdsProcessIdentity: ...
    @property
    def declared_startup(self) -> TdsCoordinatorStartup: ...
    def startup(self, *, deadline: float) -> TdsCoordinatorStartup: ...
    def send_request(self, payload: bytes, *, deadline: float) -> None: ...
    def receive_result(self, *, deadline: float) -> bytes: ...
    def wait(self, *, deadline: float) -> TdsChildExit: ...


EvidenceWriterFactory = Callable[[Path], AbstractContextManager[CreateOnlyEvidenceWriterV1]]
EvidenceActorFactory = Callable[
    [Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]], UUID, str, float, Callable[[], float]],
    SqlClientDepartureEvidenceGateway,
]


@dataclass(frozen=True, slots=True)
class DepartureInfrastructure:
    """All environment effects required by the departure supervisor.

    Exception classifiers return ``(matched, retained capability)``.  Keeping
    adapter exception types behind these functions prevents application policy
    from learning concrete process or actor implementations.
    """

    canonicalize_admission: Callable[[bytes], bytes]
    evidence_writer: EvidenceWriterFactory
    evidence_actor: EvidenceActorFactory
    journal_failure: Callable[[BaseException], tuple[bool, object | None]]
    spawn: Callable[[DepartureLauncher, float, float], DepartureProcess]
    launch_failure: Callable[[BaseException], tuple[bool, object | None]]

    def __post_init__(self) -> None:
        if not all(
            callable(value)
            for value in (
                self.canonicalize_admission,
                self.evidence_writer,
                self.evidence_actor,
                self.journal_failure,
                self.spawn,
                self.launch_failure,
            )
        ):
            raise ValueError("mssql_native.sqlclient_departure_infrastructure_invalid")
