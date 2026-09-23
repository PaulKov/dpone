"""Production assembly for the closed SQLClient departure effect boundary."""

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from uuid import UUID

from dpone.adapters import mssql_tds_coordinator_connection as connection
from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_departure_evidence_actor import SqlClientDepartureEvidenceActor
from dpone.adapters.mssql_sqlclient_departure_launch import TdsLaunchUnknown
from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1
from dpone.ports.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceGateway
from dpone.ports.mssql_sqlclient_departure_infrastructure import (
    DepartureInfrastructure,
    DepartureLauncher,
    DepartureProcess,
)


def _canonicalize(payload: bytes) -> bytes:
    return connection.encode_connection_admission(*connection.decode_connection_admission(payload))


@contextmanager
def _writer(root: Path) -> Iterator[CreateOnlyEvidenceWriterV1]:
    yield DescriptorPinnedCreateOnlyEvidenceWriter(root)


def _actor(
    factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
    helper_id: UUID,
    attempt_sha256: str,
    deadline: float,
    clock: Callable[[], float],
) -> SqlClientDepartureEvidenceGateway:
    return SqlClientDepartureEvidenceActor(factory, helper_id, attempt_sha256, deadline, clock)


def _journal_failure(error: BaseException) -> tuple[bool, object | None]:
    return (True, error.gateway) if isinstance(error, TdsJournalActorUnknown) else (False, None)


def _spawn(launcher: DepartureLauncher, startup_deadline: float, operation_deadline: float) -> DepartureProcess:
    spawn = getattr(launcher, "spawn")
    return spawn(startup_deadline=startup_deadline, operation_deadline=operation_deadline)


def _launch_failure(error: BaseException) -> tuple[bool, object | None]:
    return (True, error.launch) if isinstance(error, TdsLaunchUnknown) else (False, None)


PRODUCTION_DEPARTURE_INFRASTRUCTURE = DepartureInfrastructure(
    canonicalize_admission=_canonicalize,
    evidence_writer=_writer,
    evidence_actor=_actor,
    journal_failure=_journal_failure,
    spawn=_spawn,
    launch_failure=_launch_failure,
)


def admitted_departure_launcher(launcher: DepartureLauncher) -> tuple[bytes, str, str, str, int]:
    """Validate immutable launcher facts with the production admission codec."""
    from dpone.app.mssql_sqlclient_departure_supervisor import _admission

    return _admission(launcher, PRODUCTION_DEPARTURE_INFRASTRUCTURE)
