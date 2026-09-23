"""Route-aware composition for P10d observer inputs before irreversible claim."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from dpone.app.mssql_sqlclient_dependency_bundle import (
    SqlClientWriterObservationDependencies,
    sqlclient_writer_observation_dependencies,
    writer_observation_dependency_proxy,
)
from dpone.ports.mssql_sqlclient_writer_observer import SqlClientWriterObserverCustody
from dpone.services.mssql_tds_writer_pregrant import SqlClientWriterPreGrant

ERROR = "mssql_native.sqlclient_writer_observation_composition_invalid"
prepare_sqlclient_writer_observation = writer_observation_dependency_proxy()
_DEFAULT_PREPARE = prepare_sqlclient_writer_observation


@dataclass(frozen=True, slots=True)
class SqlClientWriterObservationInputs:
    """Already contained session-route inputs, fully materialized preclaim."""

    observer: SqlClientWriterObserverCustody
    grant_id: str
    now_ns: int

    def __post_init__(self) -> None:
        try:
            parsed = UUID(self.grant_id)
        except (ValueError, TypeError, AttributeError):
            raise ValueError(ERROR) from None
        if (
            not isinstance(self.observer, SqlClientWriterObserverCustody)
            or type(self.grant_id) is not str
            or not parsed.int
            or str(parsed) != self.grant_id
            or type(self.now_ns) is not int
            or self.now_ns < 0
        ):
            raise ValueError(ERROR)


def compose_sqlclient_writer_observation(
    pregrant: SqlClientWriterPreGrant,
    *,
    session_inputs: Callable[[], SqlClientWriterObservationInputs],
    dependencies: SqlClientWriterObservationDependencies | None = None,
) -> Any:
    """Avoid observer acquisition for result routes; materialize session inputs before claim."""
    if not isinstance(pregrant, SqlClientWriterPreGrant) or not callable(session_inputs):
        raise ValueError(ERROR)
    if type(pregrant)._p10d_identity(pregrant) is not pregrant:
        raise ValueError(ERROR)
    if dependencies is not None:
        operation = dependencies
    elif prepare_sqlclient_writer_observation is not _DEFAULT_PREPARE:
        operation = SqlClientWriterObservationDependencies(prepare=prepare_sqlclient_writer_observation)
    else:
        operation = sqlclient_writer_observation_dependencies()
    if type(operation) is not SqlClientWriterObservationDependencies or not callable(operation.prepare):
        raise ValueError(ERROR)
    route = type(pregrant)._p10d_route(pregrant, pregrant)
    if route == "result":
        return operation.prepare(pregrant, observer=None, grant_id=None, now_ns=0)
    if route != "session":
        raise ValueError(ERROR)
    inputs = session_inputs()
    if type(inputs) is not SqlClientWriterObservationInputs:
        raise ValueError(ERROR)
    observer = inputs.observer
    if not isinstance(observer, SqlClientWriterObserverCustody):
        raise ValueError(ERROR)
    if type(observer)._p10d_identity(observer) is not observer:
        raise ValueError(ERROR)
    cleanup = type(observer)._prepare_cleanup(observer, observer)
    try:
        inputs.__post_init__()
        return operation.prepare(
            pregrant,
            observer=observer,
            grant_id=inputs.grant_id,
            now_ns=inputs.now_ns,
        )
    except BaseException:
        type(cleanup)._cleanup_once(cleanup)
        raise


__all__ = (
    "SqlClientWriterObservationDependencies",
    "SqlClientWriterObservationInputs",
    "compose_sqlclient_writer_observation",
)
