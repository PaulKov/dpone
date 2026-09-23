"""Frozen callable dependencies for narrow SqlClient composition seams.

The factory functions deliberately resolve application implementations only when
the owning composition root is invoked.  This keeps import-time construction
acyclic while making the complete effectful dependency set explicit and
injectable for deployment composition and focused tests.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, Any, Protocol

from dpone.app.mssql_sqlclient_observe_dependencies import (
    SqlClientObserveDependencies,
    sqlclient_observe_dependencies,
)

Operation = Callable[..., Any]

if TYPE_CHECKING:
    from dpone.ports.mssql_sqlclient_writer_observer import SqlClientWriterObserverCustody
    from dpone.services.mssql_tds_writer_observation import SqlClientWriterGrantReady, SqlClientWriterResultReady
    from dpone.services.mssql_tds_writer_pregrant import SqlClientWriterPreGrant


class WriterObservationPrepare(Protocol):
    """Exact P10d preparation capability consumed by application composition."""

    def __call__(
        self,
        pregrant: SqlClientWriterPreGrant,
        *,
        observer: SqlClientWriterObserverCustody | None,
        grant_id: str | None,
        now_ns: int,
    ) -> SqlClientWriterResultReady | SqlClientWriterGrantReady: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPreparedContextDependencies:
    """Effectful operations used to advance one attempt through PREPARED."""

    create_attempt: Operation
    run_create: Operation
    settle_create: Operation
    stage_from_create: Operation
    open_observe: Operation
    settle_observe: Operation
    cleanup_attempt: Operation


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientWriterObservationDependencies:
    """Frozen core operation used by the route-aware observation seam."""

    prepare: WriterObservationPrepare


@cache
def sqlclient_writer_observation_dependencies() -> SqlClientWriterObservationDependencies:
    """Resolve the core observation operation at the composition boundary."""
    from dpone.services.mssql_tds_writer_observation import prepare_sqlclient_writer_observation

    return SqlClientWriterObservationDependencies(prepare=prepare_sqlclient_writer_observation)


def writer_observation_dependency_proxy() -> WriterObservationPrepare:
    """Return the late-bound default while retaining the legacy patch seam."""

    def invoke(
        pregrant: SqlClientWriterPreGrant,
        *,
        observer: SqlClientWriterObserverCustody | None,
        grant_id: str | None,
        now_ns: int,
    ) -> SqlClientWriterResultReady | SqlClientWriterGrantReady:
        return sqlclient_writer_observation_dependencies().prepare(
            pregrant,
            observer=observer,
            grant_id=grant_id,
            now_ns=now_ns,
        )

    return invoke


@cache
def sqlclient_prepared_context_dependencies() -> SqlClientPreparedContextDependencies:
    """Resolve the default production operations without import-time cycles."""
    from dpone.app.mssql_sqlclient_create_departure_composition import run_sqlclient_create_departure_v2
    from dpone.app.mssql_sqlclient_create_settlement import settle_sqlclient_create_departure
    from dpone.app.mssql_sqlclient_observe_composition import open_sqlclient_observe
    from dpone.app.mssql_sqlclient_observe_departure_composition import settle_prepared_observe
    from dpone.app.mssql_sqlclient_prepared_attempt_cleanup import cleanup_prepared_attempt
    from dpone.app.mssql_tds_attempt_composition import create_tds_attempt
    from dpone.contracts.mssql_sqlclient_stage_identity import stage_identity_from_create

    return SqlClientPreparedContextDependencies(
        create_attempt=create_tds_attempt,
        run_create=run_sqlclient_create_departure_v2,
        settle_create=settle_sqlclient_create_departure,
        stage_from_create=stage_identity_from_create,
        open_observe=open_sqlclient_observe,
        settle_observe=settle_prepared_observe,
        cleanup_attempt=cleanup_prepared_attempt,
    )


def prepared_context_dependency_proxy(field: str) -> Operation:
    """Return a late-bound default operation for compatibility patch seams."""

    def invoke(*args: Any, **kwargs: Any) -> Any:
        return getattr(sqlclient_prepared_context_dependencies(), field)(*args, **kwargs)

    setattr(invoke, "_dpone_dependency_field", field)
    return invoke


def prepared_context_dependencies_from_namespace(
    namespace: Mapping[str, Any],
) -> SqlClientPreparedContextDependencies:
    """Freeze one invocation bundle, retaining only the documented patch seam."""
    fields = {
        "create_attempt": "create_tds_attempt",
        "run_create": "run_sqlclient_create_departure_v2",
        "settle_create": "settle_sqlclient_create_departure",
        "stage_from_create": "stage_identity_from_create",
        "open_observe": "open_sqlclient_observe",
        "settle_observe": "settle_prepared_observe",
        "cleanup_attempt": "_cleanup",
    }
    if all(getattr(namespace[name], "_dpone_dependency_field", None) == field for field, name in fields.items()):
        return sqlclient_prepared_context_dependencies()
    return SqlClientPreparedContextDependencies(
        create_attempt=namespace["create_tds_attempt"],
        run_create=namespace["run_sqlclient_create_departure_v2"],
        settle_create=namespace["settle_sqlclient_create_departure"],
        stage_from_create=namespace["stage_identity_from_create"],
        open_observe=namespace["open_sqlclient_observe"],
        settle_observe=namespace["settle_prepared_observe"],
        cleanup_attempt=namespace["_cleanup"],
    )


__all__ = (
    "SqlClientObserveDependencies",
    "SqlClientPreparedContextDependencies",
    "SqlClientWriterObservationDependencies",
    "WriterObservationPrepare",
    "prepared_context_dependencies_from_namespace",
    "prepared_context_dependency_proxy",
    "sqlclient_prepared_context_dependencies",
    "sqlclient_observe_dependencies",
    "sqlclient_writer_observation_dependencies",
    "writer_observation_dependency_proxy",
)
