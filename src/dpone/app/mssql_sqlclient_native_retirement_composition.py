"""Compose admitted SQL Server retirement effects over durable P10g state."""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass

from dpone.adapters.mssql_sqlclient_native_retirement_effects import CallbackSqlClientNativeRetirementEffects
from dpone.adapters.mssql_sqlclient_native_retirement_operator import SqlClientNativeRetirementOperator
from dpone.adapters.mssql_sqlclient_native_retirement_session import SqlClientRetirementSession
from dpone.adapters.mssql_sqlclient_native_retirement_window_store import WindowStoreSqlClientNativeRetirementState
from dpone.contracts.mssql_native_chunk_retirement_authority import NativeChunkRetirementRequest
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
)
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.ports.mssql_tds_directory import TdsDirectoryObserver
from dpone.ports.mssql_tds_journal import TdsAttemptObserver

_ERROR = "mssql_native.sqlclient_retirement_composition_invalid"


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeRetirementDeployment:
    """Management-session authority and durable observer identities for P10g."""

    open_management_session: Callable[[], AbstractContextManager[SqlClientRetirementSession]]
    admission: SqlClientObserverAdmission
    expected_server: SqlClientServerAuthority
    expected_database: SqlClientDatabaseAuthority
    lifecycle_observer: TdsAttemptObserver
    directory_observer: TdsDirectoryObserver
    directory_limits: TdsDirectoryLimits
    observe_capacity_release: Callable[[NativeChunkRetirementRequest], bool] | None = None
    monotonic: Callable[[], float] = time.monotonic
    operation_timeout: float = 30.0


def compose_sqlclient_native_retirement_effects(
    deployment: SqlClientNativeRetirementDeployment,
    state: WindowStoreSqlClientNativeRetirementState,
) -> CallbackSqlClientNativeRetirementEffects:
    """Bind all seven effects to one exact nominal, restart-safe operator."""
    if (
        type(deployment) is not SqlClientNativeRetirementDeployment
        or type(state) is not WindowStoreSqlClientNativeRetirementState
        or not callable(deployment.open_management_session)
        or type(deployment.admission) is not SqlClientObserverAdmission
        or type(deployment.expected_server) is not SqlClientServerAuthority
        or type(deployment.expected_database) is not SqlClientDatabaseAuthority
        or deployment.admission.server != deployment.expected_server
        or deployment.admission.database != deployment.expected_database
        or type(deployment.directory_limits) is not TdsDirectoryLimits
        or (deployment.observe_capacity_release is not None and not callable(deployment.observe_capacity_release))
    ):
        raise ValueError(_ERROR)
    operator = SqlClientNativeRetirementOperator(
        open_management_session=deployment.open_management_session,
        admission=deployment.admission,
        expected_server=deployment.expected_server,
        expected_database=deployment.expected_database,
        attempts=deployment.lifecycle_observer,
        directories=deployment.directory_observer,
        directory_limits=deployment.directory_limits,
        progress=state,
        observe_capacity_release=deployment.observe_capacity_release,
        monotonic=deployment.monotonic,
        operation_timeout=deployment.operation_timeout,
    )
    if type(operator) is not SqlClientNativeRetirementOperator:
        raise ValueError(_ERROR)
    return CallbackSqlClientNativeRetirementEffects(
        prove_containment=operator.prove_containment,
        observe_incarnation=operator.observe_exact_incarnation,
        drop=operator.drop_exact,
        reconcile=operator.reconcile_drop,
        settle=operator.settle_drop,
        observe_absence=operator.observe_absence,
        observe_capacity=operator.observe_capacity,
    )


__all__ = ("SqlClientNativeRetirementDeployment", "compose_sqlclient_native_retirement_effects")
