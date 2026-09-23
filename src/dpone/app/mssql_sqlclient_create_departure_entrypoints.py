"""Versioned public entrypoints for trusted CREATE-to-departure composition."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from time import monotonic

from dpone.adapters.mssql_sqlclient_departure_launch import PythonSqlClientDepartureLauncher
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_coordinator_process import PythonTdsCoordinatorLauncher
from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureOutcome
from dpone.app.mssql_tds_attempt_composition import StoreFactory
from dpone.contracts.mssql_tds_operation_models import (
    SqlClientDatabasePrincipal,
    SqlClientObserverAdmission,
    TdsConnectionMaterial,
    TdsCoordinatorIdentity,
    TdsCreateRequest,
    WindowLease,
)
from dpone.services.mssql_tds_attempt import TdsAttempt


def run_sqlclient_create_departure(
    attempt: TdsAttempt,
    request: TdsCreateRequest,
    create_identity: TdsCoordinatorIdentity,
    *,
    pool: TdsActorPool,
    store_factory: StoreFactory,
    lease: WindowLease,
    evidence_root: Path,
    create_launcher: PythonTdsCoordinatorLauncher,
    departure_launcher: PythonSqlClientDepartureLauncher,
    creator_admission: SqlClientObserverAdmission,
    creator_principal: SqlClientDatabasePrincipal,
    create_connection_material: Callable[[], TdsConnectionMaterial],
    observer_connection_material: Callable[[], TdsConnectionMaterial],
    operation_deadline: float,
    create_startup_timeout: float,
    helper_startup_timeout: float,
    termination_timeout: float,
    clock: Callable[[], float] = monotonic,
) -> SqlClientCreateDepartureOutcome:
    """Explicit v1 CREATE/departure selection; no Prepared or route authority."""
    from dpone.app.mssql_sqlclient_create_departure_composition import _run_sqlclient_create_departure

    return _run_sqlclient_create_departure(
        attempt,
        request,
        create_identity,
        version=1,
        observer_admission=None,
        pool=pool,
        store_factory=store_factory,
        lease=lease,
        evidence_root=evidence_root,
        create_launcher=create_launcher,
        departure_launcher=departure_launcher,
        creator_admission=creator_admission,
        creator_principal=creator_principal,
        create_connection_material=create_connection_material,
        observer_connection_material=observer_connection_material,
        operation_deadline=operation_deadline,
        create_startup_timeout=create_startup_timeout,
        helper_startup_timeout=helper_startup_timeout,
        termination_timeout=termination_timeout,
        clock=clock,
    )


def run_sqlclient_create_departure_v2(
    attempt: TdsAttempt,
    request: TdsCreateRequest,
    create_identity: TdsCoordinatorIdentity,
    *,
    observer_admission: SqlClientObserverAdmission,
    pool: TdsActorPool,
    store_factory: StoreFactory,
    lease: WindowLease,
    evidence_root: Path,
    create_launcher: PythonTdsCoordinatorLauncher,
    departure_launcher: PythonSqlClientDepartureLauncher,
    creator_admission: SqlClientObserverAdmission,
    creator_principal: SqlClientDatabasePrincipal,
    create_connection_material: Callable[[], TdsConnectionMaterial],
    observer_connection_material: Callable[[], TdsConnectionMaterial],
    operation_deadline: float,
    create_startup_timeout: float,
    helper_startup_timeout: float,
    termination_timeout: float,
    clock: Callable[[], float] = monotonic,
) -> SqlClientCreateDepartureOutcome:
    """Explicit v2 CREATE/departure selection; no Prepared or route authority."""
    from dpone.app.mssql_sqlclient_create_departure_composition import _run_sqlclient_create_departure

    return _run_sqlclient_create_departure(
        attempt,
        request,
        create_identity,
        version=2,
        observer_admission=observer_admission,
        pool=pool,
        store_factory=store_factory,
        lease=lease,
        evidence_root=evidence_root,
        create_launcher=create_launcher,
        departure_launcher=departure_launcher,
        creator_admission=creator_admission,
        creator_principal=creator_principal,
        create_connection_material=create_connection_material,
        observer_connection_material=observer_connection_material,
        operation_deadline=operation_deadline,
        create_startup_timeout=create_startup_timeout,
        helper_startup_timeout=helper_startup_timeout,
        termination_timeout=termination_timeout,
        clock=clock,
    )
