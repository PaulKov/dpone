"""Assemble the explicit SqlClient route from frozen deployment authorities."""

from __future__ import annotations

import math
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from pathlib import Path
from threading import Event
from uuid import NAMESPACE_URL, uuid5

from dpone.adapters.mssql_native_route_capabilities import (
    FileSqlClientInputCustody,
    NativeChunkJournal,
    SqlClientCheckpointCas,
)
from dpone.app.mssql_sqlclient_fresh_chunk_execution_composition import SqlClientFreshChunkDeployment
from dpone.app.mssql_sqlclient_native_invocation_producer import (
    SqlClientFailedAttemptAuthority,
    SqlClientNativeInvocationProducer,
    SqlClientNativeProductionDeployment,
    SqlClientTargetConnector,
)
from dpone.app.mssql_sqlclient_native_observer_composition import SqlClientNativeObserverBundle
from dpone.app.mssql_sqlclient_native_parent_bridges import DurableInputCustody
from dpone.app.mssql_sqlclient_native_parent_composition import SqlClientNativeParentDeployment
from dpone.app.mssql_sqlclient_native_retirement_composition import SqlClientNativeRetirementDeployment
from dpone.app.mssql_sqlclient_native_route_composition import (
    SqlClientNativeInvocationDeployment,
    SqlClientNativeRouteDeployment,
    SqlClientNativeSink,
    compose_sqlclient_native_runtime,
)
from dpone.app.mssql_sqlclient_prepared_attempt_context import (
    SqlClientPreparedAttemptAuthority,
)
from dpone.app.mssql_sqlclient_stage_locator_composition import (
    _AdmittedSqlClientStoreFactory,
    admit_sqlclient_state_domain,
)
from dpone.app.mssql_tds_attempt_composition import observe_tds_attempt, recover_tds_attempt
from dpone.contracts.mssql_native_chunks import NativeChunkLimits, NativeChunkPlan
from dpone.contracts.mssql_native_route_capabilities import MssqlTransactionAdmission, WindowLease
from dpone.manifest.mssql_native_policy import validate_native_config
from dpone.ports.mssql_native_route_capabilities import (
    ExactEvidenceReaderV1,
    NativeActorCapacity,
    WindowStore,
)
from dpone.runtime.mssql_native_route_capabilities import (
    LoadResult,
    NativeMssqlRuntime,
    NativeRuntimeBindings,
    StagedLoadHandle,
)
from dpone.runtime.native_wire_models import SourceNativeWireContract
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody

_ERROR = "mssql_native.sqlclient_deployment_composition_invalid"


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeSourceAuthority:
    """Pure source identities plus the post-recovery payload boundary."""

    plan_factory: Callable[[object], NativeChunkPlan]
    wire_factory: Callable[[object], SourceNativeWireContract]
    limits: NativeChunkLimits
    work_dir: Path
    open_payload: Callable[[object, NativeRuntimeBindings], AbstractContextManager[object]]
    rows: Callable[[object], object]
    interval: object | None = None
    observer: object | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeSqlAuthority:
    """Target, transaction, parent settlement and retained-input authorities."""

    sink: SqlClientNativeSink
    target_connector: SqlClientTargetConnector
    database: str
    schema: str
    target_headroom: int
    file_custody: FileSqlClientInputCustody
    prepared: SqlClientPreparedAttemptAuthority
    retirement: SqlClientNativeRetirementDeployment
    admission: Callable[[NativeChunkPlan, WindowLease], MssqlTransactionAdmission]
    rollback_no_commit: Callable[[], dict[str, object]]
    evidence_reader: ExactEvidenceReaderV1
    input_custody: DurableInputCustody
    checkpoint: SqlClientCheckpointCas
    inspect_projection: Callable[..., object]
    failed_attempt: SqlClientFailedAttemptAuthority
    allocated_bytes: Callable[[], int]


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeWorkerAuthority:
    """Exact admitted worker deployment and its finite capacity."""

    fresh: SqlClientFreshChunkDeployment
    capacity: NativeActorCapacity
    implementation_sha256: str
    state_domain_timeout: float


class SqlClientNativeConfigValidator:
    """Nominal pure validator for the authored native-route policy."""

    __slots__ = ()

    def validate(self, config: object) -> None:
        validate_native_config(config)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeDeployment:
    """Frozen route authorities; composition itself performs no effects."""

    store_factory: Callable[[], AbstractContextManager[WindowStore]]
    store: WindowStore
    target_id: str
    source: SqlClientNativeSourceAuthority
    sql: SqlClientNativeSqlAuthority
    worker: SqlClientNativeWorkerAuthority
    config_validator: SqlClientNativeConfigValidator
    quality: Callable[[object, StagedLoadHandle, WindowLease], None]
    evidence: Callable[[object, LoadResult, object, WindowLease], None]
    lease_ttl: float = 60.0


def _parent_factory(
    sql: SqlClientNativeSqlAuthority,
) -> Callable[
    [
        NativeChunkJournal,
        FileSqlClientInputCustody,
        SqlClientNativeObserverBundle,
        SqlClientAttemptRetirementCustody,
        _AdmittedSqlClientStoreFactory,
    ],
    SqlClientNativeParentDeployment,
]:
    def factory(
        journal,
        custody,
        observers,
        attempt_retirement_custody: SqlClientAttemptRetirementCustody,
        admitted_factory: _AdmittedSqlClientStoreFactory,
    ):
        retirement = replace(
            sql.retirement,
            lifecycle_observer=observers.lifecycle,
            directory_observer=observers.directory,
            directory_limits=sql.prepared.directory_limits,
        )

        def recover_verified_retirement(request, deadline: float) -> None:
            observations = observe_tds_attempt(
                sql.prepared.pool,
                admitted_factory,
                request.projection.attempt,
                sql.prepared.directory_limits,
                deadline=deadline,
            )
            if observations.parent is None or observations.directory is None:
                raise RuntimeError("mssql_native.sqlclient_retirement_recovery_missing")
            token = str(
                uuid5(
                    NAMESPACE_URL,
                    f"dpone:{journal.lease.target_id}:{journal.lease.fence}:{request.projection.projection_sha256}",
                )
            )
            recovered = recover_tds_attempt(
                sql.prepared.pool,
                admitted_factory,
                observations.parent,
                observations.directory,
                sql.prepared.directory_limits,
                journal.lease,
                supervisor_token=token,
                deadline=deadline,
            )
            attempt_retirement_custody.retain(recovered, deadline=deadline)

        return SqlClientNativeParentDeployment(
            journal=journal,
            lifecycle_observer=observers.lifecycle,
            directory_observer=observers.directory,
            rollback_no_commit=sql.rollback_no_commit,
            evidence_reader=sql.evidence_reader,
            retirement=retirement,
            file_custody=custody,
            input_custody=sql.input_custody,
            checkpoint=sql.checkpoint,
            directory_limits=sql.prepared.directory_limits,
            attempt_retirement_custody=attempt_retirement_custody,
            recover_verified_retirement=recover_verified_retirement,
        )

    return factory


def _invocation_factory(
    deployment: SqlClientNativeDeployment,
) -> Callable[[object, WindowLease, Event], SqlClientNativeInvocationDeployment]:
    source, sql, worker = deployment.source, deployment.sql, deployment.worker

    def factory(config: object, lease: WindowLease, cancelled: Event) -> SqlClientNativeInvocationDeployment:
        if type(lease) is not WindowLease or lease.target_id != deployment.target_id or cancelled.is_set():
            raise ValueError(_ERROR)
        plan, wire = source.plan_factory(config), source.wire_factory(config)
        if type(plan) is not NativeChunkPlan or plan.target_id != deployment.target_id:
            raise ValueError(_ERROR)
        if type(wire) is not SourceNativeWireContract:
            raise ValueError(_ERROR)
        admitted = admit_sqlclient_state_domain(
            store_factory=deployment.store_factory,
            lease=lease,
            pool=sql.prepared.pool,
            deadline=sql.prepared.pool.deadline_after(worker.state_domain_timeout),
        )
        prepared = sql.prepared.bind(admitted)
        production = SqlClientNativeProductionDeployment(
            store=deployment.store,
            plan=plan,
            wire=wire,
            limits=source.limits,
            work_dir=source.work_dir,
            sink=sql.sink,
            target_connector=sql.target_connector,
            database=sql.database,
            schema=sql.schema,
            row_source=source.rows,
            target_headroom=sql.target_headroom,
            capacity=worker.capacity,
            implementation_sha256=worker.implementation_sha256,
            file_custody=sql.file_custody,
            admitted_factory=admitted,
            prepared_deployment=prepared,
            fresh_chunk=worker.fresh,
            retirement=sql.retirement,
            inspect_projection=sql.inspect_projection,
            failed_attempt=sql.failed_attempt,
            allocated_bytes=sql.allocated_bytes,
            parent_factory=_parent_factory(sql),
            admission_factory=sql.admission,
            interval=source.interval,
            observer=source.observer,
        )
        return SqlClientNativeInvocationProducer(production)(config, lease, cancelled)

    return factory


def compose_sqlclient_native_deployment(deployment: SqlClientNativeDeployment) -> SqlClientNativeRouteDeployment:
    """Return a pure route description; lease-bound effects remain deferred."""
    if (
        type(deployment) is not SqlClientNativeDeployment
        or type(deployment.source) is not SqlClientNativeSourceAuthority
        or type(deployment.sql) is not SqlClientNativeSqlAuthority
        or type(deployment.worker) is not SqlClientNativeWorkerAuthority
        or type(deployment.target_id) is not str
        or not deployment.target_id
        or type(deployment.lease_ttl) not in (int, float)
        or not math.isfinite(deployment.lease_ttl)
        or deployment.lease_ttl <= 0
        or type(deployment.source.limits) is not NativeChunkLimits
        or not isinstance(deployment.source.work_dir, Path)
        or type(deployment.sql.file_custody) is not FileSqlClientInputCustody
        or type(deployment.sql.prepared) is not SqlClientPreparedAttemptAuthority
        or type(deployment.sql.retirement) is not SqlClientNativeRetirementDeployment
        or type(deployment.sql.failed_attempt) is not SqlClientFailedAttemptAuthority
        or type(deployment.worker.fresh) is not SqlClientFreshChunkDeployment
        or type(deployment.worker.capacity) is not NativeActorCapacity
        or type(deployment.worker.state_domain_timeout) not in (int, float)
        or not math.isfinite(deployment.worker.state_domain_timeout)
        or deployment.worker.state_domain_timeout <= 0
        or type(deployment.config_validator) is not SqlClientNativeConfigValidator
        or len(deployment.worker.implementation_sha256) != 64
        or any(char not in "0123456789abcdef" for char in deployment.worker.implementation_sha256)
        or type(deployment.sql.database) is not str
        or not deployment.sql.database
        or type(deployment.sql.schema) is not str
        or not deployment.sql.schema
        or type(deployment.sql.target_headroom) is not int
        or deployment.sql.target_headroom < 0
        or deployment.sql.prepared.input_custody is not deployment.sql.file_custody
        or deployment.sql.prepared.pool is not deployment.worker.fresh.pool
        or any(
            not callable(getattr(deployment.store, name, None))
            for name in ("acquire", "assert_lease", "renew", "release", "load", "save")
        )
        or not isinstance(getattr(deployment.sql.sink, "_strategy_map", None), dict)
        or any(
            not callable(getattr(deployment.sql.target_connector, name, None))
            for name in ("get_records", "get_records_iterator", "execute_query", "open_session")
        )
        or not callable(deployment.store_factory)
        or any(
            not callable(value)
            for value in (
                deployment.source.plan_factory,
                deployment.source.wire_factory,
                deployment.source.open_payload,
                deployment.source.rows,
                deployment.sql.admission,
                deployment.sql.rollback_no_commit,
                deployment.sql.inspect_projection,
                deployment.sql.failed_attempt.resolve_attempt,
                deployment.sql.failed_attempt.observe_stage,
                deployment.sql.failed_attempt.observe_input_custody,
                deployment.sql.allocated_bytes,
                deployment.config_validator.validate,
                deployment.quality,
                deployment.evidence,
            )
        )
    ):
        raise ValueError(_ERROR)
    deployment.worker.capacity.admit(deployment.source.limits.effective_import_parallelism)
    return SqlClientNativeRouteDeployment(
        store=deployment.store,
        target_id=deployment.target_id,
        invocation_factory=_invocation_factory(deployment),
        source=deployment.source.open_payload,
        preflight=deployment.config_validator.validate,
        quality=deployment.quality,
        evidence=deployment.evidence,
        lease_ttl=float(deployment.lease_ttl),
        observer=deployment.source.observer,
    )


def compose_sqlclient_native_application(deployment: SqlClientNativeDeployment) -> NativeMssqlRuntime:
    """Compose the production SqlClient runtime from frozen deployment authorities."""
    return compose_sqlclient_native_runtime(compose_sqlclient_native_deployment(deployment))


__all__ = (
    "SqlClientNativeConfigValidator",
    "compose_sqlclient_native_application",
    "SqlClientNativeDeployment",
    "SqlClientPreparedAttemptAuthority",
    "SqlClientNativeSourceAuthority",
    "SqlClientNativeSqlAuthority",
    "SqlClientNativeWorkerAuthority",
    "compose_sqlclient_native_deployment",
)
