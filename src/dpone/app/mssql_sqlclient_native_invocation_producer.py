"""Produce one admitted SqlClient invocation from frozen deployment facts.

The producer deliberately separates pure identity description from opening the
durable journal and deployment factories.  Callers can therefore perform route
admission without touching credentials, files, processes, or SQL.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Protocol

from dpone.adapters.mssql_native_route_capabilities import FileSqlClientInputCustody, NativeChunkJournal
from dpone.app.mssql_sqlclient_failed_attempt_composition import (
    SqlClientFailedAttemptDeployment,
    compose_sqlclient_failed_attempt_settlement,
)
from dpone.app.mssql_sqlclient_failed_retirement_composition import (
    SqlClientFailedRetirementDeployment,
    compose_sqlclient_failed_retirement,
)
from dpone.app.mssql_sqlclient_fresh_chunk_execution_composition import (
    SqlClientFreshChunkDeployment,
    compose_sqlclient_fresh_chunk_execution,
)
from dpone.app.mssql_sqlclient_native_observer_composition import (
    SqlClientNativeObserverBundle,
    compose_sqlclient_native_observers,
)
from dpone.app.mssql_sqlclient_native_parent_composition import SqlClientNativeParentDeployment
from dpone.app.mssql_sqlclient_native_retirement_composition import SqlClientNativeRetirementDeployment
from dpone.app.mssql_sqlclient_native_route_composition import (
    SqlClientNativeInvocationDeployment,
    SqlClientNativeSink,
    SqlClientNativeStageDeployment,
)
from dpone.app.mssql_sqlclient_native_runtime_composition import (
    compose_sqlclient_native_import_capabilities,
)
from dpone.app.mssql_sqlclient_prepared_attempt_context import (
    SqlClientPreparedAttemptDeployment,
    compose_sqlclient_prepared_attempt_factory,
)
from dpone.app.mssql_sqlclient_prepared_attempt_factory import SqlClientPreparedAttemptFactory
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory
from dpone.app.mssql_tds_attempt_composition import resume_tds_attempt
from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.contracts.mssql_native_route_capabilities import MssqlTransactionAdmission, NativeChunkPlan, WindowLease
from dpone.contracts.mssql_sqlclient_stage_identity import SqlClientStageIdentity
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity
from dpone.manifest.mssql_native_policy import native_source_read_mode, native_transport_policy
from dpone.ports.mssql_native_route_capabilities import NativeActorCapacity, WindowStore
from dpone.runtime.native_wire_models import SourceNativeWireContract
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody

_INVALID = "mssql_native.sqlclient_invocation_producer_invalid"

ImportEffect = Callable[..., object]


class SqlClientTargetConnector(Protocol):
    """Narrow structural surface consumed by native stage composition."""

    get_records: Callable[..., object]
    get_records_iterator: Callable[..., object]
    execute_query: Callable[..., object]
    open_session: Callable[..., object]


ParentFactory = Callable[
    [
        NativeChunkJournal,
        FileSqlClientInputCustody,
        SqlClientNativeObserverBundle,
        SqlClientAttemptRetirementCustody,
        _AdmittedSqlClientStoreFactory,
    ],
    SqlClientNativeParentDeployment,
]
AdmissionFactory = Callable[[NativeChunkPlan, WindowLease], MssqlTransactionAdmission]


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientFailedAttemptAuthority:
    """Source-of-truth resolvers required to bind one failed attempt."""

    resolve_attempt: Callable[[NativeChunkPlan, str], TdsAttemptIdentity]
    observe_stage: Callable[[TdsAttemptIdentity], SqlClientStageIdentity]
    observe_input_custody: Callable[[NativeChunkPlan, str], str]


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeProductionDeployment:
    """Frozen production facts and named post-admission factories."""

    store: WindowStore
    plan: NativeChunkPlan
    wire: SourceNativeWireContract
    limits: NativeChunkLimits
    work_dir: Path
    sink: SqlClientNativeSink
    target_connector: SqlClientTargetConnector
    database: str
    schema: str
    row_source: Callable[[object], object]
    target_headroom: int
    capacity: NativeActorCapacity
    implementation_sha256: str
    file_custody: FileSqlClientInputCustody
    admitted_factory: _AdmittedSqlClientStoreFactory
    prepared_deployment: SqlClientPreparedAttemptDeployment
    fresh_chunk: SqlClientFreshChunkDeployment
    retirement: SqlClientNativeRetirementDeployment
    inspect_projection: ImportEffect
    failed_attempt: SqlClientFailedAttemptAuthority
    allocated_bytes: Callable[[], int]
    parent_factory: ParentFactory
    admission_factory: AdmissionFactory
    interval: object | None = None
    observer: object | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeProductionInvocationDescription:
    """Pure, exact invocation identities suitable for route admission."""

    target_id: str
    plan: NativeChunkPlan
    lease: WindowLease
    cancelled: Event
    capacity: NativeActorCapacity
    parallelism: int
    custody: FileSqlClientInputCustody
    deployment: SqlClientNativeProductionDeployment
    producer_identity: object


class SqlClientNativeInvocationProducer:
    """Open one fully bound invocation only after its pure description is admitted."""

    def __init__(self, deployment: SqlClientNativeProductionDeployment) -> None:
        _validate_deployment(deployment)
        self._deployment = deployment
        self._identity = object()

    def describe(self, lease: WindowLease, cancelled: Event) -> SqlClientNativeProductionInvocationDescription:
        """Return identities without invoking any injected factory or capability."""
        deployment = self._deployment
        parallelism = deployment.limits.effective_import_parallelism
        if (
            type(lease) is not WindowLease
            or lease.target_id != deployment.plan.target_id
            or type(cancelled) is not Event
            or cancelled.is_set()
        ):
            raise ValueError(_INVALID)
        deployment.capacity.admit(parallelism)
        return SqlClientNativeProductionInvocationDescription(
            target_id=deployment.plan.target_id,
            plan=deployment.plan,
            lease=lease,
            cancelled=cancelled,
            capacity=deployment.capacity,
            parallelism=parallelism,
            custody=deployment.file_custody,
            deployment=deployment,
            producer_identity=self._identity,
        )

    def __call__(self, config: object, lease: WindowLease, cancelled: Event) -> SqlClientNativeInvocationDeployment:
        """Admit authored policy before opening any invocation effects."""
        if self._deployment.plan.source_read_mode != native_source_read_mode(
            config
        ) or self._deployment.plan.transport != native_transport_policy(config):
            raise ValueError(_INVALID)
        return self.open(self.describe(lease, cancelled))

    def open(self, description: SqlClientNativeProductionInvocationDescription) -> SqlClientNativeInvocationDeployment:
        """Open recovery and effect factories after exact-description validation."""
        deployment = self._deployment
        if (
            type(description) is not SqlClientNativeProductionInvocationDescription
            or description.producer_identity is not self._identity
            or description.deployment is not deployment
            or description.target_id != deployment.plan.target_id
            or description.plan is not deployment.plan
            or type(description.lease) is not WindowLease
            or description.lease.target_id != deployment.plan.target_id
            or type(description.cancelled) is not Event
            or description.cancelled.is_set()
            or description.capacity is not deployment.capacity
            or description.parallelism != deployment.limits.effective_import_parallelism
            or description.custody is not deployment.file_custody
        ):
            raise ValueError(_INVALID)
        deployment.capacity.admit(description.parallelism)

        observers = compose_sqlclient_native_observers(deployment.admitted_factory)
        journal = NativeChunkJournal(
            deployment.store,
            description.lease,
            deployment.plan,
            parent_schema_version=4,
        )
        prepared = compose_sqlclient_prepared_attempt_factory(deployment.prepared_deployment)
        if type(prepared) is not SqlClientPreparedAttemptFactory:
            raise ValueError(_INVALID)
        retirement_custody = SqlClientAttemptRetirementCustody(
            lambda suspension, deadline: resume_tds_attempt(
                deployment.fresh_chunk.pool,
                deployment.admitted_factory,
                suspension,
                description.lease,
                deadline=deadline,
            )
        )
        failed_reservation, failed_terminal = compose_sqlclient_failed_retirement(
            SqlClientFailedRetirementDeployment(
                store=deployment.store,
                lease=description.lease,
                custody=retirement_custody,
                retirement=deployment.retirement,
                directory_limits=deployment.prepared_deployment.directory_limits,
            )
        )
        failed_settlement = compose_sqlclient_failed_attempt_settlement(
            SqlClientFailedAttemptDeployment(
                store=deployment.store,
                lease=description.lease,
                lifecycle_observer=observers.lifecycle,
                directory_observer=observers.directory,
                directory_limits=deployment.prepared_deployment.directory_limits,
                resolve_attempt=deployment.failed_attempt.resolve_attempt,
                observe_stage=deployment.failed_attempt.observe_stage,
                observe_input_custody=deployment.failed_attempt.observe_input_custody,
                reservation=failed_reservation,
                retirement=failed_terminal,
            )
        )
        execution = compose_sqlclient_fresh_chunk_execution(deployment.fresh_chunk, prepared, retirement_custody)
        imports = compose_sqlclient_native_import_capabilities(
            prepared_attempts=prepared,
            input_custody=deployment.file_custody,
            execution=execution,
            inspect_projection=deployment.inspect_projection,
            failed_settlement=failed_settlement,
            allocated_bytes=deployment.allocated_bytes,
        )
        parent = deployment.parent_factory(
            journal,
            deployment.file_custody,
            observers,
            retirement_custody,
            deployment.admitted_factory,
        )
        admission = deployment.admission_factory(deployment.plan, description.lease)
        if (
            type(parent) is not SqlClientNativeParentDeployment
            or parent.journal is not journal
            or parent.file_custody is not deployment.file_custody
            or parent.lifecycle_observer is not observers.lifecycle
            or parent.directory_observer is not observers.directory
            or type(admission) is not MssqlTransactionAdmission
        ):
            raise ValueError(_INVALID)

        stage = SqlClientNativeStageDeployment(
            plan=deployment.plan,
            wire=deployment.wire,
            limits=deployment.limits,
            work_dir=deployment.work_dir,
            sink=deployment.sink,
            target_connector=deployment.target_connector,
            database=deployment.database,
            schema=deployment.schema,
            row_source=deployment.row_source,
            journal_factory=lambda: journal,
            lease=description.lease,
            cancelled=description.cancelled,
            target_headroom=deployment.target_headroom,
            interval=deployment.interval,
            observer=deployment.observer,
        )
        return SqlClientNativeInvocationDeployment(
            stage=stage,
            imports=imports,
            parent=parent,
            open_import_capabilities=lambda: nullcontext(imports),
            capacity=deployment.capacity,
            implementation_sha256=deployment.implementation_sha256,
            admission=admission,
        )


def _validate_deployment(deployment: SqlClientNativeProductionDeployment) -> None:
    if type(deployment) is not SqlClientNativeProductionDeployment:
        raise ValueError(_INVALID)
    transport = deployment.plan.transport if type(deployment.plan) is NativeChunkPlan else None
    digest = deployment.implementation_sha256
    if (
        type(deployment.wire) is not SourceNativeWireContract
        or type(deployment.limits) is not NativeChunkLimits
        or not isinstance(deployment.work_dir, Path)
        or type(deployment.capacity) is not NativeActorCapacity
        or type(deployment.file_custody) is not FileSqlClientInputCustody
        or type(deployment.admitted_factory) is not _AdmittedSqlClientStoreFactory
        or type(deployment.prepared_deployment) is not SqlClientPreparedAttemptDeployment
        or deployment.admitted_factory is not deployment.prepared_deployment.admitted_factory
        or type(deployment.fresh_chunk) is not SqlClientFreshChunkDeployment
        or type(deployment.retirement) is not SqlClientNativeRetirementDeployment
        or type(deployment.failed_attempt) is not SqlClientFailedAttemptAuthority
        or transport is None
        or transport.backend != "mssql_sqlclient"
        or type(deployment.database) is not str
        or not deployment.database
        or type(deployment.schema) is not str
        or not deployment.schema
        or type(deployment.target_headroom) is not int
        or deployment.target_headroom < 0
        or type(digest) is not str
        or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
        or any(
            not callable(value)
            for value in (
                deployment.row_source,
                deployment.inspect_projection,
                deployment.failed_attempt.resolve_attempt,
                deployment.failed_attempt.observe_stage,
                deployment.failed_attempt.observe_input_custody,
                deployment.allocated_bytes,
                deployment.parent_factory,
                deployment.admission_factory,
            )
        )
        or not isinstance(getattr(deployment.sink, "_strategy_map", None), dict)
        or any(
            not callable(getattr(deployment.target_connector, name, None))
            for name in ("get_records", "get_records_iterator", "execute_query", "open_session")
        )
    ):
        raise ValueError(_INVALID)
    deployment.capacity.admit(deployment.limits.effective_import_parallelism)


__all__ = (
    "SqlClientNativeInvocationProducer",
    "SqlClientNativeProductionDeployment",
    "SqlClientNativeProductionInvocationDescription",
)
