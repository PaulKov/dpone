"""Close one production SqlClient invocation over exact runtime authorities."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING, Any, Protocol

from dpone.adapters.mssql_native_route_capabilities import NativeChunkJournal
from dpone.app.mssql_sqlclient_native_parent_composition import (
    SqlClientNativeParentDeployment,
    compose_sqlclient_native_parent_capabilities,
)
from dpone.app.mssql_sqlclient_native_runtime_composition import (
    SqlClientNativeImportCapabilities,
    compose_sqlclient_native_runtime_binding,
)
from dpone.contracts.mssql_native_route_capabilities import MssqlTransactionAdmission, NativeChunkPlan, WindowLease
from dpone.ports.mssql_native_route_capabilities import NativeActorCapacity, WindowStore
from dpone.runtime.mssql_native_route_capabilities import (
    LoadResult,
    MssqlNativeStagedLoadService,
    NativeMssqlRuntime,
    NativeRuntimeBindings,
    StagedLoadHandle,
    compose_sqlclient_native_stage_context,
)
from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer

_INVALID = "mssql_native.sqlclient_route_composition_invalid"


class SqlClientNativeSink(Protocol):
    """Narrow sink surface owning strategy publication state."""

    _strategy_map: Mapping[object, object]


if TYPE_CHECKING:
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeStageDeployment:
    """Target-owned capabilities for one exact schema-v4 stage invocation."""

    plan: NativeChunkPlan
    wire: Any
    limits: Any
    work_dir: Path
    sink: SqlClientNativeSink
    target_connector: Any
    database: str
    schema: str
    row_source: Callable[[Any], Any]
    journal_factory: Callable[[], NativeChunkJournal]
    lease: WindowLease
    cancelled: Event
    target_headroom: int
    interval: Any = None
    observer: Any = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeInvocationDeployment:
    """Complete deployment contract for one leased native invocation."""

    stage: SqlClientNativeStageDeployment
    imports: SqlClientNativeImportCapabilities
    parent: SqlClientNativeParentDeployment
    open_import_capabilities: Callable[[], AbstractContextManager[SqlClientNativeImportCapabilities]]
    capacity: NativeActorCapacity
    implementation_sha256: str
    admission: MssqlTransactionAdmission


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeRouteDeployment:
    """Application-owned route hooks; credentials stay inside injected closures."""

    store: WindowStore
    target_id: str
    invocation_factory: Callable[[Any, WindowLease, Event], SqlClientNativeInvocationDeployment]
    source: Callable[[Any, NativeRuntimeBindings], AbstractContextManager[Any]]
    preflight: Callable[[Any], None]
    quality: Callable[[Any, StagedLoadHandle, WindowLease], None]
    evidence: Callable[[Any, LoadResult, Any, WindowLease], None]
    lease_ttl: float = 60.0
    observer: Any = None


def _require_invocation(deployment: SqlClientNativeInvocationDeployment) -> NativeChunkJournal:
    if type(deployment) is not SqlClientNativeInvocationDeployment:
        raise ValueError(_INVALID)
    stage = deployment.stage
    if (
        type(stage) is not SqlClientNativeStageDeployment
        or type(deployment.imports) is not SqlClientNativeImportCapabilities
        or type(deployment.parent) is not SqlClientNativeParentDeployment
        or type(deployment.capacity) is not NativeActorCapacity
        or not callable(deployment.open_import_capabilities)
        or not callable(stage.row_source)
        or not callable(stage.journal_factory)
        or type(deployment.admission) is not MssqlTransactionAdmission
        or not isinstance(getattr(stage.sink, "_strategy_map", None), dict)
        or any(
            not callable(getattr(stage.target_connector, name, None))
            for name in ("get_records", "get_records_iterator", "execute_query", "open_session")
        )
        or type(stage.cancelled) is not Event
        or stage.cancelled.is_set()
        or type(stage.plan) is not NativeChunkPlan
        or type(stage.lease) is not WindowLease
        or not isinstance(stage.work_dir, Path)
        or stage.wire is None
        or type(getattr(stage.limits, "effective_import_parallelism", None)) is not int
        or type(stage.database) is not str
        or not stage.database
        or type(stage.schema) is not str
        or not stage.schema
        or type(stage.target_headroom) is not int
        or stage.target_headroom < 0
    ):
        raise ValueError(_INVALID)
    journal = stage.journal_factory()
    parent_journal = deployment.parent.journal
    transport = stage.plan.transport
    if (
        type(journal) is not NativeChunkJournal
        or journal is not parent_journal
        or journal.parent_schema_version != 4
        or journal.plan != stage.plan
        or journal.lease != stage.lease
        or stage.plan.target_id != stage.lease.target_id
        or transport is None
        or transport.backend != "mssql_sqlclient"
        or deployment.imports.custody.storage is not deployment.parent.file_custody
        or type(deployment.implementation_sha256) is not str
        or len(deployment.implementation_sha256) != 64
        or any(char not in "0123456789abcdef" for char in deployment.implementation_sha256)
    ):
        raise ValueError(_INVALID)
    deployment.capacity.admit(stage.limits.effective_import_parallelism)
    return journal


def compose_sqlclient_native_bindings(
    deployment: SqlClientNativeInvocationDeployment,
) -> NativeRuntimeBindings:
    """Compose exact parent, importer and typed stage bindings before source I/O."""
    journal = _require_invocation(deployment)
    stage = deployment.stage
    parent = compose_sqlclient_native_parent_capabilities(deployment.parent)
    binding = compose_sqlclient_native_runtime_binding(
        imports=deployment.imports,
        parent=parent,
        capacity=deployment.capacity,
        implementation_sha256=deployment.implementation_sha256,
        open_import_capabilities=deployment.open_import_capabilities,
    )

    def context(row_source: Callable[[], Any]):
        return compose_sqlclient_native_stage_context(
            sqlclient_binding=binding,
            store=deployment.parent.journal.store,
            plan=stage.plan,
            lease=stage.lease,
            wire_contract=stage.wire,
            limits=stage.limits,
            work_dir=stage.work_dir,
            target_connector=stage.target_connector,
            importer_connection=lambda: _bcp_forbidden(),
            bcp_options_factory=_bcp_forbidden,
            database=stage.database,
            schema=stage.schema,
            row_source=row_source,
            journal_factory=lambda: journal,
            cancelled=stage.cancelled,
            required_target_headroom_bytes=stage.target_headroom,
            interval=stage.interval,
            observer=stage.observer,
        )

    recovery = context(lambda: _recovery_source_forbidden())

    def payload_context_factory(_config: Any, payload: Any, _resolved: Any):
        return context(lambda: stage.row_source(payload))

    preparer = MssqlNativeStagePreparer(stage.sink, payload_context_factory)
    service = MssqlNativeStagedLoadService(stage.sink, preparer)
    return NativeRuntimeBindings(service, recovery, deployment.admission)


def _bcp_forbidden(*_args: Any, **_kwargs: Any) -> Any:
    raise RuntimeError("mssql_native.sqlclient_bcp_fallback_forbidden")


def _recovery_source_forbidden() -> Any:
    raise RuntimeError("mssql_native.recovery_source_forbidden")


def compose_sqlclient_native_runtime(deployment: SqlClientNativeRouteDeployment) -> NativeMssqlRuntime:
    """Build the route runtime while keeping v4 checkpointing in parent settlement."""

    if (
        type(deployment) is not SqlClientNativeRouteDeployment
        or any(
            not callable(getattr(deployment.store, name, None))
            for name in ("acquire", "assert_lease", "renew", "release", "load", "save")
        )
        or type(deployment.target_id) is not str
        or not deployment.target_id
        or not callable(deployment.invocation_factory)
        or any(
            not callable(value)
            for value in (
                deployment.source,
                deployment.preflight,
                deployment.quality,
                deployment.evidence,
            )
        )
    ):
        raise ValueError(_INVALID)

    def bindings(config: Any, owner: str, lease: WindowLease, cancelled: Event) -> NativeRuntimeBindings:
        invocation = deployment.invocation_factory(config, lease, cancelled)
        _require_invocation(invocation)
        stage = invocation.stage
        if (
            stage.lease != lease
            or stage.cancelled is not cancelled
            or invocation.parent.journal.store is not deployment.store
            or stage.plan.target_id != deployment.target_id
        ):
            raise ValueError(_INVALID)
        return compose_sqlclient_native_bindings(invocation)

    def unreachable_checkpoint(_config: Any, _result: LoadResult, _lease: WindowLease) -> None:
        raise RuntimeError("mssql_native.sqlclient_checkpoint_bypassed_parent_settlement")

    return NativeMssqlRuntime(
        store=deployment.store,
        target_id=deployment.target_id,
        bindings=bindings,
        source=deployment.source,
        preflight=deployment.preflight,
        quality=deployment.quality,
        evidence=deployment.evidence,
        advance_state=unreachable_checkpoint,
        lease_ttl=deployment.lease_ttl,
        observer=deployment.observer,
    )


__all__ = (
    "SqlClientNativeInvocationDeployment",
    "SqlClientNativeRouteDeployment",
    "SqlClientNativeSink",
    "SqlClientNativeStageDeployment",
    "compose_sqlclient_native_bindings",
    "compose_sqlclient_native_runtime",
)
