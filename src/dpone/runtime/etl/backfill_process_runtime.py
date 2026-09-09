"""Spawn-safe composition for process-isolated MSSQL backfill lanes."""

from __future__ import annotations

from contextlib import contextmanager, suppress
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dpone.backfill.process_chunk_execution import BackfillProcessChunkExecution
from dpone.backfill.process_lane_contracts import (
    BackfillProcessLaneBootstrap,
    ProcessLaneDispatch,
)
from dpone.backfill.worker_runtime import BackfillProcessLaneRuntime
from dpone.runtime.etl.backfill_portable_scope_history import (
    build_mssql_backfill_portable_scope_history_proof,
)
from dpone.runtime.etl.mssql_process_lane_authority import (
    _ParentMssqlLaneAuthority as _ParentDispatchAuthority,
)
from dpone.runtime.etl.mssql_process_lane_authority import (
    lane_route_coordinates,
)
from dpone.runtime.etl.mssql_process_lane_control import MssqlProcessLaneControlScope
from dpone.runtime.etl.mssql_process_lane_receipt_authority import (
    ParentMssqlLaneReceiptAuthority as _ParentMssqlLaneAuthority,
)
from dpone.runtime.etl.mssql_process_lane_receipt_authority import (
    identity_config as _identity_config,  # noqa: F401 - compatibility import for runtime tests.
)
from dpone.runtime.etl.portable_scope_preflight import PortableScopeColumnResolver
from dpone.runtime.state.mssql_fresh_session import MssqlFreshSessionFactory

_ENTRYPOINT = "dpone.runtime.etl.backfill_process_runtime:open_backfill_process_lane"


@dataclass(frozen=True, slots=True)
class BackfillProcessLanePayload:
    """Raw authored inputs that a spawned interpreter may safely rehydrate."""

    raw_config: Any
    run_context: Any
    dag_id: str | None
    execution_date: datetime | None


def build_backfill_process_lane_runtime(
    process_config: Any,
    *,
    run_context: Any,
    dag_id: str | None,
    execution_date: datetime | None,
) -> BackfillProcessLaneRuntime:
    """Compose parent authority and a child bootstrap without live connectors."""

    if not process_config.raw_config:
        raise RuntimeError("mssql_transaction.parallel_backfill_runtime_config_required")
    state_storage = getattr(getattr(process_config, "sink_obj", None), "state_storage", None)
    fresh_sessions = MssqlFreshSessionFactory()
    authority = _ParentMssqlLaneAuthority(
        state_storage,
        source=process_config.source_obj,
        sink=process_config.sink_obj,
        run_context=run_context,
        dag_id=dag_id,
        fresh_session_factory=fresh_sessions,
    )
    dispatch_authority = _ParentDispatchAuthority(
        state_storage,
        source=process_config.source_obj,
        sink=process_config.sink_obj,
        run_context=run_context,
        dag_id=dag_id,
        fresh_session_factory=fresh_sessions,
    )
    payload = BackfillProcessLanePayload(
        raw_config=deepcopy(process_config.raw_config),
        run_context=run_context,
        dag_id=dag_id,
        execution_date=execution_date,
    )

    def prepare_dispatch(dispatch: ProcessLaneDispatch) -> ProcessLaneDispatch:
        prepared = dispatch_authority.prepare_dispatch(dispatch)
        return authority.prepare_dispatch(prepared)

    return BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap(entrypoint=_ENTRYPOINT, payload=payload),
        renew_operation_lease=authority.renew_operation_lease,
        validate_operation_binding=authority.validate_operation_binding,
        issue_receipt_probe=authority.issue_receipt_probe,
        validate_replay_result=authority.validate_replay_result,
        receipt_recovery=authority.receipt_recovery,
        prepare_dispatch=prepare_dispatch,
        portable_scope_column_resolver=PortableScopeColumnResolver(
            source=process_config.source_obj,
            sink=process_config.sink_obj,
        ),
        portable_scope_history_proof=build_mssql_backfill_portable_scope_history_proof(
            source=process_config.source_obj,
            sink=process_config.sink_obj,
            run_context=run_context,
            dag_id=dag_id,
            state=authority,
        ),
        process_chunk_execution_factory=BackfillProcessChunkExecution,
        parent_control_scope=MssqlProcessLaneControlScope(
            _parent_control_connectors(process_config, state_storage),
            database_authorities=(state_storage,),
            fresh_session_factories=(fresh_sessions,),
        ),
    )


def _parent_control_connectors(process_config: Any, state_storage: Any) -> tuple[Any, ...]:
    sink_connector = getattr(getattr(process_config, "sink_obj", None), "connector", None)
    state_connector = getattr(state_storage, "connector", None)
    source_connector = getattr(getattr(process_config, "source_obj", None), "connector", None)
    connectors = [sink_connector, state_connector]
    if str(getattr(source_connector, "dialect", "") or "").strip().lower() == "mssql":
        connectors.append(source_connector)
    return tuple(connector for connector in connectors if connector is not None)


@contextmanager
def open_backfill_process_lane(worker_id: int, payload: Any, operation_lease_factory: Any):
    """Hydrate and reuse one source/sink connection set in one child process."""

    del worker_id
    if not isinstance(payload, BackfillProcessLanePayload):
        raise RuntimeError("mssql_transaction.parallel_backfill_process_payload_invalid")
    from dpone.ports.runtime_hydrator import ensure_runtime_hydrator
    from dpone.runtime.bootstrap_runner import _processor
    from dpone.runtime.etl.mssql_transaction_admission import MssqlTransactionAdmissionService
    from dpone.runtime.governance.audit_tap import RuntimeLoadStepAuditCollector
    from dpone.runtime.governance.service import LoadGovernanceService
    from dpone.runtime.native_transfer import NativeTransferRuntimeService
    from dpone.runtime.route_runtime_factory import RouteCapabilityRuntimeFactory

    bindings = None
    lane_route = None
    admission = MssqlTransactionAdmissionService(
        operation_lease_factory=operation_lease_factory,
        start_operation_lease_on_admission=True,
    )

    def run_chunk(load_config: Any) -> Any:
        nonlocal bindings, lane_route
        route = lane_route_coordinates(load_config)
        if lane_route is not None and route != lane_route:
            raise RuntimeError("mssql_transaction.parallel_backfill_lane_route_rebound")
        if bindings is None:
            bindings = ensure_runtime_hydrator().build(
                config=payload.raw_config,
                load_config=load_config,
            )
            lane_route = route
        route_runtime = RouteCapabilityRuntimeFactory().build(
            load_config=load_config,
            source=bindings.source_obj,
            sink=bindings.sink_obj,
            logger=bindings.etl_logger,
        )
        processor = _processor(
            bindings,
            load_config=load_config,
            native_transfer_runtime_service=NativeTransferRuntimeService(
                checkpoint_store=bindings.partition_checkpoint_store,
            ),
            route_capability_orchestrator=route_runtime,
            load_governance_service=LoadGovernanceService(audit_storage=RuntimeLoadStepAuditCollector()),
            mssql_transaction_admission_service=admission,
        )
        return processor.run(
            load_config,
            payload.run_context,
            dag_id=payload.dag_id,
            execution_date=payload.execution_date,
        )

    try:
        yield run_chunk
    finally:
        if bindings is not None:
            close_runtime_bindings(bindings)


def close_runtime_bindings(bindings: Any) -> None:
    """Close each distinct connector exactly once."""

    connectors = (
        getattr(getattr(bindings, "source_obj", None), "connector", None),
        getattr(getattr(bindings, "sink_obj", None), "connector", None),
        getattr(getattr(getattr(bindings, "sink_obj", None), "state_storage", None), "connector", None),
    )
    closed: set[int] = set()
    for connector in connectors:
        if connector is None or id(connector) in closed:
            continue
        closed.add(id(connector))
        closer = getattr(connector, "close", None)
        if callable(closer):
            with suppress(Exception):
                closer()


__all__ = [
    "BackfillProcessLanePayload",
    "build_backfill_process_lane_runtime",
    "close_runtime_bindings",
    "lane_route_coordinates",
    "open_backfill_process_lane",
]
