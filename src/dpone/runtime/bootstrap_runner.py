"""Default process runner implementation for runtime bootstrap."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from uuid import uuid4

from dpone.contracts.process_errors import ETLProcessError
from dpone.contracts.process_types import ProcessResult
from dpone.contracts.run_context import RunContext
from dpone.runtime.governance.audit_tap import RuntimeLoadStepAuditCollector
from dpone.security_redaction import redact_public_value

_RUNTIME_DETAIL_KEYS = (
    "backfill",
    "run_throughput",
    "route_capabilities",
    "runtime_decisions",
    "reconciliation_metrics",
    "validation_info",
)


class DefaultProcessRunner:
    """Execute an ETLProcess via runtime ETLProcessor."""

    def __init__(
        self,
        *,
        route_capability_factory: Any | None = None,
        backfill_orchestrator_factory: Any | None = None,
        backfill_state_store_factory: Any | None = None,
        window_runtime_factory: Any | None = None,
    ) -> None:
        self._route_capability_factory = route_capability_factory
        self._backfill_orchestrator_factory = backfill_orchestrator_factory
        self._backfill_state_store_factory = backfill_state_store_factory
        self._window_runtime_factory = window_runtime_factory

    def run(
        self,
        process,
        *,
        context: RunContext | None = None,
        dag_id: str | None = None,
        execution_date: Any | None = None,
    ) -> ProcessResult:
        from dpone.backfill.state_factory import BackfillStateStoreFactory
        from dpone.runtime.etl.backfill_orchestrator import execute_process_with_backfill
        from dpone.runtime.governance.service import LoadGovernanceService
        from dpone.runtime.native_transfer import NativeTransferRuntimeService
        from dpone.runtime.route_runtime_factory import RouteCapabilityRuntimeFactory

        run_context = _runtime_context(context, process.config.name)
        if (getattr(process.config.load_config, "options", None) or {}).get("rolling_window") is not None:
            if self._window_runtime_factory is None:
                raise ETLProcessError(
                    "rolling_window_capability_required: configure a snapshot source and an exclusive-writer "
                    "target through DefaultProcessRunner(window_runtime_factory=...)"
                )
            from dpone.runtime.rolling_window_admission import validate_window_admission

            validate_window_admission(process.config.load_config)
            result = self._window_runtime_factory(process.config).run(
                process.config.load_config, owner=run_context.run_id
            )
            process.current_state = result
            if result.status != "success":
                raise ETLProcessError(f"ETL process {process.config.name} failed: {result.errors}")
            return result
        from dpone.runtime.rolling_window_admission import reject_orphan_window_chunking

        reject_orphan_window_chunking(process.config.load_config)
        process.config.ensure_runtime_bindings()
        native_transfer_runtime_service = NativeTransferRuntimeService(
            checkpoint_store=process.config.partition_checkpoint_store,
        )
        route_capability_factory = self._route_capability_factory or RouteCapabilityRuntimeFactory()
        route_capability_orchestrator = route_capability_factory.build(
            load_config=process.config.load_config,
            source=process.config.source_obj,
            sink=process.config.sink_obj,
            logger=process.config.etl_logger,
        )
        load_step_collector = RuntimeLoadStepAuditCollector()
        load_governance_service = LoadGovernanceService(audit_storage=load_step_collector)
        backfill_state_store_factory = self._backfill_state_store_factory or BackfillStateStoreFactory()
        processor = _processor(
            process.config,
            load_config=process.config.load_config,
            native_transfer_runtime_service=native_transfer_runtime_service,
            route_capability_orchestrator=route_capability_orchestrator,
            load_governance_service=load_governance_service,
        )
        orchestration_options = {}
        if self._backfill_orchestrator_factory is not None:
            orchestration_options["orchestrator_factory"] = self._backfill_orchestrator_factory
        xmin_handoff_state_storage = getattr(process.config, "xmin_handoff_state_storage", None)
        if xmin_handoff_state_storage is not None:
            orchestration_options["xmin_handoff_state_storage"] = xmin_handoff_state_storage
        result_dict = execute_process_with_backfill(
            processor,
            process.config.load_config,
            run_context,
            dag_id=dag_id,
            execution_date=execution_date,
            worker_chunk_runner_factory=_worker_chunk_runner_factory(
                process.config,
                route_capability_factory=route_capability_factory,
                run_context=run_context,
                dag_id=dag_id,
                execution_date=execution_date,
            ),
            worker_state_store_factory=_worker_state_store_factory(
                process.config,
                state_store_factory=backfill_state_store_factory,
            ),
            **orchestration_options,
        )
        details = _runtime_result_details(
            result_dict,
            load_step_collector,
            credential_resolution_receipts=(getattr(process.config, "credential_resolution_receipts", ())),
        )
        result = ProcessResult(
            status=result_dict.get("status", "unknown"),
            inserted_rows=result_dict.get("inserted_rows", 0),
            updated_rows=result_dict.get("updated_rows", 0),
            final_rows=result_dict.get("final_rows", 0),
            extracted_rows=result_dict.get("extracted_rows", 0),
            duration_seconds=result_dict.get("duration_seconds", 0.0),
            errors=result_dict.get("errors", []),
            details=details,
        )
        process.current_state = result
        if result.status != "success":
            raise ETLProcessError(f"ETL process {process.config.name} failed: {result.errors}")
        return result


def _runtime_result_details(
    result: dict[str, Any],
    load_step_collector: RuntimeLoadStepAuditCollector,
    *,
    credential_resolution_receipts: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any] | None:
    details = {key: result[key] for key in _RUNTIME_DETAIL_KEYS if _has_value(result.get(key))}
    load_steps = load_step_collector.to_jsonable()
    if load_steps:
        details["load_steps"] = load_steps
    if credential_resolution_receipts:
        details["credential_resolution"] = redact_public_value(
            [dict(receipt) for receipt in credential_resolution_receipts]
        )
    return details or None


def _has_value(value: Any) -> bool:
    return value not in (None, {}, [], ())


def _runtime_context(context: RunContext | None, process_name: str) -> RunContext:
    """Issue a fresh manual invocation while preserving scheduler retry IDs."""

    config = dict(context.config) if context is not None else {}
    config.setdefault("process", process_name)
    config.setdefault("pipeline_id", process_name)
    config.setdefault("task_id", process_name)
    return RunContext(
        run_id=context.run_id if context is not None else f"manual-{uuid4().hex}",
        watermark=context.watermark if context is not None else None,
        config=config,
    )


def _processor(
    bindings: Any,
    *,
    load_config: Any,
    native_transfer_runtime_service: Any,
    route_capability_orchestrator: Any,
    load_governance_service: Any,
    mssql_transaction_admission_service: Any | None = None,
) -> Any:
    from dpone.runtime.etl.processor import ETLProcessor

    options = {}
    if mssql_transaction_admission_service is not None:
        options["mssql_transaction_admission_service"] = mssql_transaction_admission_service
    return ETLProcessor(
        source=bindings.source_obj,
        sink=bindings.sink_obj,
        log_sample_rows=load_config.log_sample_rows,
        etl_logger=bindings.etl_logger,
        run_state_storage=bindings.run_state_storage,
        load_identity_service=getattr(bindings, "load_identity_service", None),
        native_transfer_runtime_service=native_transfer_runtime_service,
        route_capability_orchestrator=route_capability_orchestrator,
        load_governance_service=load_governance_service,
        **options,
    )


def _worker_chunk_runner_factory(
    process_config: Any,
    *,
    route_capability_factory: Any,
    run_context: RunContext,
    dag_id: str | None,
    execution_date: Any | None,
):
    """Open one connection set per lane and rebuild only lightweight chunk services."""

    from dpone.backfill import normalize_backfill_execution_policy

    load_config = process_config.load_config
    options = getattr(load_config, "options", {}) or {}
    policy = normalize_backfill_execution_policy(dict(options.get("backfill") or {}))
    state_storage = getattr(getattr(process_config, "sink_obj", None), "state_storage", None)
    if policy.parallel_workers > 1 and getattr(state_storage, "atomicity", None) == "target_atomic":
        from dpone.runtime.etl.backfill_process_runtime import build_backfill_process_lane_runtime
        from dpone.runtime.route_runtime_factory import RouteCapabilityRuntimeFactory

        if type(route_capability_factory) is not RouteCapabilityRuntimeFactory:
            raise RuntimeError("mssql_transaction.parallel_backfill_custom_route_factory_unsupported")

        return build_backfill_process_lane_runtime(
            process_config,
            run_context=run_context,
            dag_id=dag_id,
            execution_date=execution_date,
        )

    @contextmanager
    def build(_worker_id: int):
        from dpone.ports.runtime_hydrator import ensure_runtime_hydrator
        from dpone.runtime.etl.backfill_process_runtime import (
            close_runtime_bindings,
            lane_route_coordinates,
        )
        from dpone.runtime.governance.service import LoadGovernanceService
        from dpone.runtime.native_transfer import NativeTransferRuntimeService

        if not process_config.raw_config:
            raise RuntimeError("mssql_transaction.parallel_backfill_runtime_config_required")
        bindings = None
        lane_route = None

        def run_chunk(load_config: Any) -> Any:
            nonlocal bindings, lane_route
            route = lane_route_coordinates(load_config)
            if lane_route is not None and route != lane_route:
                raise RuntimeError("mssql_transaction.parallel_backfill_lane_route_rebound")
            if bindings is None:
                bindings = ensure_runtime_hydrator().build(
                    config=process_config.raw_config,
                    load_config=load_config,
                )
                lane_route = route
            route_runtime = route_capability_factory.build(
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
            )
            return processor.run(
                load_config,
                run_context,
                dag_id=dag_id,
                execution_date=execution_date,
            )

        try:
            yield run_chunk
        finally:
            if bindings is not None:
                close_runtime_bindings(bindings)

    return build


def _worker_state_store_factory(process_config: Any, *, state_store_factory: Any):
    """Open one independently hydrated SQL state session per executor lane."""

    @contextmanager
    def build(worker_id: int):
        from dpone.ports.runtime_hydrator import ensure_runtime_hydrator
        from dpone.runtime.etl.backfill_process_runtime import close_runtime_bindings

        if not process_config.raw_config:
            raise RuntimeError("mssql_transaction.parallel_backfill_runtime_config_required")
        load_config = process_config.load_config
        bindings = ensure_runtime_hydrator().build(
            config=process_config.raw_config,
            load_config=load_config,
        )
        try:
            options = load_config.options or {}
            yield state_store_factory.build_worker(
                options.get("backfill") or {},
                sink_type=str(options.get("sink_type") or ""),
                sink_connector=bindings.sink_obj.connector,
                worker_id=worker_id,
            )
        finally:
            close_runtime_bindings(bindings)

    return build


__all__ = ["DefaultProcessRunner"]
