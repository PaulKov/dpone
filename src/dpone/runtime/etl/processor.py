"""ETL processor orchestration for Source → Sink runtime execution."""

from __future__ import annotations

import time
from contextlib import nullcontext, suppress
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dpone.runtime.etl import processor_runtime as runtime
from dpone.runtime.etl.decision_lifecycle import RuntimeDecisionLifecycle
from dpone.runtime.etl.extracted_payload_load import (
    ExtractedPayloadLoadService,
    create_load_governance_service,
    create_load_identity_service,
)
from dpone.runtime.etl.load_config_runtime import LoadConfigRuntimeService
from dpone.runtime.etl.processor_payload_mixin import ProcessorPayloadMixin
from dpone.runtime.etl.reconciliation_service import ReconciliationService
from dpone.runtime.etl.run_state_tracker import RunStateTracker
from dpone.runtime.etl.source_state import SourceStateService
from dpone.runtime.process_logging import create_etl_logger

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.runtime.etl.run_state_tracker import RunStateStoragePort
    from dpone.runtime.process_logging import ETLLogger
    from dpone.runtime.sinks.sink_protocol import AbstractSink
    from dpone.runtime.sources import AbstractSource


class ETLProcessor(ProcessorPayloadMixin):
    """Coordinate Source → Sink execution through injected config, state, reconciliation, and payload services."""

    def __init__(
        self,
        source: AbstractSource,
        sink: AbstractSink,
        log_sample_rows: int = 5,
        etl_logger: ETLLogger | None = None,
        run_state_storage: RunStateStoragePort | None = None,
        load_config_service: LoadConfigRuntimeService | None = None,
        reconciliation_service: ReconciliationService | None = None,
        schema_evolution_service: Any | None = None,
        runtime_lifecycle_service: Any | None = None,
        temporal_fidelity_projector_cls: Any | None = None,
        load_identity_service: Any | None = None,
        row_lineage_enricher: Any | None = None,
        nested_normalization_service: Any | None = None,
        nested_load_service: Any | None = None,
        strategy_metadata_enricher: Any | None = None,
        source_state_service: SourceStateService | None = None,
        source_extraction_lifecycle_service: Any | None = None,
        load_governance_service: Any | None = None,
        native_transfer_runtime_service: Any | None = None,
        payload_load_service: Any | None = None,
        extracted_payload_load_service: Any | None = None,
        route_capability_orchestrator: Any | None = None,
        mssql_transaction_admission_service: Any | None = None,
        run_state_tracker_cls: type[RunStateTracker] = RunStateTracker,
    ):
        self.source = source
        self.sink = sink
        self.log_sample_rows = log_sample_rows
        self.logger = etl_logger or create_etl_logger()
        self.run_state_storage = run_state_storage
        self.load_config_service = load_config_service or LoadConfigRuntimeService()
        self.reconciliation_service = reconciliation_service or ReconciliationService(
            source=source,
            sink=sink,
            logger=self.logger,
            run_state_storage=run_state_storage,
        )
        self.load_identity_service = load_identity_service or create_load_identity_service()
        self.source_state_service = source_state_service or SourceStateService()
        self.load_governance_service = load_governance_service or create_load_governance_service()
        self.extracted_payload_load_service = extracted_payload_load_service or ExtractedPayloadLoadService(
            source=self.source,
            sink=self.sink,
            logger=self.logger,
            load_identity_service=self.load_identity_service,
            row_lineage_enricher=row_lineage_enricher,
            nested_normalization_service=nested_normalization_service,
            nested_load_service=nested_load_service,
            strategy_metadata_enricher=strategy_metadata_enricher,
            payload_load_service=payload_load_service,
            schema_evolution_service=schema_evolution_service,
            runtime_lifecycle_service=runtime_lifecycle_service,
            temporal_fidelity_projector_cls=temporal_fidelity_projector_cls,
            native_transfer_runtime_service=native_transfer_runtime_service,
            load_governance_service=self.load_governance_service,
        )
        self.row_lineage_enricher = getattr(self.extracted_payload_load_service, "row_lineage_enricher", None)
        self.nested_load_service = getattr(self.extracted_payload_load_service, "nested_load_service", None)
        self.strategy_metadata_enricher = getattr(
            self.extracted_payload_load_service,
            "strategy_metadata_enricher",
            None,
        )
        self.payload_load_service = getattr(self.extracted_payload_load_service, "payload_load_service", None)
        self._runtime = runtime.ProcessorRuntimeServices(
            source=self.source,
            sink=self.sink,
            source_state_service=self.source_state_service,
            payload_load_service=self.payload_load_service,
            source_extraction_lifecycle_service=source_extraction_lifecycle_service,
            mssql_transaction_admission_service=mssql_transaction_admission_service,
            route_capability_orchestrator=route_capability_orchestrator,
        )
        self.source_extraction_lifecycle_service = self._runtime.source_extraction_lifecycle_service
        self.route_capability_orchestrator = self._runtime.route_capability_orchestrator
        self.mssql_transaction_admission_service = self._runtime.mssql_transaction_admission_service
        self.run_state_tracker_cls = run_state_tracker_cls

    def run(
        self,
        load_config: LoadConfig,
        run_context: Any | None = None,
        dag_id: str | None = None,
        execution_date: datetime | None = None,
    ) -> dict[str, Any]:
        runtime_config = self.load_config_service.prepare(load_config)
        quality_snapshot = self._quality_snapshot_from_load_config(runtime_config)
        self.logger.log_etl_start(runtime.etl_start_payload(load_config))

        start_time = time.time()
        tracker = self.run_state_tracker_cls(self.run_state_storage)
        tracker.start(load_config, dag_id, execution_date, started_at=datetime.now())

        result = runtime.initial_etl_result()
        decision_lifecycle = RuntimeDecisionLifecycle(
            load_governance_service=self.load_governance_service,
            load_identity_service=self.load_identity_service,
            logger=self.logger,
        )
        decision_lifecycle.configure_audit_storage(sink=self.sink, load_config=runtime_config)
        load_record: Any | None = None
        transaction_lease: Any | None = None
        owned_payload_scope: Any | None = None
        replay_result: Any | None = None

        try:
            load_record = self.load_identity_service.start(runtime_config, process_name=dag_id)
            runtime_config = self.load_config_service.inject_extract_identity(runtime_config, load_record)
            quality_execution = self._create_quality_gate_execution(
                quality_snapshot,
                load_record,
            )
            quality_execution.assert_current(load_config=runtime_config)
            result["run_id"] = load_record.run_id
            result["load_id"] = load_record.load_id
            with decision_lifecycle.activate(load_config=runtime_config, load_record=load_record):
                runtime_config = self._runtime.prepare_admission(
                    runtime_config,
                    run_context=run_context,
                    load_record=load_record,
                    dag_id=dag_id,
                )
                transaction_lease = self._runtime.start_transaction_lease(runtime_config)
                replay_result = self._runtime.replay_result(runtime_config)
                if replay_result is not None:
                    self.load_identity_service.mark_committed(load_record, replay_result)
                    runtime.populate_success_result(
                        result,
                        replay_result,
                        validation_info=None,
                        reconciliation_metrics=replay_result.reconciliation_metrics,
                    )
                    runtime.complete_replay_governance(
                        load_config=runtime_config,
                        replay_result=replay_result,
                        quality_snapshot=quality_snapshot,
                        quality_execution=quality_execution,
                        load_governance_service=self.load_governance_service,
                        source=self.source,
                        sink=self.sink,
                        load_record=load_record,
                        process_name=dag_id,
                    )
                    return result
                self._runtime.preflight_before_extract(runtime_config, load_record)
                self.load_governance_service.run_pre_hooks(
                    load_config=runtime_config,
                    source=self.source,
                    sink=self.sink,
                    load_record=load_record,
                    process_name=dag_id,
                )
                route_context = self._runtime.prepare_route_capabilities(runtime_config, load_record)
                if route_context is not None:
                    result["route_capabilities"] = self._runtime.route_capability_summary()
                incremental_state = self._runtime.load_incremental_state(runtime_config)
                extract_result = self._runtime.extract(runtime_config, incremental_state, load_record, route_context)
                owned_payload_scope = self._runtime.own_extract_result(extract_result)
                if transaction_lease is not None:
                    transaction_lease.assert_healthy()
                self.logger.log_etl_progress(
                    "EXTRACTION_COMPLETE",
                    {"Has State": extract_result.state is not None},
                )

                effective_config = self.load_config_service.apply_extract_result_overrides(
                    runtime_config,
                    extract_result,
                    self.logger,
                    run_state_tracker=tracker,
                )
                effective_config = self.load_config_service.enrich_for_load(
                    effective_config,
                    extract_result,
                    self.source,
                    self.logger,
                )

                load_result, reconciliation_metrics = self._load_extracted_payload(
                    effective_config,
                    extract_result,
                    load_record,
                    quality_execution,
                    owned_payload_scope,
                )
                terminal_receipt = owned_payload_scope.success()
                result["artifact_terminal"] = terminal_receipt.to_dict()
                runtime.warn_terminal_cleanup_failure(self.logger, terminal_receipt)
                if transaction_lease is not None:
                    transaction_lease.assert_healthy()
                runtime.staged_quality_gate_report(
                    load_result,
                    load_config=effective_config,
                )

                validation_info = self.load_config_service.build_validation_info(
                    self.source,
                    effective_config,
                    load_result,
                    extract_result,
                )
                quality_execution.accept_state(
                    getattr(load_result, "quality_gate_receipt", None),
                    load_config=effective_config,
                )
                runtime.populate_success_result(
                    result,
                    load_result,
                    validation_info=validation_info,
                    reconciliation_metrics=reconciliation_metrics,
                )

                self._runtime.persist_source_state(
                    original_load_config=load_config,
                    effective_load_config=effective_config,
                    extract_result=extract_result,
                    load_result=load_result,
                    should_persist=self.load_config_service.should_persist_state(
                        load_config,
                        effective_config,
                        extract_result,
                    ),
                )
                self.load_governance_service.run_post_hooks(
                    load_config=effective_config,
                    source=self.source,
                    sink=self.sink,
                    load_record=load_record,
                    process_name=dag_id,
                )
        except BaseException as exc:
            committed = runtime.committed_result(owned_payload_scope, replay_result)
            if owned_payload_scope is not None:
                # The main decision context has unwound on failure. Keep the
                # same load identity/publisher active for cleanup evidence too.
                with decision_lifecycle.activate(load_config=runtime_config, load_record=load_record):
                    terminal_receipt = (
                        owned_payload_scope.success()
                        if committed is not None and not runtime.blocks_committed_success(exc)
                        else owned_payload_scope.terminate_for_error(exc)
                    )
                result["artifact_terminal"] = terminal_receipt.to_dict()
                runtime.warn_terminal_cleanup_failure(self.logger, terminal_receipt)
            if not isinstance(exc, Exception):
                raise
            runtime.enrich_post_commit_quality_failure_result(result, exc)
            if self.route_capability_orchestrator is not None:
                with suppress(Exception):
                    result["route_capabilities"] = self._runtime.route_capability_summary()
            safe_error = runtime.safe_error(exc)
            if committed is not None and not runtime.blocks_committed_success(exc):
                if result.get("status") != "success":
                    runtime.populate_success_result(
                        result,
                        committed,
                        validation_info=None,
                        reconciliation_metrics=getattr(committed, "reconciliation_metrics", None),
                    )
                result.setdefault("secondary_warnings", []).append(safe_error)
                if load_record is not None:
                    try:
                        self.load_identity_service.mark_committed(load_record, committed)
                    except Exception:  # noqa: BLE001 - target receipt remains authoritative.
                        runtime.warn_persistence_failure(self.logger, "load_audit_post_commit")
                with suppress(Exception):
                    self.logger.warning(f"event=dpone.committed_secondary_failed error={safe_error}")
                return result
            result["errors"].append(safe_error)
            result["status"] = "error"
            safe_context = runtime.safe_error_context(dag_id, result)
            if load_record is not None:
                try:
                    if committed is None:
                        self.load_identity_service.mark_failed(load_record, RuntimeError(safe_error))
                    else:
                        self.load_identity_service.mark_committed(load_record, committed)
                except Exception:  # noqa: BLE001 - failure evidence must not replace the primary exception.
                    runtime.warn_persistence_failure(self.logger, "load_audit")
            with suppress(Exception):
                self.logger.log_etl_error(safe_error, safe_context)
            try:
                tracker.mark_failed(result, RuntimeError(safe_error))
            except Exception:  # noqa: BLE001 - run state is secondary to the primary runtime exception.
                runtime.warn_persistence_failure(self.logger, "run_state")
            raise
        finally:
            if transaction_lease is not None:
                with suppress(Exception):
                    transaction_lease.stop()
            finalization_guard = suppress(Exception) if result["status"] == "error" else nullcontext()
            with finalization_guard:
                self._runtime.abort_prepared_source_boundary(runtime_config)
                result["duration_seconds"] = time.time() - start_time
                runtime.enrich_throughput(result)
                result["runtime_decisions"] = decision_lifecycle.summary_json()
                self.logger.log_etl_end(result)
            if result["status"] != "error":
                if not tracker.mark_success_preserving_commit(result):
                    runtime.warn_persistence_failure(self.logger, "run_state_post_commit")

        return result
