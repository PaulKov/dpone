"""Cohesive runtime services used by :mod:`dpone.runtime.etl.processor`.

This module keeps source admission/extraction boundaries and result-safe
secondary handling out of the top-level orchestration class.  It is a facade
over focused services, not a second processor: business sequencing remains in
``ETLProcessor.run``.
"""

from __future__ import annotations

from contextlib import suppress
from typing import Any

from dpone.contracts.quality_failure import QualityGateFailure, QualityGateReceiptError
from dpone.runtime.commit_unknown import CommitUnknownError
from dpone.runtime.etl.mssql_operation_lease import lease_from_config
from dpone.runtime.etl.mssql_transaction_admission import MssqlTransactionAdmissionService
from dpone.runtime.etl.owned_payload_scope import OwnedPayloadScope
from dpone.runtime.etl.result_metrics import (
    enrich_post_commit_quality_failure_result,
    etl_start_payload,
    initial_etl_result,
    populate_success_result,
    staged_quality_gate_report,
)
from dpone.runtime.etl.source_extraction_lifecycle import SourceExtractionLifecycleService
from dpone.runtime.runtime_throughput import enrich_run_result_with_throughput
from dpone.security_redaction import redact_public_context, redact_public_text

RUNTIME_FAILURE_FALLBACK = "Runtime failed; error details unavailable."


class MssqlReplayQualityEvidenceRequired(RuntimeError):
    """A committed replay cannot prove the current non-inert quality policy."""

    code = "DPONE_MSSQL_REPLAY_QUALITY_EVIDENCE_REQUIRED"
    blocks_committed_success = True

    def __init__(self) -> None:
        super().__init__("mssql_transaction.replay_quality_evidence_required")


class ProcessorRuntimeServices:
    """Coordinate pre-source admission, route extraction, and source ownership."""

    def __init__(
        self,
        *,
        source: Any,
        sink: Any,
        source_state_service: Any,
        payload_load_service: Any,
        source_extraction_lifecycle_service: Any | None = None,
        mssql_transaction_admission_service: Any | None = None,
        route_capability_orchestrator: Any | None = None,
    ) -> None:
        self.source = source
        self.sink = sink
        self.source_state_service = source_state_service
        self.payload_load_service = payload_load_service
        self.source_extraction_lifecycle_service = (
            source_extraction_lifecycle_service or SourceExtractionLifecycleService()
        )
        self.mssql_transaction_admission_service = (
            mssql_transaction_admission_service or MssqlTransactionAdmissionService()
        )
        self.route_capability_orchestrator = route_capability_orchestrator

    def prepare_admission(
        self,
        load_config: Any,
        *,
        run_context: Any,
        load_record: Any,
        dag_id: str | None,
    ) -> Any:
        return self.mssql_transaction_admission_service.prepare(
            load_config,
            source=self.source,
            sink=self.sink,
            run_context=run_context,
            load_record=load_record,
            dag_id=dag_id,
        )

    def replay_result(self, load_config: Any) -> Any | None:
        return self.mssql_transaction_admission_service.replay_result(load_config)

    def preflight_before_extract(self, load_config: Any, load_record: Any) -> None:
        self.source_extraction_lifecycle_service.assert_supported(self.source, load_config)
        preflight = getattr(self.sink, "preflight_before_extract", None)
        if callable(preflight):
            preflight(load_config=load_config, load_record=load_record)
        schema_evolution = getattr(self.payload_load_service, "schema_evolution_service", None)
        schema_preflight = getattr(schema_evolution, "preflight_before_extract", None)
        if callable(schema_preflight):
            schema_preflight(load_config=load_config, source=self.source, sink=self.sink)

    def prepare_route_capabilities(self, load_config: Any, load_record: Any) -> Any | None:
        if self.route_capability_orchestrator is None:
            return None
        return self.route_capability_orchestrator.prepare(
            load_config=load_config,
            source=self.source,
            sink=self.sink,
            load_record=load_record,
        )

    def route_capability_summary(self) -> dict[str, Any] | None:
        if self.route_capability_orchestrator is None:
            return None
        summary = self.route_capability_orchestrator.summary()
        return {"summary": summary} if summary is not None else None

    def load_incremental_state(self, load_config: Any) -> Any:
        return self.source_state_service.load_for_extract(self.source, load_config)

    def extract(
        self,
        load_config: Any,
        incremental_state: Any,
        load_record: Any,
        route_context: Any | None,
    ) -> Any:
        def invoke() -> Any:
            if route_context is None:
                return self.source.extract(load_config, incremental_state)
            return route_context.extract(
                load_config=load_config,
                source=self.source,
                sink=self.sink,
                state=incremental_state,
                load_record=load_record,
            )

        return self.source_extraction_lifecycle_service.capture(invoke)

    @staticmethod
    def start_transaction_lease(load_config: Any) -> Any | None:
        lease = lease_from_config(load_config)
        if lease is not None:
            lease.start()
        return lease

    @staticmethod
    def own_extract_result(extract_result: Any) -> OwnedPayloadScope:
        return OwnedPayloadScope.from_extract_result(extract_result)

    def persist_source_state(
        self,
        *,
        original_load_config: Any,
        effective_load_config: Any,
        extract_result: Any,
        load_result: Any,
        should_persist: bool,
    ) -> None:
        self.source_state_service.persist_after_load(
            source=self.source,
            sink=self.sink,
            original_load_config=original_load_config,
            effective_load_config=effective_load_config,
            extract_result=extract_result,
            load_result=load_result,
            should_persist=should_persist,
        )

    def abort_prepared_source_boundary(self, load_config: Any) -> None:
        """Release a post-admission source lease that never reached terminal ownership."""

        abort = getattr(self.source, "abort_mssql_source_boundary", None)
        if callable(abort):
            abort(load_config)


def complete_replay_governance(
    *,
    load_config: Any,
    replay_result: Any,
    quality_snapshot: Any,
    quality_execution: Any,
    load_governance_service: Any,
    source: Any,
    sink: Any,
    load_record: Any,
    process_name: str | None,
) -> None:
    """Complete a source-free replay without accepting missing quality proof."""

    if not quality_snapshot.is_inert():
        raise MssqlReplayQualityEvidenceRequired
    staged_quality_gate_report(replay_result, load_config=load_config)
    quality_execution.accept_state(
        getattr(replay_result, "quality_gate_receipt", None),
        load_config=load_config,
    )
    load_governance_service.run_post_hooks(
        load_config=load_config,
        source=source,
        sink=sink,
        load_record=load_record,
        process_name=process_name,
    )


def committed_result(scope: OwnedPayloadScope | None, replay_result: Any | None) -> Any | None:
    """Return commit-boundary evidence even when secondary work raised."""

    if replay_result is not None:
        return replay_result
    return None if scope is None else scope.target_commit_result


def blocks_committed_success(error: BaseException) -> bool:
    """Keep blocking quality/governance outcomes visible across exact replay."""

    return bool(
        getattr(error, "blocks_committed_success", False)
        or isinstance(error, CommitUnknownError | QualityGateFailure | QualityGateReceiptError)
    )


def warn_persistence_failure(logger: Any, stage: str) -> None:
    with suppress(Exception):
        logger.warning(f"event=dpone.runtime_failure_persistence_failed stage={stage}")


def warn_terminal_cleanup_failure(logger: Any, receipt: Any) -> None:
    if getattr(receipt, "cleanup_succeeded", True):
        return
    with suppress(Exception):
        logger.warning(
            "event=dpone.source_artifact_terminal_cleanup_failed "
            f"outcome={receipt.outcome.value} error_code={receipt.cleanup_error_code}"
        )


def safe_error(error: BaseException) -> str:
    return redact_public_text(error, fallback=RUNTIME_FAILURE_FALLBACK, strip_traceback=True)


def safe_error_context(process_name: str | None, result: dict[str, Any]) -> dict[str, Any]:
    return redact_public_context({"process_name": process_name, "run_id": result.get("run_id")})


def enrich_throughput(result: dict[str, Any]) -> None:
    enrich_run_result_with_throughput(result)


__all__ = [
    "MssqlReplayQualityEvidenceRequired",
    "ProcessorRuntimeServices",
    "blocks_committed_success",
    "committed_result",
    "complete_replay_governance",
    "enrich_post_commit_quality_failure_result",
    "enrich_throughput",
    "etl_start_payload",
    "initial_etl_result",
    "populate_success_result",
    "safe_error",
    "safe_error_context",
    "staged_quality_gate_report",
    "warn_persistence_failure",
    "warn_terminal_cleanup_failure",
]
