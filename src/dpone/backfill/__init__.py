"""Backfill domain package (public facade).

Pure (stdlib-only) building blocks for resumable chunked historical loads:

- :mod:`dpone.backfill.models` — chunk spec parsing and chunk value objects
- :mod:`dpone.backfill.planner` — deterministic chunk plan generation
- :mod:`dpone.backfill.state` — durable chunk ledger for resume semantics
- :mod:`dpone.backfill.verification` — route-refresh verification bridge

Consumers should import from this facade; symbols resolve lazily so
metadata-only tooling stays lightweight. Runtime orchestration lives in
:mod:`dpone.runtime.etl.backfill_orchestrator`.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "CHUNK_STATUS_FAILED",
    "CHUNK_STATUS_PENDING",
    "CHUNK_STATUS_RUNNING",
    "CHUNK_STATUS_SUCCESS",
    "BackfillChunk",
    "BackfillChunkExecutor",
    "BackfillChunkRecord",
    "BackfillChunkSpec",
    "BackfillDiagnosticConnectorResolver",
    "BackfillEvidenceBundle",
    "BackfillEvidenceCollector",
    "BackfillExecutionSelection",
    "BackfillExecutionPolicy",
    "BackfillLedger",
    "BackfillPerformanceAdvisor",
    "BackfillPredicateRenderer",
    "BackfillStateStore",
    "BackfillStateStoreFactory",
    "BackfillStatePolicy",
    "AirflowBackfillMappingViolation",
    "AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY",
    "ChunkRunner",
    "ClickHouseBackfillStateStore",
    "FileBackfillStateStore",
    "MSSQLBackfillStateStore",
    "PostgresBackfillStateStore",
    "backfill_run_key",
    "build_backfill_result",
    "chunk_spec_from_options",
    "collect_advisor_evidence",
    "combined_predicate",
    "config_hash",
    "execution_policy_from_load_config",
    "inner_mode_from_options",
    "lease_expires_at",
    "load_or_create_backfill_ledger",
    "normalize_retry_policy",
    "normalize_backfill_execution_policy",
    "parallel_workers_from_options",
    "plan_hash",
    "plan_chunks",
    "render_chunk_predicate",
    "row_count_verification",
    "select_pending_chunks",
    "verification_execution_payload",
    "write_verification_execution",
]

_EXPORTS: dict[str, str] = {
    "AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY": "dpone.backfill.mapping:AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY",
    "AirflowBackfillMappingViolation": "dpone.backfill.mapping:AirflowBackfillMappingViolation",
    "BackfillExecutionSelection": "dpone.backfill.mapping:BackfillExecutionSelection",
    "BackfillExecutionPolicy": "dpone.backfill.execution_policy:BackfillExecutionPolicy",
    "BackfillStatePolicy": "dpone.backfill.execution_policy:BackfillStatePolicy",
    "BackfillChunkExecutor": "dpone.backfill.runtime_execution:BackfillChunkExecutor",
    "ChunkRunner": "dpone.backfill.runtime_execution:ChunkRunner",
    "build_backfill_result": "dpone.backfill.runtime_execution:build_backfill_result",
    "load_or_create_backfill_ledger": "dpone.backfill.runtime_execution:load_or_create_backfill_ledger",
    "BackfillChunk": "dpone.backfill.models:BackfillChunk",
    "BackfillChunkSpec": "dpone.backfill.models:BackfillChunkSpec",
    "chunk_spec_from_options": "dpone.backfill.models:chunk_spec_from_options",
    "inner_mode_from_options": "dpone.backfill.models:inner_mode_from_options",
    "parallel_workers_from_options": "dpone.backfill.models:parallel_workers_from_options",
    "plan_chunks": "dpone.backfill.planner:plan_chunks",
    "backfill_run_key": "dpone.backfill.planner:backfill_run_key",
    "BackfillChunkRecord": "dpone.backfill.state:BackfillChunkRecord",
    "BackfillLedger": "dpone.backfill.state:BackfillLedger",
    "FileBackfillStateStore": "dpone.backfill.state:FileBackfillStateStore",
    "BackfillStateStore": "dpone.backfill.state:BackfillStateStore",
    "BackfillPredicateRenderer": "dpone.backfill.predicates:BackfillPredicateRenderer",
    "BackfillPerformanceAdvisor": "dpone.backfill.advisor:BackfillPerformanceAdvisor",
    "BackfillDiagnosticConnectorResolver": ("dpone.backfill.diagnostic_connector:BackfillDiagnosticConnectorResolver"),
    "BackfillEvidenceBundle": "dpone.backfill.evidence:BackfillEvidenceBundle",
    "BackfillEvidenceCollector": "dpone.backfill.evidence:BackfillEvidenceCollector",
    "collect_advisor_evidence": "dpone.backfill.evidence:collect_advisor_evidence",
    "BackfillStateStoreFactory": "dpone.backfill.state_factory:BackfillStateStoreFactory",
    "ClickHouseBackfillStateStore": "dpone.backfill.sql_state:ClickHouseBackfillStateStore",
    "MSSQLBackfillStateStore": "dpone.backfill.sql_state:MSSQLBackfillStateStore",
    "PostgresBackfillStateStore": "dpone.backfill.sql_state:PostgresBackfillStateStore",
    "combined_predicate": "dpone.backfill.execution_policy:combined_predicate",
    "config_hash": "dpone.backfill.execution_policy:config_hash",
    "execution_policy_from_load_config": "dpone.backfill.execution_policy:execution_policy_from_load_config",
    "lease_expires_at": "dpone.backfill.execution_policy:lease_expires_at",
    "normalize_retry_policy": "dpone.backfill.execution_policy:normalize_retry_policy",
    "normalize_backfill_execution_policy": "dpone.backfill.execution_policy:normalize_backfill_execution_policy",
    "plan_hash": "dpone.backfill.execution_policy:plan_hash",
    "render_chunk_predicate": "dpone.backfill.execution_policy:render_chunk_predicate",
    "row_count_verification": "dpone.backfill.execution_policy:row_count_verification",
    "select_pending_chunks": "dpone.backfill.execution_policy:select_pending_chunks",
    "CHUNK_STATUS_FAILED": "dpone.backfill.state:CHUNK_STATUS_FAILED",
    "CHUNK_STATUS_PENDING": "dpone.backfill.state:CHUNK_STATUS_PENDING",
    "CHUNK_STATUS_RUNNING": "dpone.backfill.state:CHUNK_STATUS_RUNNING",
    "CHUNK_STATUS_SUCCESS": "dpone.backfill.state:CHUNK_STATUS_SUCCESS",
    "verification_execution_payload": "dpone.backfill.verification:verification_execution_payload",
    "write_verification_execution": "dpone.backfill.verification:write_verification_execution",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))
