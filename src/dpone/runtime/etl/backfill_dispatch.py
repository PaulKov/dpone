"""Process-runner dispatch and mapped chunk execution for backfill orchestration."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from dpone.backfill import (
    AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY,
    AirflowBackfillMappingViolation,
    BackfillChunkExecutor,
    BackfillExecutionSelection,
    BackfillLedger,
    FileBackfillStateStore,
    build_backfill_result,
    load_or_create_backfill_ledger,
    normalize_backfill_execution_policy,
    select_pending_chunks,
)
from dpone.backfill.worker_runtime import (
    BackfillProcessLaneRuntime,
    BackfillWorkerChunkRunnerFactory,
)
from dpone.backfill.worker_state import BackfillWorkerStateStoreFactory
from dpone.config.load_strategy import LoadStrategy

if TYPE_CHECKING:
    from dpone.backfill import BackfillChunk
    from dpone.config.load_config import LoadConfig

_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


@dataclass(frozen=True, slots=True)
class MappedBackfillOutcome:
    """Mapped result paired with the authoritative ledger used to build it."""

    result: dict[str, Any]
    ledger: BackfillLedger


def is_chunked_backfill(load_config: LoadConfig) -> bool:
    if load_config.load_strategy != LoadStrategy.BACKFILL:
        return False
    backfill_options = (load_config.options or {}).get("backfill") or {}
    return bool(backfill_options.get("chunk"))


def run_mapped_backfill(
    *,
    chunk_executor: BackfillChunkExecutor,
    load_config: LoadConfig,
    store: FileBackfillStateStore,
    selection: BackfillExecutionSelection,
    chunks: tuple[BackfillChunk, ...],
    run_key: str,
    dataset: str,
    inner_mode: str,
    retry_policy: str,
    spec: Any,
    current_plan_hash: str,
    current_config_hash: str,
    started: float,
) -> MappedBackfillOutcome:
    """Initialize and execute the selected contiguous range under its mapping lock."""

    _verify_mapped_selection(selection, chunks=chunks, current_plan_hash=current_plan_hash)
    _require_mapped_state_capabilities(store)
    owner = f"dpone-backfill-mapping-init:{uuid4().hex}"
    if not store.acquire_initialization_lock(run_key, owner=owner):
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_CAMPAIGN_BUSY",
            f"mapped backfill campaign initialization is busy for {run_key}",
        )
    try:
        ledger = load_or_create_backfill_ledger(
            store=store,
            run_key=run_key,
            dataset=dataset,
            inner_mode=inner_mode,
            chunks=chunks,
            spec_config=spec,
            plan_hash=current_plan_hash,
            config_hash=current_config_hash,
        )
        store.recover_stale_running(run_key, now=datetime.now(_UTC))
    finally:
        store.release_initialization_lock(run_key, owner=owner)

    ledger = store.load(run_key) or ledger
    selected_indexes = set(selection.chunk_indexes)
    if ledger.status == "cancel_requested":
        result = build_backfill_result(
            ledger=ledger,
            store=store,
            chunks=chunks,
            retry_policy=retry_policy,
            selected=0,
            skipped=len(selected_indexes & ledger.committed_indexes()),
            errors=[],
            duration_seconds=time.time() - started,
            selection=selection,
        )
        return MappedBackfillOutcome(result=result, ledger=ledger)

    pending = [
        chunk
        for chunk in select_pending_chunks(chunks, ledger=ledger, retry_policy=retry_policy)
        if chunk.index in selected_indexes
    ]
    committed_before = selected_indexes & ledger.committed_indexes()
    errors = chunk_executor.run(pending, load_config, ledger, store)
    ledger = store.load(run_key) or ledger
    result = build_backfill_result(
        ledger=ledger,
        store=store,
        chunks=chunks,
        retry_policy=retry_policy,
        selected=len(pending),
        skipped=len(committed_before),
        errors=errors,
        duration_seconds=time.time() - started,
        selection=selection,
    )
    return MappedBackfillOutcome(result=result, ledger=ledger)


def _verify_mapped_selection(
    selection: BackfillExecutionSelection,
    *,
    chunks: tuple[BackfillChunk, ...],
    current_plan_hash: str,
) -> None:
    expected_hash = "sha256:" + current_plan_hash
    indexes = selection.chunk_indexes
    if selection.backfill_plan_hash != expected_hash or not indexes or indexes[-1] > len(chunks):
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_PLAN_MISMATCH",
            "runtime backfill plan does not match the immutable Airflow mapping item",
        )
    if tuple(chunk.index for chunk in chunks if chunk.index in set(indexes)) != indexes:
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_PLAN_MISMATCH",
            "runtime backfill chunk range is not contiguous in the immutable plan",
        )


def _require_mapped_state_capabilities(store: FileBackfillStateStore) -> None:
    capabilities = store.state_capabilities()
    required = ("distributed_lock", "distributed_chunk_lease", "compare_and_set_completion")
    if not all(capabilities.get(name) is True for name in required):
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_STATE_UNSUPPORTED",
            "mapped backfill requires distributed campaign lock, chunk lease and owner-CAS completion",
        )


def execute_process_with_backfill(
    processor: Any,
    load_config: LoadConfig,
    run_context: Any = None,
    *,
    dag_id: str | None = None,
    execution_date: datetime | None = None,
    worker_chunk_runner_factory: BackfillWorkerChunkRunnerFactory | BackfillProcessLaneRuntime | None = None,
    worker_state_store_factory: BackfillWorkerStateStoreFactory | None = None,
    xmin_handoff_state_storage: Any | None = None,
    orchestrator_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Run one process, orchestrating chunks when backfill chunking is configured."""

    context_config = getattr(run_context, "config", {}) if run_context is not None else {}
    selection = (
        context_config.get(AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY) if isinstance(context_config, Mapping) else None
    )
    if selection is not None and not isinstance(selection, BackfillExecutionSelection):
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_ITEM_INVALID",
            "run context contains an invalid Airflow mapping selection",
        )
    if not is_chunked_backfill(load_config):
        if selection is not None:
            raise AirflowBackfillMappingViolation(
                "DPONE_AIRFLOW_MAPPING_REQUIRES_CHUNKED_BACKFILL",
                "an Airflow mapping item can execute only a chunked backfill process",
            )
        return processor.run(load_config, run_context, dag_id=dag_id, execution_date=execution_date)

    execution_policy = normalize_backfill_execution_policy(dict((load_config.options or {}).get("backfill") or {}))
    state_storage = getattr(getattr(processor, "sink", None), "state_storage", None)
    worker_scoped = (
        execution_policy.parallel_workers > 1 and getattr(state_storage, "atomicity", None) == "target_atomic"
    )
    if worker_scoped and not isinstance(worker_chunk_runner_factory, BackfillProcessLaneRuntime):
        raise RuntimeError("mssql_transaction.parallel_backfill_process_bootstrap_required")

    def run_chunk(chunk_config: LoadConfig) -> Mapping[str, Any]:
        return processor.run(chunk_config, run_context, dag_id=dag_id, execution_date=execution_date)

    if orchestrator_factory is None:
        from dpone.runtime.etl.backfill_orchestrator import BackfillOrchestrator

        orchestrator_factory = BackfillOrchestrator
    sink_connector = getattr(getattr(processor, "sink", None), "connector", None)
    orchestrator_options: dict[str, Any] = {
        "chunk_runner": run_chunk,
        "sink_connector": sink_connector,
        "worker_state_store_factory": worker_state_store_factory,
        "worker_chunk_runner_factory": worker_chunk_runner_factory if worker_scoped else None,
    }
    from dpone.backfill.portable_scope_runtime import is_postgres_mssql_backfill_route

    if is_postgres_mssql_backfill_route(load_config) and not worker_scoped:
        from dpone.runtime.etl.backfill_portable_scope_history import (
            build_mssql_backfill_portable_scope_history_proof,
        )
        from dpone.runtime.etl.portable_scope_preflight import PortableScopeColumnResolver

        orchestrator_options["portable_scope_column_resolver"] = PortableScopeColumnResolver(
            source=processor.source,
            sink=processor.sink,
        )
        orchestrator_options["portable_scope_history_proof"] = build_mssql_backfill_portable_scope_history_proof(
            source=processor.source,
            sink=processor.sink,
            run_context=run_context,
            dag_id=dag_id,
        )
    if execution_policy.publication.mode == "shadow_swap":
        if sink_connector is None:
            raise RuntimeError("mssql_backfill_publication.runtime_composition_required")
        from dpone.runtime.sinks.mssql_backfill_publication import MssqlBackfillShadowPublisher

        orchestrator_options["target_publisher"] = MssqlBackfillShadowPublisher(sink_connector)
    from dpone.config.postgres_xmin_execution import require_postgres_xmin_execution_route
    from dpone.runtime.postgres_xmin_execution import PostgresXminExecutionMode

    xmin_execution = require_postgres_xmin_execution_route(load_config)
    if xmin_execution.mode is PostgresXminExecutionMode.INITIAL:
        if xmin_handoff_state_storage is None:
            raise RuntimeError("postgres_xmin_handoff.state_storage_required")
        source_builder = getattr(getattr(processor, "source", None), "build_xmin_initial_handoff_source", None)
        if not callable(source_builder) or sink_connector is None:
            raise RuntimeError("postgres_xmin_handoff.runtime_composition_required")
        from dpone.backfill.xmin_handoff import PostgresXminInitialHandoffLifecycle
        from dpone.runtime.state.mssql_xmin_handoff import MssqlXminHandoffCommitter

        orchestrator_options["campaign_lifecycle"] = PostgresXminInitialHandoffLifecycle(
            source=source_builder(xmin_handoff_state_storage),
            committer=MssqlXminHandoffCommitter(
                target_connector=sink_connector,
                state_storage=xmin_handoff_state_storage,
                load_config=load_config,
            ),
        )
    return orchestrator_factory(
        **orchestrator_options,
    ).run(load_config, selection=selection)
