"""Chunked backfill orchestration on top of :class:`ETLProcessor`."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dpone.backfill import (
    BackfillChunkExecutor,
    BackfillExecutionSelection,
    BackfillStateStoreFactory,
    ChunkRunner,
    FileBackfillStateStore,
    backfill_run_key,
    build_backfill_result,
    load_or_create_backfill_ledger,
    normalize_backfill_execution_policy,
    plan_chunks,
    write_verification_execution,
)
from dpone.backfill.campaign_contract import campaign_identity_hashes, compose_campaign_contract
from dpone.backfill.campaign_lease import BackfillCampaignLease
from dpone.backfill.campaign_lifecycle import BackfillCampaignLifecyclePort
from dpone.backfill.chunk_lifecycle import BackfillChunkLifecyclePort
from dpone.backfill.lease_heartbeat import BackfillLeaseHeartbeat
from dpone.backfill.portable_scope_campaign import (
    compose_campaign_binding_identity,
)
from dpone.backfill.portable_scope_runtime import is_mssql_backfill_route
from dpone.backfill.target_publication import BackfillTargetPublicationPort
from dpone.backfill.worker_runtime import (
    BackfillProcessLaneRuntime,
    BackfillWorkerChunkRunnerFactory,
)
from dpone.backfill.worker_state import BackfillWorkerStateStoreFactory
from dpone.config.mssql_strategy_contract import normalize_mssql_backfill_campaign_strategy
from dpone.runtime.etl.backfill_campaign_preparation import BackfillCampaignPreparationService
from dpone.runtime.etl.backfill_dispatch import (
    execute_process_with_backfill,
    is_chunked_backfill,
    run_mapped_backfill,
)
from dpone.runtime.etl.backfill_mapped_campaign import (
    MappedCampaignExecutor as _MappedCampaignExecutor,
)
from dpone.runtime.etl.backfill_result_runtime import (
    aggregate_backfill_result,
    campaign_completed,
    export_verification_execution,
    requires_post_publication_normalization,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


class BackfillOrchestrator:
    """Plans, resumes and executes chunked backfill campaigns."""

    def __init__(
        self,
        *,
        chunk_runner: ChunkRunner,
        state_store: FileBackfillStateStore | None = None,
        state_store_factory: BackfillStateStoreFactory | None = None,
        sink_connector: Any | None = None,
        heartbeat_factory: Callable[..., BackfillLeaseHeartbeat] | None = None,
        campaign_lease_factory: Callable[[Any, str], BackfillCampaignLease] | None = None,
        lifecycle_port: BackfillChunkLifecyclePort | None = None,
        worker_state_store_factory: BackfillWorkerStateStoreFactory | None = None,
        worker_chunk_runner_factory: BackfillWorkerChunkRunnerFactory | BackfillProcessLaneRuntime | None = None,
        portable_scope_column_resolver: Callable[[Any, str], Any] | None = None,
        portable_scope_history_proof: Callable[..., None] | None = None,
        campaign_lifecycle: BackfillCampaignLifecyclePort | None = None,
        target_publisher: BackfillTargetPublicationPort | None = None,
    ) -> None:
        self._chunk_executor = BackfillChunkExecutor(
            chunk_runner,
            heartbeat_factory=heartbeat_factory,
            lifecycle_port=lifecycle_port,
            worker_state_store_factory=worker_state_store_factory,
            worker_chunk_runner_factory=worker_chunk_runner_factory,
        )
        self._state_store = state_store
        self._state_store_factory = state_store_factory or BackfillStateStoreFactory()
        self._sink_connector = sink_connector
        self._campaign_lease_factory = campaign_lease_factory or BackfillCampaignLease
        self._worker_state_store_factory = worker_state_store_factory
        self._campaign_preparation = BackfillCampaignPreparationService.compose(
            worker_runtime=worker_chunk_runner_factory,
            column_resolver=portable_scope_column_resolver,
            history_proof=portable_scope_history_proof,
        )
        self._campaign_lifecycle = campaign_lifecycle
        self._target_publisher = target_publisher

    def run(
        self,
        load_config: LoadConfig,
        *,
        selection: BackfillExecutionSelection | None = None,
    ) -> dict[str, Any]:
        started = time.time()
        backfill_options = dict((load_config.options or {}).get("backfill") or {})
        execution_policy = normalize_backfill_execution_policy(backfill_options)
        spec = execution_policy.chunk
        if spec is None:
            raise ValueError("BackfillOrchestrator requires a backfill.chunk configuration")
        if is_mssql_backfill_route(load_config):
            normalize_mssql_backfill_campaign_strategy(load_config)
        inner_mode = execution_policy.inner_mode
        retry_policy = execution_policy.retry_policy
        workers = execution_policy.parallel_workers
        if selection is None and workers > 1 and self._worker_state_store_factory is None:
            raise RuntimeError("backfill.parallel_worker_state_store_factory_required")
        self._chunk_executor.preflight(load_config)
        campaign_lifecycle = self._campaign_lifecycle
        target_publisher = self._target_publisher

        dataset = f"{load_config.target_schema}.{load_config.target_table}"
        run_key = str(backfill_options.get("backfill_id") or "") or backfill_run_key(
            dataset=dataset, spec=spec, inner_mode=inner_mode
        )
        chunks = plan_chunks(spec, run_key=run_key)
        if inner_mode == "full_refresh" and len(chunks) > 1:
            raise ValueError(
                "backfill.inner_mode=full_refresh truncates the whole target and is only valid for a "
                f"single-chunk plan; this plan has {len(chunks)} chunks"
            )

        store = self._resolve_state_store(load_config, backfill_options)
        existing_ledger = store.load(run_key)
        campaign_binding = self._campaign_preparation.resolve_initial_binding(
            load_config,
            column=spec.column,
            existing_ledger=existing_ledger,
        )
        campaign_contract = compose_campaign_contract(
            load_config,
            lifecycle=campaign_lifecycle,
            target_publisher=target_publisher,
        )
        campaign_contract = compose_campaign_binding_identity(campaign_contract, campaign_binding)
        current_plan_hash, current_config_hash = campaign_identity_hashes(
            chunks=chunks,
            dataset=dataset,
            execution_policy=execution_policy,
            campaign_contract=campaign_contract,
        )
        if campaign_lifecycle is not None:
            campaign_lifecycle.require_unmapped(selection=selection)
        if target_publisher is not None:
            target_publisher.require_unmapped(selection=selection)
        if selection is not None:
            mapped_executor = _MappedCampaignExecutor(
                executor=self._chunk_executor,
                preparation=self._campaign_preparation,
                binding=campaign_binding,
                lease_factory=self._campaign_lease_factory,
                column=spec.column,
                chunks=chunks,
            )
            outcome = run_mapped_backfill(
                chunk_executor=mapped_executor,
                load_config=load_config,
                store=store,
                selection=selection,
                chunks=chunks,
                run_key=run_key,
                dataset=dataset,
                inner_mode=inner_mode,
                retry_policy=retry_policy,
                spec=spec,
                current_plan_hash=current_plan_hash,
                current_config_hash=current_config_hash,
                started=started,
            )
            export_verification_execution(
                outcome.result,
                ledger=outcome.ledger,
                store=store,
                load_config=load_config,
                verification_writer=write_verification_execution,
            )
            return outcome.result
        ledger = load_or_create_backfill_ledger(
            store=store,
            run_key=run_key,
            dataset=dataset,
            inner_mode=inner_mode,
            chunks=chunks,
            spec_config=spec,
            plan_hash=current_plan_hash,
            config_hash=current_config_hash,
            portable_scope_column_contract=(campaign_binding.to_jsonable() if campaign_binding is not None else None),
        )
        if ledger.status == "cancel_requested" and not requires_post_publication_normalization(ledger):
            return aggregate_backfill_result(
                ledger=ledger,
                store=store,
                chunks=chunks,
                retry_policy=retry_policy,
                selected=0,
                skipped=len(ledger.committed_indexes()),
                errors=[],
                duration_seconds=time.time() - started,
                result_builder=build_backfill_result,
            )
        campaign_lease = self._campaign_lease_factory(store, ledger.run_key)
        campaign_lease.acquire()
        try:
            store.recover_stale_running(ledger.run_key, now=datetime.now(_UTC))
            ledger = store.load(ledger.run_key) or ledger
            prepared = self._campaign_preparation.prepare(
                load_config,
                ledger=ledger,
                store=store,
                lease=campaign_lease,
                chunks=chunks,
                column=spec.column,
                retry_policy=retry_policy,
                binding=campaign_binding,
                lifecycle=campaign_lifecycle,
                publisher=target_publisher,
            )
            ledger = prepared.ledger
            chunk_load_config = prepared.load_config
            committed = prepared.committed_indexes
            pending = prepared.pending_chunks

            errors = self._chunk_executor.run(
                pending,
                chunk_load_config,
                ledger,
                store,
                workers=workers,
                coordinator_tick=campaign_lease.tick,
                coordinator_reprove=lambda: campaign_lease.tick(force=True),
            )
            campaign_lease.tick(force=True)
            ledger = store.load(ledger.run_key) or ledger

            target_publication = xmin_handoff = None
            if campaign_completed(ledger, chunks, errors):
                campaign_lease.tick(force=True)
                if target_publisher is not None:
                    fenced_publication = getattr(target_publisher, "after_chunks_fenced", None)

                    def publish_target() -> dict[str, Any]:
                        if not callable(fenced_publication):
                            return target_publisher.after_chunks(load_config, ledger, store)
                        return fenced_publication(
                            load_config,
                            ledger,
                            store,
                            campaign_owner=campaign_lease.owner,
                        )

                    target_publication = campaign_lease.run_fenced(publish_target)
                if campaign_lifecycle is not None:
                    xmin_handoff = campaign_lease.run_fenced(
                        lambda: campaign_lifecycle.after_chunks(load_config, ledger, store)
                    )
            ledger = store.load(ledger.run_key) or ledger
            result = aggregate_backfill_result(
                ledger=ledger,
                store=store,
                chunks=chunks,
                retry_policy=retry_policy,
                selected=len(pending),
                skipped=len(committed),
                errors=errors,
                duration_seconds=time.time() - started,
                result_builder=build_backfill_result,
            )
            if target_publication is not None:
                result["target_publication"] = target_publication
                result["backfill"]["publication"] = target_publication
            if xmin_handoff is not None:
                result["xmin_handoff"] = xmin_handoff
            export_verification_execution(
                result,
                ledger=ledger,
                store=store,
                load_config=load_config,
                verification_writer=write_verification_execution,
            )
            campaign_lease.tick(force=True)
            campaign_lease.assert_healthy()
            return result
        finally:
            campaign_lease.release()

    def _resolve_state_store(
        self, load_config: LoadConfig, backfill_options: Mapping[str, Any]
    ) -> FileBackfillStateStore:
        if self._state_store is not None:
            return self._state_store
        options = load_config.options or {}
        return self._state_store_factory.build(
            backfill_options,
            sink_type=str(options.get("sink_type") or ""),
            sink_connector=self._sink_connector,
        )


__all__ = ["BackfillOrchestrator", "execute_process_with_backfill", "is_chunked_backfill"]
