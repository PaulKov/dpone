"""Lease-safe sequential, threaded and process-lane chunk execution."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from dpone.backfill.chunk_lifecycle import BackfillChunkLifecycleContext, BackfillChunkLifecyclePort
from dpone.backfill.execution_policy import lease_expires_at
from dpone.backfill.lease_heartbeat import BackfillLeaseHeartbeat
from dpone.backfill.parallel_execution import run_parallel_chunk_lanes
from dpone.backfill.portable_scope_campaign import CampaignPortableScopeBinding
from dpone.backfill.process_lane_preflight import require_process_lane_runtime
from dpone.backfill.progress import backfill_progress
from dpone.backfill.runtime_results import project_chunk_execution_evidence
from dpone.backfill.state import (
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_SUCCESS,
    BackfillLedger,
    FileBackfillStateStore,
)
from dpone.backfill.worker_runtime import BackfillProcessLaneRuntime, BackfillWorkerChunkRunnerFactory

if TYPE_CHECKING:
    from dpone.backfill.models import BackfillChunk
    from dpone.backfill.worker_state import BackfillWorkerStateStoreFactory
    from dpone.config.load_config import LoadConfig

_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.
_LOG = logging.getLogger("dpone.backfill.runtime_execution")

ChunkRunner = Callable[["LoadConfig"], Mapping[str, Any]]


class BackfillChunkExecutionMixin:
    """Coordinate chunk leases, heartbeats and isolated execution lanes."""

    if TYPE_CHECKING:
        _chunk_runner: ChunkRunner
        _heartbeat_factory: Callable[..., BackfillLeaseHeartbeat]
        _lifecycle: BackfillChunkLifecyclePort
        _worker_state_store_factory: BackfillWorkerStateStoreFactory | None
        _worker_chunk_runner_factory: BackfillWorkerChunkRunnerFactory | BackfillProcessLaneRuntime | None

        @staticmethod
        def build_chunk_load_config(
            load_config: LoadConfig,
            chunk: BackfillChunk,
            *,
            run_key: str,
            plan_hash: str | None,
            owner: str,
            lease_expires_at_utc: datetime,
            portable_scope_campaign: CampaignPortableScopeBinding | None = None,
        ) -> LoadConfig: ...

    def preflight(self, load_config: LoadConfig | None = None) -> None:
        """Validate a spawned runtime before any campaign mutation or I/O."""

        if isinstance(self._worker_chunk_runner_factory, BackfillProcessLaneRuntime):
            require_process_lane_runtime(self._worker_chunk_runner_factory, load_config)

    def run(
        self,
        pending: list[BackfillChunk],
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: FileBackfillStateStore,
        *,
        workers: int = 1,
        coordinator_tick: Callable[[], None] | None = None,
        coordinator_reprove: Callable[[], None] | None = None,
    ) -> list[str]:
        self.preflight(load_config)
        errors: list[str] = []
        if isinstance(self._worker_chunk_runner_factory, BackfillProcessLaneRuntime):
            if coordinator_tick is None:
                raise RuntimeError("backfill.process_campaign_lease_tick_required")
            if coordinator_reprove is None:
                raise RuntimeError("backfill.process_campaign_lease_reprove_required")
            process_runtime = self._worker_chunk_runner_factory.with_coordinator_lease(
                coordinator_tick,
                coordinator_reprove,
            )
            process_execution_factory = process_runtime.process_chunk_execution_factory
            if not callable(process_execution_factory):  # Defensive after frozen dataclass replacement.
                raise RuntimeError("backfill.process_chunk_execution_factory_unavailable")
            errors.extend(
                process_execution_factory(
                    runtime=process_runtime,
                    lifecycle=self._lifecycle,
                    heartbeat_factory=self._heartbeat_factory,
                    chunk_config_builder=self.build_chunk_load_config,
                    progress_logger=self._log_progress,
                ).run(
                    pending,
                    load_config,
                    ledger,
                    store,
                    workers=workers,
                )
            )
        elif workers > 1 and len(pending) > 1:
            self._run_parallel(
                pending,
                load_config,
                ledger,
                store,
                errors,
                workers,
                coordinator_tick=coordinator_tick,
            )
        else:
            self._run_sequential(
                pending,
                load_config,
                ledger,
                store,
                errors,
                coordinator_tick=coordinator_tick,
            )
        return errors

    def _run_sequential(
        self,
        pending: list[BackfillChunk],
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: FileBackfillStateStore,
        errors: list[str],
        *,
        coordinator_tick: Callable[[], None] | None,
    ) -> None:
        for chunk in pending:
            if not self._run_one(
                chunk,
                load_config,
                ledger,
                store,
                errors,
                coordinator_tick=coordinator_tick,
            ):
                break

    def _run_parallel(
        self,
        pending: list[BackfillChunk],
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: FileBackfillStateStore,
        errors: list[str],
        workers: int,
        *,
        coordinator_tick: Callable[[], None] | None,
    ) -> None:
        del store

        def run_chunk(
            chunk: Any,
            worker_store: FileBackfillStateStore,
            worker_errors: list[str],
            chunk_runner: ChunkRunner | None,
        ) -> bool:
            return self._run_one(
                chunk,
                load_config,
                ledger,
                worker_store,
                worker_errors,
                chunk_runner=chunk_runner,
                coordinator_tick=None,
            )

        errors.extend(
            run_parallel_chunk_lanes(
                pending,
                workers=workers,
                state_store_factory=self._worker_state_store_factory,
                chunk_runner_factory=cast(
                    BackfillWorkerChunkRunnerFactory | None,
                    self._worker_chunk_runner_factory,
                ),
                run_chunk=run_chunk,
                coordinator_tick=coordinator_tick,
            )
        )

    def _run_one(
        self,
        chunk: BackfillChunk,
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: FileBackfillStateStore,
        errors: list[str],
        *,
        chunk_runner: ChunkRunner | None = None,
        coordinator_tick: Callable[[], None] | None = None,
    ) -> bool:
        record = ledger.chunk(chunk.index)
        owner = f"dpone-backfill-chunk:{uuid4().hex}"
        lease_acquired = False
        try:
            chunk_lease_expires_at = lease_expires_at(load_config)
            lease_acquired = store.acquire_chunk_lease(
                ledger.run_key,
                chunk.index,
                owner=owner,
                lease_expires_at=chunk_lease_expires_at,
            )
            if not lease_acquired:
                current = store.load(ledger.run_key) or ledger
                record = current.chunk(chunk.index)
                if current.status == "cancel_requested":
                    errors.append(f"chunk {chunk.index} cancelled before start")
                elif record.status != CHUNK_STATUS_SUCCESS:
                    errors.append(f"chunk {chunk.index} lease is not available")
                return False
            current = store.load(ledger.run_key) or ledger
            record = current.chunk(chunk.index)
            lifecycle_context = BackfillChunkLifecycleContext(
                run_key=ledger.run_key,
                chunk_index=chunk.index,
                owner=owner,
                store=store,
            )
            self._lifecycle.before_heartbeat(lifecycle_context)

            def renew_and_report() -> bool:
                renewed = store.renew_chunk_lease(
                    ledger.run_key,
                    chunk.index,
                    owner=owner,
                    lease_expires_at=lease_expires_at(load_config),
                )
                if renewed:
                    self._log_progress(store, ledger.run_key, event="heartbeat")
                    if coordinator_tick is not None:
                        coordinator_tick()
                return renewed

            heartbeat = self._heartbeat_factory(
                renew_and_report,
                initial_expiry=chunk_lease_expires_at,
                now=lambda: datetime.now(_UTC),
            )
            heartbeat.prove_ownership()
            self._lifecycle.before_chunk_runner(lifecycle_context)
            heartbeat.start()
            try:
                result = (chunk_runner or self._chunk_runner)(
                    self.build_chunk_load_config(
                        load_config,
                        chunk,
                        run_key=ledger.run_key,
                        plan_hash=ledger.plan_hash,
                        owner=owner,
                        lease_expires_at_utc=chunk_lease_expires_at,
                        portable_scope_campaign=CampaignPortableScopeBinding.from_jsonable(
                            ledger.portable_scope_column_contract
                        ),
                    )
                )
            finally:
                heartbeat.stop()
            heartbeat.assert_healthy()
            self._lifecycle.before_ledger_completion(lifecycle_context, result)
        except Exception as exc:
            if not lease_acquired:
                errors.append(f"chunk {chunk.index} lease acquisition failed: {exc}")
                return False
            record.status = CHUNK_STATUS_FAILED
            record.error = str(exc)
            if not store.complete_chunk_if_owned(ledger.run_key, record, owner=owner):
                errors.append(f"DPONE_BACKFILL_CHUNK_LEASE_LOST: chunk {chunk.index} lease owner changed")
                return False
            self._log_progress(store, ledger.run_key, event="chunk_failed")
            errors.append(f"chunk {chunk.index} [{chunk.start}..{chunk.end}]: {exc}")
            return False
        return self._complete_success(record, result, run_key=ledger.run_key, owner=owner, store=store, errors=errors)

    @staticmethod
    def _complete_success(
        record: Any,
        result: Mapping[str, Any],
        *,
        run_key: str,
        owner: str,
        store: FileBackfillStateStore,
        errors: list[str],
    ) -> bool:
        record.status = CHUNK_STATUS_SUCCESS
        record.error = None
        record.finished_at = datetime.now(_UTC).isoformat()
        record.rows_extracted = int(result.get("extracted_rows", 0) or 0)
        record.rows_loaded = int(result.get("loaded_rows", 0) or 0)
        record.run_id = result.get("run_id")
        record.load_id = result.get("load_id")
        record.execution_evidence = project_chunk_execution_evidence(result)
        if store.complete_chunk_if_owned(run_key, record, owner=owner):
            BackfillChunkExecutionMixin._log_progress(store, run_key, event="chunk_committed")
            return True
        errors.append(f"DPONE_BACKFILL_CHUNK_LEASE_LOST: chunk {record.index} lease owner changed")
        return False

    @staticmethod
    def _log_progress(store: FileBackfillStateStore, run_key: str, *, event: str) -> None:
        current = store.load(run_key)
        if current is None:
            return
        progress = backfill_progress(current)
        _LOG.info(
            "event=dpone.backfill_progress transition=%s run_key=%s committed=%s running=%s "
            "failed=%s pending=%s rows_loaded=%s rows_per_second=%s eta_seconds=%s",
            event,
            run_key,
            progress["committed"],
            progress["running"],
            progress["failed"],
            progress["pending"],
            progress["rows_loaded"],
            progress["rows_per_second"],
            progress["eta_seconds"],
        )


__all__ = ["BackfillChunkExecutionMixin", "ChunkRunner"]
