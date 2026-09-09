"""Parent-owned durable chunk transitions for spawned MSSQL lanes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from dpone.backfill.chunk_lifecycle import (
    BackfillChunkLifecycleContext,
    BackfillChunkLifecyclePort,
)
from dpone.backfill.execution_contract import BACKFILL_OPERATION_SCOPE_OPTION
from dpone.backfill.lease_heartbeat import BackfillLeaseHeartbeat
from dpone.backfill.portable_scope_campaign import CampaignPortableScopeBinding
from dpone.backfill.process_lane_contracts import (
    ProcessLaneBinding,
    ProcessLaneClaimHandoff,
    ProcessLaneDispatch,
)
from dpone.backfill.process_lane_lease import (
    PROCESS_LANE_RENEW_INTERVAL,
    process_lane_lease_expires_at,
)
from dpone.backfill.process_lane_parent import run_process_chunk_lanes
from dpone.backfill.runtime_results import project_chunk_execution_evidence
from dpone.backfill.state import CHUNK_STATUS_FAILED, CHUNK_STATUS_SUCCESS
from dpone.backfill.worker_runtime import BackfillProcessLaneRuntime, parent_control_scope
from dpone.security_redaction import redact_public_text

if TYPE_CHECKING:
    from dpone.backfill.models import BackfillChunk
    from dpone.backfill.state import BackfillLedger, FileBackfillStateStore
    from dpone.config.load_config import LoadConfig

_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.
_LEASE_LOST = "DPONE_BACKFILL_CHUNK_LEASE_LOST"


@dataclass(slots=True)
class _ClaimedChunk:
    chunk: BackfillChunk
    record: Any
    lifecycle: BackfillChunkLifecycleContext
    heartbeat: BackfillLeaseHeartbeat


class BackfillProcessChunkExecution:
    """Serialize ledger I/O in the parent while children own source/sink I/O."""

    def __init__(
        self,
        *,
        runtime: BackfillProcessLaneRuntime,
        lifecycle: BackfillChunkLifecyclePort,
        heartbeat_factory: Callable[..., BackfillLeaseHeartbeat],
        chunk_config_builder: Callable[..., LoadConfig],
        progress_logger: Callable[..., None],
    ) -> None:
        self._runtime = runtime
        self._lifecycle = lifecycle
        self._heartbeat_factory = heartbeat_factory
        self._chunk_config_builder = chunk_config_builder
        self._progress_logger = progress_logger
        self._errors: list[str] = []

    def run(
        self,
        pending: list[BackfillChunk],
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: FileBackfillStateStore,
        *,
        workers: int,
    ) -> list[str]:
        """Run one or more resumable chunks through fixed spawned lanes."""

        self._errors = []
        campaign_binding = CampaignPortableScopeBinding.from_jsonable(ledger.portable_scope_column_contract)
        control_scope = parent_control_scope(self._runtime, store)
        with control_scope:
            summary = run_process_chunk_lanes(
                pending,
                workers=workers,
                runtime=self._runtime.bootstrap,
                claim_chunk=lambda worker_id, chunk, capture: self._claim(
                    worker_id,
                    chunk,
                    capture,
                    load_config=load_config,
                    ledger=ledger,
                    store=store,
                    campaign_binding=campaign_binding,
                ),
                complete_chunk=lambda dispatch, result: self._complete(dispatch, result, store=store),
                fail_chunk=lambda dispatch, error: self._fail(dispatch, error, store=store),
                heartbeat_chunk=lambda dispatch: self._heartbeat(dispatch, store=store),
                heartbeat_interval_seconds=PROCESS_LANE_RENEW_INTERVAL.total_seconds(),
                renew_operation_lease=self._runtime.renew_operation_lease,
                validate_operation_binding=self._runtime.validate_operation_binding,
                issue_receipt_probe=self._runtime.issue_receipt_probe,
                validate_replay_result=self._runtime.validate_replay_result,
                coordinator_tick=self._runtime.coordinator_tick,
                coordinator_reprove=self._runtime.coordinator_reprove,
                recover_committed_receipt=lambda dispatch, operation: self._recover(
                    dispatch,
                    operation,
                    store=store,
                ),
                refresh_dispatch=lambda dispatch: self._refresh_dispatch(dispatch, store=store),
            )
        return [*self._errors, *summary.errors]

    def _claim(
        self,
        worker_id: int,
        chunk: BackfillChunk,
        capture: ProcessLaneClaimHandoff,
        *,
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: FileBackfillStateStore,
        campaign_binding: CampaignPortableScopeBinding | None,
    ) -> None:
        owner = f"dpone-backfill-chunk:{uuid4().hex}"
        expiry = process_lane_lease_expires_at()
        acquired = False
        try:
            acquired = store.acquire_chunk_lease(
                ledger.run_key,
                chunk.index,
                owner=owner,
                lease_expires_at=expiry,
            )
            if not acquired:
                raise RuntimeError(f"backfill.process_chunk_lease_unavailable:{chunk.index}")
            current = store.load(ledger.run_key) or ledger
            record = current.chunk(chunk.index)
            lifecycle = BackfillChunkLifecycleContext(
                run_key=ledger.run_key,
                chunk_index=chunk.index,
                owner=owner,
                store=store,
            )

            def renew_and_report() -> bool:
                return self._renew(
                    ledger.run_key,
                    chunk.index,
                    owner,
                    store=store,
                )

            heartbeat = self._heartbeat_factory(
                renew_and_report,
                initial_expiry=expiry,
                now=lambda: datetime.now(_UTC),
            )
            # Spawned lanes never start a parent-side SQL heartbeat thread.
            # The serial coordinator loop invokes this same policy at every
            # safe point, preserving custom failure/health DI contracts.
            self._require_live_claim(lifecycle, heartbeat)
            self._lifecycle.before_chunk_runner(lifecycle)
            chunk_config = self._chunk_config_builder(
                load_config,
                chunk,
                run_key=ledger.run_key,
                plan_hash=ledger.plan_hash,
                owner=owner,
                lease_expires_at_utc=expiry,
                portable_scope_campaign=campaign_binding,
            )
            dispatch = ProcessLaneDispatch(
                command_id=f"{ledger.run_key}:{chunk.index}:{owner}",
                load_config=chunk_config,
                binding=ProcessLaneBinding(ledger.run_key, chunk.index, owner),
                parent_context=_ClaimedChunk(chunk, record, lifecycle, heartbeat),
            )
            prepare = self._runtime.prepare_dispatch
            prepared = prepare(dispatch) if prepare is not None else dispatch
            self._require_live_claim(lifecycle, heartbeat)
            capture(prepared)
            return
        except BaseException as exc:
            binding = ProcessLaneBinding(ledger.run_key, chunk.index, owner)
            try:
                transferred = capture.owns(binding)
            except BaseException:
                transferred = False
            try:
                error = redact_public_text(
                    exc,
                    fallback="backfill.process_chunk_claim_failed",
                    max_length=512,
                )
            except BaseException:
                error = "backfill.process_chunk_claim_failed"
            if acquired and not transferred:
                try:
                    self._fail_owned_claim(ledger.run_key, chunk.index, owner, error, store=store)
                except BaseException:
                    self._errors.append(f"DPONE_BACKFILL_PROCESS_LEDGER_FAILURE_WRITE_FAILED: chunk {chunk.index}")
            raise

    def _require_live_claim(
        self,
        lifecycle: BackfillChunkLifecycleContext,
        heartbeat: BackfillLeaseHeartbeat,
    ) -> None:
        """Re-prove the exact parent lease around potentially slow preflight."""

        self._lifecycle.before_heartbeat(lifecycle)
        heartbeat.prove_ownership()
        heartbeat.assert_healthy()

    def _refresh_dispatch(
        self,
        dispatch: ProcessLaneDispatch,
        *,
        store: FileBackfillStateStore,
    ) -> ProcessLaneDispatch:
        """Renew the exact owner and issue a fresh finite child deadline."""

        claimed = _claimed(dispatch)
        expiry = process_lane_lease_expires_at()
        self._lifecycle.before_heartbeat(claimed.lifecycle)
        renewed = store.renew_chunk_lease(
            dispatch.binding.run_key,
            dispatch.binding.chunk_index,
            owner=dispatch.binding.owner,
            lease_expires_at=expiry,
        )
        if not renewed:
            raise RuntimeError(_LEASE_LOST)
        self._progress_logger(store, dispatch.binding.run_key, event="heartbeat")
        options = deepcopy(dispatch.load_config.options) if dispatch.load_config.options is not None else {}
        raw_scope = options.get(BACKFILL_OPERATION_SCOPE_OPTION)
        if not isinstance(raw_scope, Mapping):
            raise RuntimeError("backfill.process_lane_operation_scope_missing")
        operation_scope = dict(raw_scope)
        operation_scope["lease_expires_at_utc"] = expiry.isoformat()
        options[BACKFILL_OPERATION_SCOPE_OPTION] = operation_scope
        refreshed = replace(
            dispatch,
            load_config=replace(dispatch.load_config, options=options),
        )
        prepare = self._runtime.prepare_dispatch
        return prepare(refreshed) if prepare is not None else refreshed

    def _heartbeat(
        self,
        dispatch: ProcessLaneDispatch,
        *,
        store: FileBackfillStateStore,
    ) -> bool:
        del store
        heartbeat = _claimed(dispatch).heartbeat
        try:
            heartbeat.prove_ownership()
            heartbeat.assert_healthy()
        except Exception:
            return False
        return True

    def _renew(
        self,
        run_key: str,
        chunk_index: int,
        owner: str,
        *,
        store: FileBackfillStateStore,
    ) -> bool:
        renewed = store.renew_chunk_lease(
            run_key,
            chunk_index,
            owner=owner,
            lease_expires_at=process_lane_lease_expires_at(),
        )
        if renewed:
            self._progress_logger(store, run_key, event="heartbeat")
        return bool(renewed)

    def _complete(
        self,
        dispatch: ProcessLaneDispatch,
        result: Mapping[str, Any],
        *,
        store: FileBackfillStateStore,
    ) -> bool:
        claimed = _claimed(dispatch)
        claimed.heartbeat.assert_healthy()
        self._lifecycle.before_ledger_completion(claimed.lifecycle, result)
        record = claimed.record
        record.status = CHUNK_STATUS_SUCCESS
        record.error = None
        record.finished_at = datetime.now(_UTC).isoformat()
        record.rows_extracted = int(result.get("extracted_rows", 0) or 0)
        record.rows_loaded = int(result.get("loaded_rows", 0) or 0)
        record.run_id = result.get("run_id")
        record.load_id = result.get("load_id")
        record.execution_evidence = project_chunk_execution_evidence(result)
        binding = dispatch.binding
        if store.complete_chunk_if_owned(binding.run_key, record, owner=binding.owner):
            self._progress_logger(store, binding.run_key, event="chunk_committed")
            return True
        self._errors.append(f"{_LEASE_LOST}: chunk {binding.chunk_index} lease owner changed")
        return False

    def _fail(
        self,
        dispatch: ProcessLaneDispatch,
        error: str,
        *,
        store: FileBackfillStateStore,
    ) -> None:
        binding = dispatch.binding
        self._fail_owned_claim(
            binding.run_key,
            binding.chunk_index,
            binding.owner,
            error,
            store=store,
        )

    def _fail_owned_claim(
        self,
        run_key: str,
        chunk_index: int,
        owner: str,
        error: str,
        *,
        store: FileBackfillStateStore,
    ) -> None:
        current = store.load(run_key)
        if current is None:
            self._errors.append(f"DPONE_BACKFILL_PROCESS_LEDGER_MISSING: chunk {chunk_index}")
            return
        record = current.chunk(chunk_index)
        if record.status == CHUNK_STATUS_SUCCESS:
            return
        record.status = CHUNK_STATUS_FAILED
        record.error = error
        if not store.complete_chunk_if_owned(run_key, record, owner=owner):
            self._errors.append(f"{_LEASE_LOST}: chunk {chunk_index} lease owner changed")
            return
        self._progress_logger(store, run_key, event="chunk_failed")

    def _recover(
        self,
        dispatch: ProcessLaneDispatch,
        operation: Any,
        *,
        store: FileBackfillStateStore,
    ) -> bool:
        binding = dispatch.binding
        evidence = self._runtime.receipt_recovery.promote_owned(
            operation,
            store=store,
            run_key=binding.run_key,
            chunk_index=binding.chunk_index,
            owner=binding.owner,
        )
        if evidence is None:
            return False
        self._progress_logger(store, binding.run_key, event="chunk_receipt_recovered")
        return True


def _claimed(dispatch: ProcessLaneDispatch) -> _ClaimedChunk:
    context = dispatch.parent_context
    if not isinstance(context, _ClaimedChunk):
        raise RuntimeError("backfill.process_chunk_parent_context_invalid")
    return context


__all__ = ["BackfillProcessChunkExecution"]
