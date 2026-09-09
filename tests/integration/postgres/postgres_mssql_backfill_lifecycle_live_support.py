"""Runtime-only lifecycle faults for PostgreSQL→MSSQL backfill acceptance.

The reviewed lifecycle label never enters ``LoadConfig``.  This module uses
the production DI boundary to disturb genuine MSSQL-backed lease transitions
at deterministic crash windows while every source and target operation still
travels through ``DefaultProcessRunner`` and ``ETLProcessor``.
"""

from __future__ import annotations

import json
import os
import signal
import time
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from typing import Any

from dpone.backfill.campaign_lease import BackfillCampaignLease
from dpone.backfill.chunk_lifecycle import (
    BackfillChunkLifecycleContext,
    DefaultBackfillChunkLifecycle,
)
from dpone.backfill.lease_heartbeat import BackfillLeaseHeartbeat
from dpone.backfill.process_lane_contracts import BackfillProcessLaneBootstrap
from dpone.backfill.worker_runtime import BackfillProcessLaneRuntime
from dpone.runtime.etl.backfill_orchestrator import BackfillOrchestrator
from dpone.runtime.etl.backfill_process_runtime import open_backfill_process_lane
from tests.integration.postgres.postgres_mssql_backfill_lease_fault_live_support import (
    mssql_running_journal_rows,
    reacquire_with_transition_trace,
)
from tests.integration.postgres.postgres_mssql_backfill_retry_barrier_live_support import (
    wrap_retry_barrier_runtime,
)

_REVIEWED_CRASH_ENTRYPOINT = (
    "tests.integration.postgres.postgres_mssql_backfill_lifecycle_live_support:open_reviewed_native_crash_process_lane"
)
_CHILD_SIGSEGV = "child_sigsegv"
_PARENT_SIGKILL = "parent_sigkill"
_PARENT_KILL_DEADLINE_SECONDS = 30.0
_SHORT_CAMPAIGN_LEASE_TTL = timedelta(seconds=15)
_SHORT_CAMPAIGN_RENEW_INTERVAL = timedelta(seconds=5)


@dataclass(frozen=True, slots=True)
class ReviewedNativeCrashLanePayload:
    """Spawn-safe test authority wrapping the unmodified production opener."""

    production_payload: Any
    crash_marker: str
    events_path: str
    inject_crash: bool
    crash_mode: str


@dataclass(frozen=True, slots=True)
class ReviewedHardDeathParentPayload:
    """Spawn-safe inputs for one killable public KPO-parent invocation."""

    load_config: Any
    runtime_config: dict[str, Any]
    crash_marker: str
    events_path: str
    invocation_id: str


class ReviewedBackfillLifecycleController(DefaultBackfillChunkLifecycle):
    """Inject exactly the reviewed lifecycle at trusted runtime boundaries."""

    def __init__(
        self,
        lifecycle: str,
        *,
        native_crash_marker: Path | None = None,
        native_crash_events: Path | None = None,
        native_crash_mode: str = _CHILD_SIGSEGV,
        retry_barrier_marker: Path | None = None,
    ) -> None:
        self.lifecycle = lifecycle
        if (native_crash_marker is None) != (native_crash_events is None):
            raise ValueError("reviewed native crash requires both marker and events paths")
        self.native_crash_marker = native_crash_marker
        self.native_crash_events = native_crash_events
        if native_crash_marker is not None and retry_barrier_marker is not None:
            raise ValueError("native crash and retry barrier fault authorities are mutually exclusive")
        self.retry_barrier_marker = retry_barrier_marker
        if native_crash_mode not in {_CHILD_SIGSEGV, _PARENT_SIGKILL}:
            raise ValueError(f"unsupported reviewed native crash mode: {native_crash_mode}")
        self.native_crash_mode = native_crash_mode
        self._first_invocation = True
        self._post_commit_fault_observed = False
        self._events: list[dict[str, Any]] = []
        self._lock = Lock()

    @property
    def events(self) -> tuple[dict[str, Any], ...]:
        with self._lock:
            return tuple(dict(event) for event in self._events)

    def begin_retry(self) -> None:
        """Open the retry only after the post-commit owner-CAS fault occurred."""

        with self._lock:
            native_crash_observed = self.native_crash_marker is not None and self.native_crash_marker.is_file()
            legacy_fault_observed = self.native_crash_marker is None and self._post_commit_fault_observed
            if self.lifecycle != "retry_resume" or not (native_crash_observed or legacy_fault_observed):
                raise AssertionError("retry requires an observed post-commit owner-CAS fault")
            self._first_invocation = False
            self._events.append({"event": "retry_started"})

    def heartbeat_factory(
        self,
        renew: Callable[[], bool],
        *,
        initial_expiry: datetime,
        now: Callable[[], datetime],
    ) -> BackfillLeaseHeartbeat:
        """Return the real heartbeat, optionally rejecting its first live proof."""

        if self.lifecycle != "heartbeat_failure":
            return BackfillLeaseHeartbeat(renew, initial_expiry=initial_expiry, now=now)

        def reject_after_vendor_renewal() -> bool:
            renewed = renew()
            self._record("heartbeat_failure", vendor_renewed=renewed)
            return False

        return BackfillLeaseHeartbeat(
            reject_after_vendor_renewal,
            initial_expiry=initial_expiry,
            now=now,
        )

    def before_heartbeat(self, context: BackfillChunkLifecycleContext) -> None:
        if self.lifecycle == "retry_resume":
            self._record(
                "retry_chunk_selected",
                phase="first_invocation" if self._is_first_invocation() else "retry",
                chunk_index=context.chunk_index,
                owner=context.owner,
            )
            return
        if self.lifecycle != "lease_expiry":
            return
        current = context.store.load(context.run_key)
        if current is None:
            raise AssertionError("lease-expiry fault requires a durable campaign")
        record = current.chunk(context.chunk_index)
        record.lease_expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        context.store.update_chunk(current, record)
        journal_rows = mssql_running_journal_rows(context)
        self._record(
            "lease_expiry_journal_before_reacquire",
            chunk_index=context.chunk_index,
            rows=journal_rows,
        )
        replacement_owner = f"reviewed-expiry-owner:{context.chunk_index}"
        reacquired, transition_trace = reacquire_with_transition_trace(
            context,
            replacement_owner=replacement_owner,
        )
        self._record(
            "lease_reacquire_transition",
            chunk_index=context.chunk_index,
            reacquired=reacquired,
            **transition_trace,
        )
        if not reacquired:
            raise AssertionError("reviewed lease-expiry fault could not reacquire the real vendor lease")
        self._record(
            "lease_expired_and_reacquired",
            chunk_index=context.chunk_index,
            stale_owner=context.owner,
            replacement_owner=replacement_owner,
        )

    def before_chunk_runner(self, context: BackfillChunkLifecycleContext) -> None:
        if self.lifecycle == "worker_failure":
            self._record("worker_failure", chunk_index=context.chunk_index, owner=context.owner)
            raise RuntimeError("DPONE_BACKFILL_WORKER_FAILURE")
        if self.lifecycle == "retry_resume":
            if (
                self.native_crash_marker is None
                and self.retry_barrier_marker is None
                and self._is_first_invocation()
                and context.chunk_index != 1
            ):
                self._record("retry_window_worker_paused", chunk_index=context.chunk_index)
                raise RuntimeError("DPONE_BACKFILL_RETRY_WINDOW_WORKER_PAUSED")
            self._record(
                "retry_worker_started",
                phase="first_invocation" if self._is_first_invocation() else "retry",
                chunk_index=context.chunk_index,
            )

    def before_ledger_completion(
        self,
        context: BackfillChunkLifecycleContext,
        result: dict[str, Any] | Any,
    ) -> None:
        if self.native_crash_marker is not None:
            if self.lifecycle == "retry_resume" and not self._is_first_invocation():
                self._record("retry_worker_result", chunk_index=context.chunk_index)
            return
        if self.lifecycle != "retry_resume" or context.chunk_index != 1:
            if self.lifecycle == "retry_resume" and not self._is_first_invocation():
                self._record("retry_worker_result", chunk_index=context.chunk_index)
            return
        if not self._is_first_invocation():
            metrics = result.get("reconciliation_metrics") if isinstance(result, Mapping) else None
            self._record(
                "source_free_receipt_replay",
                chunk_index=context.chunk_index,
                replay_suppressed=isinstance(metrics, Mapping)
                and metrics.get("mssql_transaction_replay_suppressed") is True,
                reported_extracted_rows=result.get("extracted_rows") if isinstance(result, Mapping) else None,
            )
            self._record("retry_worker_result", chunk_index=context.chunk_index)
            return
        current = context.store.load(context.run_key)
        if current is None:
            raise AssertionError("retry fault requires a durable campaign")
        record = current.chunk(context.chunk_index)
        record.lease_owner = "reviewed-post-commit-stale-owner"
        record.lease_expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        context.store.update_chunk(current, record)
        with self._lock:
            self._post_commit_fault_observed = True
            self._events.append(
                {
                    "event": "target_commit_before_ledger_cas",
                    "chunk_index": context.chunk_index,
                    "stale_owner": context.owner,
                }
            )
        if self.retry_barrier_marker is not None:
            _create_marker(
                self.retry_barrier_marker,
                b"target-commit-observed-before-ledger-cas\n",
            )

    def _is_first_invocation(self) -> bool:
        with self._lock:
            return self._first_invocation

    def _record(self, event: str, **details: Any) -> None:
        with self._lock:
            self._events.append({"event": event, **details})


class ReviewedBackfillOrchestratorFactory:
    """Build a normal orchestrator with one shared reviewed lifecycle port."""

    def __init__(
        self,
        controller: ReviewedBackfillLifecycleController,
        *,
        campaign_lease_factory: Callable[[Any, str], BackfillCampaignLease] | None = None,
    ) -> None:
        self.controller = controller
        self.campaign_lease_factory = campaign_lease_factory

    def __call__(
        self,
        *,
        chunk_runner: Any,
        sink_connector: Any,
        worker_state_store_factory: Any,
        worker_chunk_runner_factory: Any = None,
        portable_scope_column_resolver: Any = None,
        portable_scope_history_proof: Any = None,
        campaign_lifecycle: Any = None,
        target_publisher: Any = None,
    ) -> BackfillOrchestrator:
        if self.controller.native_crash_marker is not None:
            if not isinstance(worker_chunk_runner_factory, BackfillProcessLaneRuntime):
                raise AssertionError("reviewed native crash requires the production process-lane runtime")
            bootstrap = worker_chunk_runner_factory.bootstrap
            worker_chunk_runner_factory = replace(
                worker_chunk_runner_factory,
                bootstrap=BackfillProcessLaneBootstrap(
                    entrypoint=_REVIEWED_CRASH_ENTRYPOINT,
                    payload=ReviewedNativeCrashLanePayload(
                        production_payload=bootstrap.payload,
                        crash_marker=str(self.controller.native_crash_marker),
                        events_path=str(self.controller.native_crash_events),
                        inject_crash=self.controller._is_first_invocation(),
                        crash_mode=self.controller.native_crash_mode,
                    ),
                ),
            )
        elif self.controller.retry_barrier_marker is not None:
            worker_chunk_runner_factory = wrap_retry_barrier_runtime(
                worker_chunk_runner_factory,
                marker=self.controller.retry_barrier_marker,
                inject_barrier=self.controller._is_first_invocation(),
            )
        return BackfillOrchestrator(
            chunk_runner=chunk_runner,
            sink_connector=sink_connector,
            heartbeat_factory=self.controller.heartbeat_factory,
            campaign_lease_factory=self.campaign_lease_factory,
            lifecycle_port=self.controller,
            worker_state_store_factory=worker_state_store_factory,
            worker_chunk_runner_factory=worker_chunk_runner_factory,
            portable_scope_column_resolver=portable_scope_column_resolver,
            portable_scope_history_proof=portable_scope_history_proof,
            campaign_lifecycle=campaign_lifecycle,
            target_publisher=target_publisher,
        )


@contextmanager
def open_reviewed_native_crash_process_lane(worker_id: int, payload: Any, operation_lease_factory: Any):
    """Crash one real child after its receipt and hold peers before source I/O."""

    if not isinstance(payload, ReviewedNativeCrashLanePayload):
        raise RuntimeError("reviewed native crash payload is invalid")
    marker = Path(payload.crash_marker)
    with open_backfill_process_lane(
        worker_id,
        payload.production_payload,
        operation_lease_factory,
    ) as production_run:
        _append_native_event(payload.events_path, "lane_opened", worker_id, -1)

        def run_chunk(load_config: Any) -> Any:
            chunk_index = int(load_config.options["backfill"]["chunk_context"]["index"])
            if payload.inject_crash and chunk_index != 1:
                _append_native_event(payload.events_path, "peer_waiting", worker_id, chunk_index)
                _wait_for_marker(marker)
                if payload.crash_mode == _PARENT_SIGKILL:
                    _wait_for_parent_sigkill(payload.events_path, worker_id, chunk_index)
                _wait_for_native_exit_shutdown(payload.events_path, worker_id, chunk_index)
            result = production_run(load_config)
            metrics = result.get("reconciliation_metrics") if isinstance(result, Mapping) else None
            _append_native_event(
                payload.events_path,
                "production_result",
                worker_id,
                chunk_index,
                replay_suppressed=isinstance(metrics, Mapping)
                and metrics.get("mssql_transaction_replay_suppressed") is True,
            )
            if payload.inject_crash and chunk_index == 1 and _create_crash_marker(marker):
                if payload.crash_mode == _PARENT_SIGKILL:
                    _append_native_event(payload.events_path, "parent_sigkill_ready", worker_id, chunk_index)
                    _wait_for_parent_sigkill(payload.events_path, worker_id, chunk_index)
                _disable_deliberate_crash_core_dump()
                _append_native_event(payload.events_path, "sigsegv_after_receipt", worker_id, chunk_index)
                os.kill(os.getpid(), signal.SIGSEGV)
            return result

        yield run_chunk


def run_reviewed_hard_death_parent(payload: ReviewedHardDeathParentPayload) -> None:
    """Run a genuine public parent until its post-receipt process group is killed."""

    from tests.integration.postgres.postgres_mssql_backfill_orchestration_live_support import (
        invoke_public_process,
    )

    controller = ReviewedBackfillLifecycleController(
        "retry_resume",
        native_crash_marker=Path(payload.crash_marker),
        native_crash_events=Path(payload.events_path),
        native_crash_mode=_PARENT_SIGKILL,
    )
    invocation = invoke_public_process(
        SimpleNamespace(runtime_config=payload.runtime_config),
        payload.load_config,
        invocation_id=payload.invocation_id,
        backfill_orchestrator_factory=ReviewedBackfillOrchestratorFactory(
            controller,
            campaign_lease_factory=reviewed_short_campaign_lease,
        ),
        runtime_config=payload.runtime_config,
    )
    _append_native_event(
        payload.events_path,
        "parent_returned_unexpectedly",
        -1,
        -1,
        error_type=type(invocation.error).__name__ if invocation.error is not None else None,
    )


def reviewed_short_campaign_lease(store: Any, run_key: str) -> BackfillCampaignLease:
    """Inject a short, production-equivalent campaign lease into live smokes."""

    return BackfillCampaignLease(
        store,
        run_key,
        ttl=_SHORT_CAMPAIGN_LEASE_TTL,
        renew_interval=_SHORT_CAMPAIGN_RENEW_INTERVAL,
    )


def _wait_for_marker(marker: Path) -> None:
    deadline = time.monotonic() + _PARENT_KILL_DEADLINE_SECONDS
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    if not marker.exists():
        raise RuntimeError("reviewed parent-kill receipt marker timed out")


def _wait_for_parent_sigkill(events_path: str, worker_id: int, chunk_index: int) -> None:
    """Remain bounded while the outer harness performs whole-pod hard death."""

    deadline = time.monotonic() + _PARENT_KILL_DEADLINE_SECONDS
    while time.monotonic() < deadline:
        time.sleep(0.05)
    _append_native_event(events_path, "parent_sigkill_timeout", worker_id, chunk_index)
    raise RuntimeError("reviewed parent SIGKILL was not delivered within the bounded deadline")


def _wait_for_native_exit_shutdown(events_path: str, worker_id: int, chunk_index: int) -> None:
    """Leave abort ownership with the supervisor until it observes native exit.

    The receipt marker precedes SIGSEGV delivery and OS process termination.
    Raising on that marker races the supervisor's peer-drain deadline against
    the deliberately crashing child. Peers remain before source I/O instead.
    The existing fixture watchdog still detects a supervisor that never stops.
    """
    deadline = time.monotonic() + _PARENT_KILL_DEADLINE_SECONDS
    while time.monotonic() < deadline:
        time.sleep(0.05)
    _append_native_event(events_path, "native_exit_shutdown_timeout", worker_id, chunk_index)
    raise RuntimeError("reviewed native-exit supervisor did not stop peer within the bounded deadline")


def _disable_deliberate_crash_core_dump() -> None:
    """Disable binary dumps only in the child about to inject SIGSEGV.

    Faulthandler output remains enabled. Preserve the inherited hard limit;
    kernel crash-dump collection must not control this fixture's exit timing.
    """
    import resource

    _, hard_limit = resource.getrlimit(resource.RLIMIT_CORE)
    resource.setrlimit(resource.RLIMIT_CORE, (0, hard_limit))


def _create_crash_marker(path: Path) -> bool:
    return _create_marker(path, b"receipt-committed-before-native-exit\n")


def _create_marker(path: Path, content: bytes) -> bool:
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return False
    try:
        os.write(descriptor, content)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return True


def _append_native_event(
    path: str,
    event: str,
    worker_id: int,
    chunk_index: int,
    **details: Any,
) -> None:
    payload = (
        json.dumps(
            {
                "event": event,
                "worker_id": worker_id,
                "chunk_index": chunk_index,
                "pid": os.getpid(),
                **details,
            },
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )
    descriptor = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, payload)
    finally:
        os.close(descriptor)


__all__ = [
    "ReviewedBackfillLifecycleController",
    "ReviewedHardDeathParentPayload",
    "ReviewedBackfillOrchestratorFactory",
    "ReviewedNativeCrashLanePayload",
    "open_reviewed_native_crash_process_lane",
    "reviewed_short_campaign_lease",
    "run_reviewed_hard_death_parent",
]
