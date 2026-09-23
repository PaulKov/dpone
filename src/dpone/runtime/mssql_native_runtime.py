"""Compose native staging, prepublication quality, evidence and state in order.

Factories are explicit application boundaries. The binding factory is target-only:
it restores invocation identity and acquires fresh target admission without source
I/O. Evidence/state callbacks must enforce CAS fencing at their durable write.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from threading import Event, Thread
from time import monotonic
from typing import TYPE_CHECKING, Any

from dpone.contracts.bounded_window import WindowContractError, WindowLease
from dpone.contracts.process_types import ProcessResult
from dpone.manifest.mssql_native_policy import native_source_read_mode, native_transport_policy, validate_native_config
from dpone.ports.bounded_window import WindowStore
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.mssql_native_chunks_observations import NativeDeliverySession, delivery_session
from dpone.runtime.sinks.load_result import LoadResult

if TYPE_CHECKING:
    from dpone.ports.native_delivery_observer import NativeDeliveryObserver


@dataclass(frozen=True)
class NativeRuntimeBindings:
    """One target-only composition, reusable through source-free recovery."""

    service: Any
    stage_context: Any
    admission: Any


class NativeMssqlRuntime:
    """Run one stable invocation through an owned sink's staged lifecycle."""

    def __init__(
        self,
        *,
        store: WindowStore,
        target_id: str,
        bindings: Callable[[Any, str, WindowLease, Event], NativeRuntimeBindings],
        source: Callable[[Any, NativeRuntimeBindings], AbstractContextManager[Any]],
        preflight: Callable[[Any], None],
        quality: Callable[[Any, StagedLoadHandle, WindowLease], None],
        evidence: Callable[[Any, LoadResult, Any, WindowLease], None],
        advance_state: Callable[[Any, LoadResult, WindowLease], None],
        lease_ttl: float = 60.0,
        observer: NativeDeliveryObserver | NativeDeliverySession | None = None,
    ) -> None:
        if lease_ttl <= 0:
            raise ValueError("mssql_native.lease_ttl_invalid")
        self.store, self.target_id = store, target_id
        self.bindings, self.source, self.preflight = bindings, source, preflight
        self.quality, self.evidence, self.advance_state = quality, evidence, advance_state
        self.lease_ttl = lease_ttl
        self.observations = delivery_session(observer)

    def run(self, load_config: Any, *, owner: str) -> ProcessResult:
        """Resume target receipts before any source factory; never replay unknown commit."""
        validate_native_config(load_config)
        self.preflight(load_config)
        started = monotonic()
        lease = self.store.acquire(self.target_id, owner, self.lease_ttl)
        stopped, lost = Event(), Event()
        heartbeat = Thread(target=self._renew, args=(lease, stopped, lost), daemon=True)
        heartbeat.start()
        try:
            self._check(lease, lost)
            binding = self.bindings(load_config, owner, lease, lost)
            service, context = binding.service, binding.stage_context
            if context.plan.target_id != self.target_id or context.lease != lease or context.cancelled is not lost:
                raise WindowContractError("mssql_native.binding_lease_mismatch")
            if getattr(context.plan, "source_read_mode", None) != native_source_read_mode(load_config):
                raise WindowContractError("mssql_native.source_policy_mismatch")
            if getattr(context.plan, "transport", None) != native_transport_policy(load_config):
                raise WindowContractError("mssql_native.transport_policy_mismatch")
            recovered = service.resume(load_config, context, binding.admission)
            self._check(lease, lost)
            handle = recovered if isinstance(recovered, StagedLoadHandle) else None
            if recovered is None:
                with self.source(load_config, binding) as payload:
                    handle = service.stage(load_config, payload)
                    result = self._publish(load_config, service, handle, lease, lost)
            elif handle is not None:
                result = self._publish(load_config, service, handle, lease, lost)
            elif isinstance(recovered, LoadResult):
                result = recovered
            else:
                raise WindowContractError("mssql_native.invalid_resume_result")
            self._check(lease, lost)
            journal = context.journal_factory()
            state = journal.publication.state()
            if state is None:
                raise WindowContractError("mssql_native.publication_receipt_missing")
            if self._is_v4(context):
                self._complete_v4(load_config, result, context, lease, lost)
            elif state["phase"] == "published":
                with self.observations.recorder("runtime").phase("evidence"):
                    self.evidence(load_config, result, context, lease)
                    self._check(lease, lost)
                    journal.publication.evidence_complete()
                state = journal.publication.state()
            if not self._is_v4(context) and state["phase"] == "evidence-complete":
                with self.observations.recorder("runtime").phase("checkpoint"):
                    self.advance_state(load_config, result, lease)
                    self._check(lease, lost)
                    journal.publication.succeeded()
            if journal.publication.state()["phase"] != "succeeded":
                raise WindowContractError("mssql_native.incomplete_publication")
            if handle is not None:
                service.cleanup(handle)
            else:
                service.cleanup_recovered(load_config, context, binding.admission)
            return ProcessResult(
                status="success",
                inserted_rows=result.inserted_rows,
                updated_rows=result.updated_rows,
                final_rows=result.total_rows,
                extracted_rows=result.staging_rows or result.inserted_rows,
                duration_seconds=monotonic() - started,
                errors=[],
                details={
                    "native_transport": "mssql_native",
                    "source_opened": recovered is None,
                    "commit_receipt_id": result.commit_receipt_id,
                },
            )
        finally:
            primary = sys.exc_info()[1]
            stopped.set()
            heartbeat.join(timeout=35)
            try:
                if heartbeat.is_alive():
                    raise WindowContractError("mssql_native.lease_renewal_not_settled")
                self.store.release(lease)
            except BaseException as error:
                if primary is None:
                    raise
                primary.add_note(f"native runtime cleanup failed: {type(error).__name__}")

    @staticmethod
    def _is_v4(context: Any) -> bool:
        data = getattr(context.journal_factory(), "data", None)
        return isinstance(data, dict) and data.get("version") == 4

    def _complete_v4(self, config: Any, result: LoadResult, context: Any, lease: WindowLease, lost: Event) -> None:
        """Retire exact SqlClient stages before the only checkpoint acknowledgement."""
        journal = context.journal_factory()
        state = journal.publication.state()
        if state is None:
            raise WindowContractError("mssql_native.publication_receipt_missing")
        if state["phase"] in {
            "published",
            "aborted",
            "retirement_required",
            "retiring",
            "retired",
            "checkpoint_required",
        }:
            if context.settle_parent is None:
                raise WindowContractError("mssql_native.sqlclient_parent_settlement_required")
            with self.observations.recorder("runtime").phase("evidence"):
                self.evidence(config, result, context, lease)
            self._check(lease, lost)
            with self.observations.recorder("runtime").phase("checkpoint"):
                context.settle_parent()
                self._check(lease, lost)

    def _publish(
        self, config: Any, service: Any, handle: StagedLoadHandle, lease: WindowLease, lost: Event
    ) -> LoadResult:
        self._check(lease, lost)
        try:
            with self.observations.recorder("runtime").phase("quality"):
                self.quality(config, handle, lease)
            self._check(lease, lost)
        except BaseException as error:
            try:
                service.abort(handle)
            except BaseException as cleanup:
                error.add_note(f"native prepublication cleanup failed: {type(cleanup).__name__}")
            raise
        # Finalizer owns unknown-outcome classification; never abort after intent.
        with self.observations.recorder("runtime").phase("publish", reason="service_finalize"):
            return service.finalize(config, handle)

    def _check(self, lease: WindowLease, lost: Event) -> None:
        if lost.is_set():
            raise WindowContractError("mssql_native.lease_lost")
        self.store.assert_lease(lease)

    def _renew(self, lease: WindowLease, stopped: Event, lost: Event) -> None:
        while not stopped.wait(self.lease_ttl / 3):
            try:
                self.store.renew(lease, self.lease_ttl)
            except BaseException:
                lost.set()
                return
