"""Compose the bounded executor without bypassing capability admission.

Source contexts own snapshot lifetime. The target factory must supply verified
exclusive-writer authority; a manifest boolean never substitutes for that port.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager
from time import monotonic
from typing import Any, Protocol

from dpone.adapters.bounded_window_journal import WindowInvocationRegistry, WindowJournal
from dpone.contracts.bounded_window import (
    WindowChunk,
    WindowContractError,
    WindowLease,
    WindowPlan,
    WindowResult,
    invocation_fingerprint,
)
from dpone.contracts.process_types import ProcessResult
from dpone.contracts.rolling_window import FrozenRollingWindow, RollingWindowSpec
from dpone.ports.bounded_window import WindowExecutor, WindowSource, WindowStore
from dpone.runtime.rolling_window_admission import validate_window_admission


class VersionedWindowSource(WindowSource, Protocol):
    """A source context exports identity only while its consistency scope is live."""

    source_version: str
    parameters_fingerprint: str


class RollingWindowRuntime:
    """Run a compiled rolling manifest through injected, admitted I/O capabilities.

    The executor factory binds target exclusion, store, durable evidence and source
    state at the composition root. There is no connector lookup or hidden client.
    """

    def __init__(
        self,
        *,
        source_factory: Callable[[FrozenRollingWindow], AbstractContextManager[VersionedWindowSource]],
        executor_factory: Callable[[VersionedWindowSource], WindowExecutor],
        route_id: str,
        target_id: str,
        schema_fingerprint: str,
        generation_total: Callable[[WindowPlan, WindowResult], int],
        store: WindowStore,
        lease_ttl: float = 60.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._source_factory = source_factory
        self._executor_factory = executor_factory
        self._route_id, self._target_id = route_id, target_id
        self._schema_fingerprint, self._clock = schema_fingerprint, clock
        self._generation_total = generation_total
        self._store, self._lease_ttl = store, lease_ttl

    def run(self, load_config: Any, *, owner: str) -> ProcessResult:
        """Resolve the existing interval contract once; never read wall-clock time."""
        validate_window_admission(load_config)
        started = self._clock()
        options = load_config.options or {}
        spec = RollingWindowSpec.from_mapping(options.get("rolling_window"))
        interval = options.get("interval")
        if not isinstance(interval, Mapping):
            raise ValueError("rolling_window_interval_required: provide the existing runtime interval context")
        frozen = spec.freeze({"data_interval_end": interval.get("interval_end")})
        workers = _parallelism(options)
        # route_id must identify immutable, sanitized source query configuration.
        request = invocation_fingerprint(
            self._route_id, self._target_id, self._schema_fingerprint, frozen.fingerprint, workers
        )
        lease = self._store.acquire(self._target_id, owner, self._lease_ttl)
        try:
            registry = WindowInvocationRegistry(self._store, lease, owner, request)
            saved = registry.load()
            if saved is not None:
                self._assert_request_plan(saved, frozen, workers)
                state = WindowJournal(self._store, lease, saved.run_id).read("run")
                if state is not None and state["phase"] != "planned":
                    source = UnavailableWindowSource(saved)
                    result = self._executor_factory(source).execute_leased(saved, lease)
                    return _result(result, frozen, self._clock() - started, self._generation_total(saved, result))
            return self._extract(frozen, workers, lease, registry, saved, started)
        finally:
            primary = sys.exc_info()[1]
            try:
                self._store.release(lease)
            except BaseException as error:
                _cleanup_error(primary, error)

    def _assert_request_plan(self, plan: WindowPlan, frozen: FrozenRollingWindow, workers: int) -> None:
        if (
            plan.route_id != self._route_id
            or plan.target_id != self._target_id
            or plan.schema_fingerprint != self._schema_fingerprint
            or plan.window_column != frozen.column
            or plan.workers != workers
            or plan.start != frozen.start
            or plan.end != frozen.end
            or plan.boundaries != frozen.boundaries
        ):
            raise WindowContractError("rolling_window_registry_request_plan_mismatch")

    def _extract(
        self,
        frozen: FrozenRollingWindow,
        workers: int,
        lease: WindowLease,
        registry: WindowInvocationRegistry,
        saved: WindowPlan | None,
        started: float,
    ) -> ProcessResult:
        try:
            context = self._source_factory(frozen)
            source = context.__enter__()
        except Exception as error:
            if saved is not None:
                raise WindowContractError(
                    "rolling_window_snapshot_unavailable: full re-extraction requires a new invocation"
                ) from error
            raise
        try:
            plan = WindowPlan(
                self._route_id,
                self._target_id,
                frozen.start,
                frozen.end,
                frozen.boundaries,
                self._schema_fingerprint,
                source.source_version,
                source.parameters_fingerprint,
                workers,
                window_column=frozen.column,
            )
            if saved is not None and saved.run_id != plan.run_id:
                raise WindowContractError(
                    "rolling_window_snapshot_changed: full re-extraction requires a new invocation"
                )
            if saved is not None:
                try:
                    source.validate(plan)
                except Exception as error:
                    raise WindowContractError(
                        "rolling_window_snapshot_unavailable: full re-extraction requires a new invocation"
                    ) from error
            else:
                registry.create(plan)
            result = self._executor_factory(source).execute_leased(plan, lease)
            return _result(result, frozen, self._clock() - started, self._generation_total(plan, result))
        finally:
            primary = sys.exc_info()[1]
            try:
                context.__exit__(*sys.exc_info())
            except BaseException as error:
                _cleanup_error(primary, error)


def _cleanup_error(primary: BaseException | None, cleanup: BaseException) -> None:
    if primary is None:
        raise cleanup
    add_note = getattr(primary, "add_note", None)
    if add_note is not None:
        add_note(f"Cleanup also failed: {type(cleanup).__name__}: {cleanup}")


def _parallelism(options: Mapping[str, Any]) -> int:
    native = options.get("native_transfer") or {}
    if not isinstance(native, Mapping):
        raise ValueError("rolling_window_native_transfer_invalid")
    execution = native.get("execution") or {}
    if not isinstance(execution, Mapping):
        raise ValueError("rolling_window_execution_invalid")
    chunking = execution.get("chunking") or {}
    if not isinstance(chunking, Mapping) or set(chunking) - {"mode", "parallelism", "checkpointing"}:
        raise ValueError("rolling_window_chunking_invalid")
    if chunking.get("mode", "bounded_window") != "bounded_window":
        raise ValueError("rolling_window_chunking_mode_unsupported")
    if chunking.get("checkpointing", "resumable") != "resumable":
        raise ValueError("rolling_window_checkpointing_unsupported")
    value = chunking.get("parallelism", 1)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 64:
        raise ValueError("rolling_window_parallelism_invalid: require integer 1..64")
    return value


def _result(result: WindowResult, frozen: FrozenRollingWindow, duration: float, total: int) -> ProcessResult:
    rows = sum(receipt.row_count for receipt in result.receipts)
    if isinstance(total, bool) or not isinstance(total, int) or total < rows:
        raise ValueError("rolling_window_verified_total_invalid")
    return ProcessResult(
        status="success",
        inserted_rows=rows,
        updated_rows=0,
        final_rows=total,
        extracted_rows=rows,
        duration_seconds=duration,
        errors=[],
        details={
            "rolling_window": {
                **frozen.to_dict(),
                "fingerprint": frozen.fingerprint,
                "run_id": result.run_id,
                "generation": result.generation,
                "chunks": len(result.receipts),
                "window_rows": rows,
            }
        },
    )


class UnavailableWindowSource:
    """Restore source identity without reopening an expired snapshot after publication."""

    def __init__(self, plan: WindowPlan) -> None:
        self.source_version = plan.source_version
        self.parameters_fingerprint = plan.parameters_fingerprint

    def validate(self, plan: WindowPlan) -> None:
        raise WindowContractError("rolling_window_source_unavailable_for_reextraction")

    def read(self, plan: WindowPlan, chunk: WindowChunk) -> Iterator[tuple[object, ...]]:
        raise WindowContractError("rolling_window_source_unavailable_for_reextraction")
