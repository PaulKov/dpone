"""Planning, admission, and bounded execution for columnar source ranges."""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from threading import Condition, Event, Lock
from typing import Any, Protocol
from uuid import UUID

import dpone.ports.columnar_range_parallelism as range_contracts
from dpone.runtime.partitioning import RangePartition, RangePartitioner


class RangeSession(Protocol):
    """Minimal independently owned source-session lifecycle."""

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class RangeExecutionResult:
    range_id: str
    rows: int
    retained_bytes: int
    eof_confirmed: bool
    chunks: tuple[range_contracts.RangeChunkReceipt, ...] = ()


@dataclass(frozen=True, slots=True)
class AggregateBudgetSnapshot:
    current_ranges: int
    current_rows: int
    current_bytes: int
    ranges_high_water: int
    rows_high_water: int
    bytes_high_water: int


class _BudgetLease(AbstractContextManager["_BudgetLease"]):
    def __init__(self, budget: AggregateRangeBudget, *, ranges: int, rows: int, retained_bytes: int) -> None:
        self._budget = budget
        self._amounts = (ranges, rows, retained_bytes)
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._budget._release(*self._amounts)

    def __exit__(self, *args: object) -> None:
        self.release()


class AggregateRangeBudget:
    """One aggregate retained-resource budget shared by every reader."""

    def __init__(self, *, max_ranges: int, max_rows: int, max_bytes: int) -> None:
        self.max_ranges = _positive("max_ranges", max_ranges)
        self.max_rows = _positive("max_rows", max_rows)
        self.max_bytes = _positive("max_bytes", max_bytes)
        self._condition = Condition()
        self._current = [0, 0, 0]
        self._high_water = [0, 0, 0]

    def open_range(self, cancelled: Event) -> _BudgetLease:
        return self._acquire(ranges=1, rows=0, retained_bytes=0, cancelled=cancelled)

    def acquire(self, *, rows: int, retained_bytes: int, cancelled: Event | None = None) -> _BudgetLease:
        if rows > self.max_rows:
            raise ValueError("One item exceeds the aggregate row budget.")
        if retained_bytes > self.max_bytes:
            raise ValueError("One item exceeds the aggregate byte budget.")
        return self._acquire(
            ranges=0,
            rows=_nonnegative("rows", rows),
            retained_bytes=_nonnegative("retained_bytes", retained_bytes),
            cancelled=cancelled or Event(),
        )

    def _acquire(self, *, ranges: int, rows: int, retained_bytes: int, cancelled: Event) -> _BudgetLease:
        with self._condition:
            while (
                self._current[0] + ranges > self.max_ranges
                or self._current[1] + rows > self.max_rows
                or self._current[2] + retained_bytes > self.max_bytes
            ):
                if cancelled.is_set():
                    raise RuntimeError("Range execution cancelled while waiting for aggregate budget.")
                self._condition.wait(timeout=0.05)
            if cancelled.is_set():
                raise RuntimeError("Range execution cancelled before aggregate budget acquisition.")
            additions = (ranges, rows, retained_bytes)
            for index, amount in enumerate(additions):
                self._current[index] += amount
                self._high_water[index] = max(self._high_water[index], self._current[index])
        return _BudgetLease(self, ranges=ranges, rows=rows, retained_bytes=retained_bytes)

    def _release(self, ranges: int, rows: int, retained_bytes: int) -> None:
        with self._condition:
            for index, amount in enumerate((ranges, rows, retained_bytes)):
                self._current[index] -= amount
            self._condition.notify_all()

    def snapshot(self) -> AggregateBudgetSnapshot:
        with self._condition:
            return AggregateBudgetSnapshot(*self._current, *self._high_water)


@dataclass(frozen=True, slots=True)
class RangeExecutionSummary:
    ranges: tuple[RangeExecutionResult, ...]
    observed_reader_concurrency: int
    budget: AggregateBudgetSnapshot


class BoundedRangeExecutor:
    """Run deterministic ranges with independent sessions and fail-fast cancellation."""

    def __init__(
        self,
        *,
        session_factory: Callable[[range_contracts.ColumnarRangeDescriptor], RangeSession],
        worker: Callable[
            [RangeSession, range_contracts.ColumnarRangeDescriptor, Event, AggregateRangeBudget], RangeExecutionResult
        ],
        executor_factory: Callable[[int], ThreadPoolExecutor] = ThreadPoolExecutor,
    ) -> None:
        self._session_factory = session_factory
        self._worker = worker
        self._executor_factory = executor_factory

    def execute(self, plan: range_contracts.ColumnarRangePlan) -> RangeExecutionSummary:
        policy = plan.policy
        cancelled = Event()
        budget = AggregateRangeBudget(
            max_ranges=policy.max_inflight_ranges,
            max_rows=policy.max_inflight_rows,
            max_bytes=policy.max_inflight_bytes,
        )
        sessions: list[RangeSession] = []
        active_lock = Lock()
        active = 0
        observed = 0

        try:
            for item in plan.ranges:
                sessions.append(self._session_factory(item))
        except BaseException as error:
            for session in reversed(sessions):
                try:
                    session.close()
                except BaseException as cleanup_error:
                    _note_cleanup_error(error, cleanup_error)
            raise

        def execute_one(
            item: range_contracts.ColumnarRangeDescriptor,
            session: RangeSession,
        ) -> RangeExecutionResult:
            nonlocal active, observed
            if cancelled.is_set():
                raise RuntimeError("Range execution cancelled before source read.")
            with budget.open_range(cancelled):
                with active_lock:
                    active += 1
                    observed = max(observed, active)
                try:
                    result = self._worker(session, item, cancelled, budget)
                    if result.range_id != item.range_id or not result.eof_confirmed:
                        raise ValueError("Range worker must return matching identity and confirmed EOF.")
                    return result
                finally:
                    with active_lock:
                        active -= 1

        futures: dict[Future[RangeExecutionResult], int] = {}
        results: dict[int, RangeExecutionResult] = {}
        pool = self._executor_factory(min(policy.reader_workers, len(plan.ranges)))
        try:
            futures = {
                pool.submit(execute_one, item, sessions[item.ordinal]): item.ordinal for item in plan.ranges
            }
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        except BaseException:
            cancelled.set()
            for session in sessions:
                cancel = getattr(session, "cancel", None)
                if callable(cancel):
                    try:
                        cancel()
                    except BaseException as cleanup_error:
                        _note_cleanup_error(sys.exc_info()[1], cleanup_error)
            for future in futures:
                future.cancel()
            raise
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
            primary = sys.exc_info()[1]
            close_error: BaseException | None = None
            for session in reversed(sessions):
                try:
                    session.close()
                except BaseException as cleanup_error:
                    if primary is not None:
                        _note_cleanup_error(primary, cleanup_error)
                    else:
                        close_error = close_error or cleanup_error
            if close_error is not None:
                raise close_error
        if len(results) != len(plan.ranges):
            raise RuntimeError("Range executor did not complete every planned range.")
        return RangeExecutionSummary(
            tuple(results[index] for index in range(len(results))), observed, budget.snapshot()
        )


class RangeParallelismPreflight:
    """Fail closed before source I/O for unsupported integrity combinations."""

    @staticmethod
    def validate(
        policy: range_contracts.RangeParallelismPolicy,
        *,
        partition_column: str,
        query_has_window_functions: bool,
        window_partition_key: Sequence[str] = (),
        window_policy_verified: bool = False,
        supported_topologies: set[str],
    ) -> None:
        if policy.staging_topology not in supported_topologies:
            raise ValueError(f"Staging topology {policy.staging_topology!r} is not supported by the sink.")
        if policy.group_key and partition_column not in policy.group_key:
            raise ValueError("The partition column must be part of the declared group key.")
        if not query_has_window_functions:
            return
        if not window_policy_verified:
            raise ValueError("Window SQL requires a machine-checkable complete window partition policy.")
        if not policy.group_key or tuple(window_partition_key) != policy.group_key:
            raise ValueError("group_key must equal the complete window partition key.")


def build_columnar_range_plan(
    partitioner: RangePartitioner, *, query_identity: str, execution_identity: str | None = None
) -> range_contracts.ColumnarRangePlan:
    """Project the canonical partitioner into a sanitized immutable plan."""

    policy = partitioner.range_parallelism
    if not isinstance(policy, range_contracts.RangeParallelismPolicy):
        policy = range_contracts.RangeParallelismPolicy.from_mapping(
            {}, reader_workers=partitioner.max_workers, load_workers=partitioner.load_workers
        )
    descriptors: list[range_contracts.ColumnarRangeDescriptor] = []
    for ordinal, partition in enumerate(partitioner.partitions()):
        payload = _range_payload(partition, ordinal=ordinal)
        descriptors.append(
            range_contracts.ColumnarRangeDescriptor(
                range_id=range_contracts.columnar_range_fingerprint(payload),
                ordinal=ordinal,
                boundary_family=partition.boundary.kind.value,
                lower=_bound_digest(partition.lower_bound),
                upper=_bound_digest(partition.upper_bound),
                include_lower=partition.include_lower,
                include_upper=partition.include_upper,
                is_null=partition.is_null_partition,
            )
        )
    logical_identity = range_contracts.columnar_range_fingerprint(
        {"source_schema_identity": query_identity, "partition_column": partitioner.column}
    )
    return range_contracts.ColumnarRangePlan.create(
        policy=policy,
        ranges=descriptors,
        query_identity=logical_identity,
        execution_identity=execution_identity,
    )


def _range_payload(partition: RangePartition, *, ordinal: int) -> dict[str, Any]:
    return {
        "ordinal": ordinal,
        "boundary_family": partition.boundary.kind.value,
        "lower_sha256": _bound_digest(partition.lower_bound),
        "upper_sha256": _bound_digest(partition.upper_bound),
        "include_lower": partition.include_lower,
        "include_upper": partition.include_upper,
        "is_null": partition.is_null_partition,
        "include_nulls": partition.include_nulls,
    }


def _bound_digest(value: object) -> str | None:
    if value is None:
        return None
    typed: Any
    if isinstance(value, datetime | date):
        typed = value.isoformat()
    elif isinstance(value, Decimal | UUID):
        typed = str(value)
    else:
        typed = value
    return range_contracts.columnar_range_fingerprint({"value": typed, "python_type": type(value).__name__})


def _positive(name: str, value: object) -> int:
    parsed = _nonnegative(name, value)
    if parsed == 0:
        raise ValueError(f"{name} must be greater than zero.")
    return parsed


def _nonnegative(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer.")
    if value < 0:
        raise ValueError(f"{name} must not be negative.")
    return value


def _note_cleanup_error(primary: BaseException | None, cleanup: BaseException) -> None:
    if primary is not None and hasattr(primary, "add_note"):
        primary.add_note(f"Session cancellation also failed: {type(cleanup).__name__}: {cleanup}")


__all__ = [
    "AggregateRangeBudget",
    "BoundedRangeExecutor",
    "RangeExecutionResult",
    "RangeExecutionSummary",
    "RangeParallelismPreflight",
    "build_columnar_range_plan",
]
