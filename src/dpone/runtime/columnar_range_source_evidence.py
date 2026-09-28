"""Source receipts and deferred cleanup for columnar range execution."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from threading import Lock
from typing import Any, cast

from dpone.ports.columnar_range_parallelism import ColumnarRangeExecutionEvidence, RangeChunkReceipt


def chunk_receipt(chunk: Any, *, ordinal: int) -> RangeChunkReceipt:
    match = re.fullmatch(r"(?:sha256:)?([0-9a-fA-F]{64})", str(getattr(chunk, "sha256", "")))
    if match is None:
        raise ValueError("columnar_range_chunk_checksum_invalid")
    return RangeChunkReceipt(
        ordinal=ordinal,
        object_identity=str(getattr(chunk, "uri", "")),
        checksum_sha256=f"sha256:{match.group(1).lower()}",
        rows=int(getattr(chunk, "row_count", -1)),
        bytes=int(getattr(chunk, "size_bytes", -1)),
    )


def failure_evidence(
    *,
    plan: Any,
    produced: Mapping[int, Sequence[Any]],
    observed_reader_concurrency: int,
    observed_upload_concurrency: int,
    budget: Any | None,
    cancellation_requested: bool,
    cleanup_status: str,
    cleanup_failures: Sequence[str],
) -> Any:
    results = tuple(_result(plan.ranges[index].range_id, windows) for index, windows in sorted(produced.items()))
    factory = cast(Any, ColumnarRangeExecutionEvidence)
    return factory.from_failure(
        plan=plan,
        results=results,
        primary_outcome="failed",
        failure_code="columnar_range_extraction_failed",
        cancellation_requested=cancellation_requested,
        cancellation_observed=cancellation_requested,
        cleanup_status=cleanup_status,
        cleanup_failures=cleanup_failures,
        observed_reader_concurrency=observed_reader_concurrency,
        observed_upload_concurrency=observed_upload_concurrency,
        rows_high_water=budget.rows_high_water if budget else 0,
        bytes_high_water=budget.bytes_high_water if budget else 0,
    )


def success_evidence(*, plan: Any, results: Sequence[Any], reader: int, upload: int, budget: Any) -> Any:
    factory = cast(Any, ColumnarRangeExecutionEvidence)
    return factory.from_results(
        plan=plan,
        results=results,
        observed_reader_concurrency=reader,
        observed_upload_concurrency=upload,
        rows_high_water=budget.rows_high_water,
        bytes_high_water=budget.bytes_high_water,
    )


@dataclass(frozen=True, slots=True)
class _Result:
    range_id: str
    rows: int
    retained_bytes: int
    eof_confirmed: bool
    chunks: tuple[RangeChunkReceipt, ...]


def _result(range_id: str, windows: Sequence[Any]) -> _Result:
    receipts = tuple(
        chunk_receipt(chunk, ordinal=ordinal)
        for ordinal, chunk in enumerate(chunk for window in windows for chunk in window.chunks)
    )
    return _Result(
        range_id,
        sum(int(window.row_count) for window in windows),
        sum(int(window.size_bytes) for window in windows),
        True,
        receipts,
    )


class SerialRunCleanup:
    """Delete a serial run root only after iteration and every window cleanup."""

    def __init__(self, object_client: Any, prefix: Any, *, enabled: bool) -> None:
        self._object_client = object_client
        self._prefix = prefix
        self._enabled = enabled
        self._lock = Lock()
        self._emitted = 0
        self._cleaned = 0
        self._finished = False

    def register(self) -> Callable[[], None]:
        with self._lock:
            self._emitted += 1
        return self._cleaned_one

    def finish(self) -> None:
        with self._lock:
            self._finished = True
            self._cleanup_if_complete()

    def _cleaned_one(self) -> None:
        with self._lock:
            self._cleaned += 1
            self._cleanup_if_complete()

    def _cleanup_if_complete(self) -> None:
        if self._enabled and self._finished and self._cleaned == self._emitted:
            self._object_client.delete_prefix(self._prefix)


__all__ = ["SerialRunCleanup", "chunk_receipt", "failure_evidence", "success_evidence"]


class RangeSourceObservation:
    """Track failure-path measurements that the bounded executor cannot return."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._active = 0
        self.observed = 0
        self.budget: Any | None = None

    def enter(self, budget: Any) -> None:
        self.budget = budget
        with self._lock:
            self._active += 1
            self.observed = max(self.observed, self._active)

    def leave(self) -> None:
        with self._lock:
            self._active -= 1

    def fail(
        self,
        error: BaseException,
        *,
        plan: Any,
        produced: Mapping[int, Sequence[Any]],
        upload_lane: Any,
        object_client: Any,
        prefix: Any,
    ) -> None:
        upload_lane.close(cancel=True)
        cleanup_status = "completed"
        cleanup_failures: tuple[str, ...] = ()
        try:
            object_client.delete_prefix(prefix)
        except BaseException as cleanup_error:
            cleanup_status = "failed"
            cleanup_failures = ("columnar_range_source_cleanup_failed",)
            error.add_note(f"range source cleanup failed: {type(cleanup_error).__name__}")
        evidence = failure_evidence(
            plan=plan,
            produced=produced,
            cleanup_status=cleanup_status,
            cleanup_failures=cleanup_failures,
            observed_reader_concurrency=self.observed,
            observed_upload_concurrency=upload_lane.observed,
            budget=self.budget.snapshot() if self.budget is not None else None,
            cancellation_requested=self.budget is not None,
        )
        setattr(error, "_dpone_range_execution_evidence", evidence)


__all__.append("RangeSourceObservation")
