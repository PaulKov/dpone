"""MSSQL columnar object-storage window builder."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock, Semaphore
from typing import Any

from dpone.runtime.columnar_fast_path_models import ObjectStorageChunk
from dpone.runtime.columnar_object_storage_windows import ObjectStorageChunkWindow
from dpone.runtime.columnar_range_parallelism import (
    AggregateRangeBudget,
    BoundedRangeExecutor,
    RangeExecutionResult,
    RangeSession,
)
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest
from dpone.runtime.sources.strategies.mssql import mssql_columnar_chunks as chunks
from dpone.runtime.sources.strategies.mssql.mssql_columnar_reader import (
    ColumnarRangeSession,
    iter_columnar_batches,
)
from dpone.storage import ObjectStorageUri

WriteChunkFile = Callable[[Path, ColumnarSnapshotRequest, Sequence[tuple[object, ...]]], tuple[int, int, str]]


@dataclass(frozen=True, slots=True)
class ParallelWindowResult:
    windows: tuple[ObjectStorageChunkWindow, ...]
    evidence: dict[str, object]


class _ObservedUploadLane:
    def __init__(self, workers: int) -> None:
        self._semaphore = Semaphore(workers)
        self._lock = Lock()
        self._active = 0
        self.observed = 0

    def __enter__(self) -> _ObservedUploadLane:
        self._semaphore.acquire()
        with self._lock:
            self._active += 1
            self.observed = max(self.observed, self._active)
        return self

    def __exit__(self, *args: object) -> None:
        with self._lock:
            self._active -= 1
        self._semaphore.release()


def iter_serial_object_windows(
    *,
    connector: Any,
    request: ColumnarSnapshotRequest,
    prefix: ObjectStorageUri,
    schema_hash: str,
    object_client: Any,
    write_chunk_file: WriteChunkFile,
    read_contract: Any,
    tmp_dir: Path,
):
    row_target = chunks.initial_chunk_rows(request)
    buffered_rows: list[tuple[object, ...]] = []
    window_index = 0
    for rows in iter_columnar_batches(
        connector,
        query=request.query,
        schema=request.schema,
        batch_size=chunks.batch_size(request),
    ):
        buffered_rows.extend(rows)
        if len(buffered_rows) < row_target:
            continue
        window, row_target = build_object_window(
            tmp_dir=tmp_dir,
            index=window_index,
            rows=buffered_rows,
            request=request,
            prefix=prefix,
            schema_hash=schema_hash,
            object_client=object_client,
            write_chunk_file=write_chunk_file,
            read_contract=read_contract,
        )
        buffered_rows = []
        window_index += 1
        if window is not None:
            yield window
    if buffered_rows:
        window, _ = build_object_window(
            tmp_dir=tmp_dir,
            index=window_index,
            rows=buffered_rows,
            request=request,
            prefix=prefix,
            schema_hash=schema_hash,
            object_client=object_client,
            write_chunk_file=write_chunk_file,
            read_contract=read_contract,
        )
        if window is not None:
            yield window


def build_parallel_object_windows(
    *,
    connector: Any,
    request: ColumnarSnapshotRequest,
    prefix: ObjectStorageUri,
    schema_hash: str,
    object_client: Any,
    write_chunk_file: WriteChunkFile,
    read_contract: Any,
    tmp_dir: Path,
) -> ParallelWindowResult:
    partitioner = request.range_partitioner
    plan = request.range_plan
    if partitioner is None or plan is None:
        raise ValueError("A canonical range partitioner and plan are required.")
    partitions = tuple(partitioner.partitions())
    if len(partitions) != len(plan.ranges):
        raise ValueError("Canonical range plan does not match the partitioner.")
    upload_lane = _ObservedUploadLane(plan.policy.upload_workers)
    produced: dict[int, tuple[ObjectStorageChunkWindow, ...]] = {}
    produced_lock = Lock()

    def session_factory(descriptor: Any) -> ColumnarRangeSession:
        session = connector.open_session(application_name=f"dpone-columnar-range-{descriptor.ordinal:06d}")
        return ColumnarRangeSession(session)

    def worker(
        session: RangeSession,
        descriptor: Any,
        cancelled: Event,
        budget: AggregateRangeBudget,
    ) -> RangeExecutionResult:
        partition = partitions[descriptor.ordinal]
        windows: list[ObjectStorageChunkWindow] = []
        rows_total = 0
        bytes_total = 0
        range_dir = tmp_dir / f"range-{descriptor.ordinal:06d}"
        range_dir.mkdir(parents=True, exist_ok=True)

        def flush(rows: Sequence[tuple[object, ...]]) -> None:
            nonlocal rows_total, bytes_total
            if not rows:
                return
            window, _ = build_object_window(
                tmp_dir=range_dir,
                index=len(windows),
                rows=rows,
                request=request,
                prefix=prefix,
                schema_hash=schema_hash,
                object_client=object_client,
                write_chunk_file=write_chunk_file,
                read_contract=read_contract,
                range_id=descriptor.range_id,
                range_ordinal=descriptor.ordinal,
                upload_guard=upload_lane,
            )
            if window is not None:
                windows.append(window)
                rows_total += window.row_count
                bytes_total += window.size_bytes

        batch_size = chunks.batch_size(request)
        batches = iter(
            iter_columnar_batches(
                session,
                query=request.query,
                schema=request.schema,
                batch_size=batch_size,
                partitioner=partitioner,
                partition=partition,
            )
        )
        while True:
            if cancelled.is_set():
                raise RuntimeError("Range execution cancelled during source read.")
            with budget.acquire(rows=batch_size, retained_bytes=0, cancelled=cancelled):
                try:
                    rows = next(batches)
                except StopIteration:
                    break
                if len(rows) > batch_size:
                    raise ValueError("Source batch exceeds its declared row reservation.")
                with budget.acquire(rows=0, retained_bytes=request.max_chunk_bytes, cancelled=cancelled):
                    flush(rows)
        with produced_lock:
            produced[descriptor.ordinal] = tuple(windows)
        return RangeExecutionResult(descriptor.range_id, rows_total, bytes_total, eof_confirmed=True)

    summary = BoundedRangeExecutor(session_factory=session_factory, worker=worker).execute(plan)
    ordered_windows = tuple(window for ordinal in range(len(partitions)) for window in produced.get(ordinal, ()))
    execution = {
        "schema": "dpone.native_transfer.columnar_range_parallelism.v1",
        "plan_fingerprint": plan.plan_fingerprint,
        "policy_fingerprint": plan.policy.fingerprint,
        "ranges": [
            {
                "range_id": item.range_id,
                "rows": item.rows,
                "retained_bytes": item.retained_bytes,
                "eof_confirmed": item.eof_confirmed,
            }
            for item in summary.ranges
        ],
        "observed_reader_concurrency": summary.observed_reader_concurrency,
        "rows_high_water": summary.budget.rows_high_water,
        "bytes_high_water": summary.budget.bytes_high_water,
        "rows_high_water_kind": "declared_batch_reservation",
        "bytes_high_water_kind": "configured_max_chunk_reservation",
        "all_ranges_confirmed": True,
        "observed_upload_concurrency": upload_lane.observed,
        "requested_upload_concurrency": plan.policy.upload_workers,
        "byte_measurement_scope": "preencode_max_chunk_reservation_and_postencode_actual",
        "byte_reservation_per_item": request.max_chunk_bytes,
        "row_reservation_per_item": chunks.batch_size(request),
        "actual_rows": sum(item.rows for item in summary.ranges),
        "actual_encoded_bytes": sum(item.retained_bytes for item in summary.ranges),
        "rss_bounded": False,
    }
    return ParallelWindowResult(ordered_windows, execution)


def build_object_window(
    *,
    tmp_dir: Path,
    index: int,
    rows: Sequence[tuple[object, ...]],
    request: ColumnarSnapshotRequest,
    prefix: ObjectStorageUri,
    schema_hash: str,
    object_client: Any,
    write_chunk_file: WriteChunkFile,
    read_contract: Any,
    source_read_seconds: float = 0.0,
    clock: Callable[[], float] | None = None,
    range_id: str | None = None,
    range_ordinal: int = 0,
    upload_guard: Any | None = None,
) -> tuple[ObjectStorageChunkWindow | None, int]:
    local_path = tmp_dir / f"range-{range_ordinal:06d}-window-{index:06d}-chunk-00000.parquet"
    write_started = _now(clock)
    row_count, size_bytes, digest = write_chunk_file(local_path, request, rows)
    write_finished = _now(clock)
    if row_count == 0:
        return None, chunks.next_chunk_rows(request, chunks.initial_chunk_rows(request), row_count, size_bytes)
    if range_id is None:
        window_prefix = prefix.child(f"window-{index + 1:06d}").prefix()
    else:
        window_prefix = prefix.child(f"range-{range_ordinal:06d}").child(f"window-{index + 1:06d}").prefix()
    upload_started = write_finished
    with upload_guard or nullcontext():
        uploaded = object_client.put_file(
            local_path,
            window_prefix.child("chunk-00000.parquet"),
            content_type="application/vnd.apache.parquet",
        )
    upload_finished = _now(clock)
    cleanup_started = upload_finished
    local_path.unlink(missing_ok=True)
    cleanup_finished = _now(clock)
    producer_metrics = _producer_metrics(
        index=index + 1,
        row_count=row_count,
        size_bytes=uploaded.size_bytes,
        source_read_seconds=source_read_seconds,
        parquet_write_seconds=_elapsed(write_started, write_finished),
        object_upload_seconds=_elapsed(upload_started, upload_finished),
        local_cleanup_seconds=_elapsed(cleanup_started, cleanup_finished),
    )
    object_chunk = ObjectStorageChunk(
        uri=uploaded.uri,
        index=index,
        row_count=row_count,
        size_bytes=uploaded.size_bytes,
        sha256=uploaded.sha256 or digest,
        schema_hash=schema_hash,
    )
    return (
        ObjectStorageChunkWindow(
            uri_prefix=str(window_prefix),
            columns=tuple(column for column, _ in request.schema),
            chunks=(object_chunk,),
            read_contract=read_contract,
            schema_hash=schema_hash,
            format=request.format,
            object_client=object_client,
            cleanup_policy=str((request.options or {}).get("cleanup_policy") or "eager"),
            estimated_rows=row_count,
            producer_metrics=producer_metrics,
            range_id=range_id,
            range_ordinal=range_ordinal,
            range_window_ordinal=index,
        ),
        chunks.next_chunk_rows(request, len(rows), row_count, size_bytes),
    )


def upload_manifest_chunk(
    *,
    tmp_dir: Path,
    index: int,
    rows: Sequence[tuple[object, ...]],
    request: ColumnarSnapshotRequest,
    prefix: ObjectStorageUri,
    schema_hash: str,
    object_client: Any,
    write_chunk_file: WriteChunkFile,
    uploaded_chunks: list[ObjectStorageChunk],
) -> int:
    local_path = tmp_dir / f"chunk-{index:05d}.parquet"
    row_count, size_bytes, digest = write_chunk_file(local_path, request, rows)
    if row_count == 0:
        return chunks.next_chunk_rows(request, chunks.initial_chunk_rows(request), row_count, size_bytes)
    uploaded = object_client.put_file(
        local_path,
        prefix.child(local_path.name),
        content_type="application/vnd.apache.parquet",
    )
    local_path.unlink(missing_ok=True)
    uploaded_chunks.append(
        ObjectStorageChunk(
            uri=uploaded.uri,
            index=index,
            row_count=row_count,
            size_bytes=uploaded.size_bytes,
            sha256=uploaded.sha256 or digest,
            schema_hash=schema_hash,
        )
    )
    return chunks.next_chunk_rows(request, len(rows), row_count, size_bytes)


def _producer_metrics(
    *,
    index: int,
    row_count: int,
    size_bytes: int,
    source_read_seconds: float,
    parquet_write_seconds: float,
    object_upload_seconds: float,
    local_cleanup_seconds: float,
) -> dict[str, object]:
    total_seconds = source_read_seconds + parquet_write_seconds + object_upload_seconds + local_cleanup_seconds
    return {
        "schema_version": "dpone.native_transfer.columnar_window_producer_metrics.v1",
        "window_index": index,
        "row_count": row_count,
        "size_bytes": size_bytes,
        "source_read_seconds": source_read_seconds,
        "parquet_write_seconds": parquet_write_seconds,
        "object_upload_seconds": object_upload_seconds,
        "local_cleanup_seconds": local_cleanup_seconds,
        "total_seconds": total_seconds,
    }


def _now(clock: Callable[[], float] | None) -> float:
    return float(clock()) if clock is not None else 0.0


def _elapsed(started: float, finished: float) -> float:
    return max(0.0, finished - started)


__all__ = [
    "ParallelWindowResult",
    "build_object_window",
    "build_parallel_object_windows",
    "iter_serial_object_windows",
    "upload_manifest_chunk",
]
