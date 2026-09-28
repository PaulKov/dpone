"""MSSQL columnar object-storage window builder."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock
from typing import Any

from dpone.runtime.columnar_fast_path_models import ObjectStorageChunk
from dpone.runtime.columnar_object_storage_windows import ObjectStorageChunkWindow
from dpone.runtime.columnar_range_parallelism import (
    AggregateRangeBudget,
    BoundedRangeExecutor,
    RangeExecutionResult,
    RangeSession,
)
from dpone.runtime.columnar_range_source_evidence import (
    RangeSourceObservation,
    SerialRunCleanup,
    chunk_receipt,
    success_evidence,
)
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest
from dpone.runtime.sources.strategies.mssql import mssql_columnar_chunks as chunks
from dpone.runtime.sources.strategies.mssql.mssql_columnar_reader import (
    BoundedColumnarUploadLane,
    ColumnarRangeSession,
    iter_columnar_batches,
)
from dpone.storage import ObjectStorageUri

WriteChunkFile = Callable[[Path, ColumnarSnapshotRequest, Sequence[tuple[object, ...]]], tuple[int, int, str]]


@dataclass(frozen=True, slots=True)
class ParallelWindowResult:
    windows: tuple[ObjectStorageChunkWindow, ...]
    evidence: Any


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
    cleanup_policy = str((request.options or {}).get("cleanup_policy") or "eager")
    cleanup = SerialRunCleanup(object_client, prefix, enabled=cleanup_policy in {"eager", "on_success"})
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
            cleanup_callback=cleanup.register(),
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
            cleanup_callback=cleanup.register(),
        )
        if window is not None:
            yield window
    cleanup.finish()


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
    upload_lane = BoundedColumnarUploadLane(plan.policy.upload_workers)
    produced: dict[int, tuple[ObjectStorageChunkWindow, ...]] = {}
    produced_lock = Lock()
    source_sessions: set[int] = set()
    source_sessions_lock = Lock()
    observation = RangeSourceObservation()

    def session_factory(descriptor: Any) -> ColumnarRangeSession:
        session = connector.open_session(application_name=f"dpone-columnar-range-{descriptor.ordinal:06d}")
        if session is connector or not callable(getattr(session, "close", None)):
            raise RuntimeError("mssql_range_session_not_independent")
        with source_sessions_lock:
            identity = id(session)
            if identity in source_sessions:
                raise RuntimeError("mssql_range_session_reused")
            source_sessions.add(identity)
        return ColumnarRangeSession(session)

    def worker(
        session: RangeSession,
        descriptor: Any,
        cancelled: Event,
        budget: AggregateRangeBudget,
    ) -> RangeExecutionResult:
        observation.enter(budget)
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

        try:
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
        finally:
            observation.leave()
        with produced_lock:
            produced[descriptor.ordinal] = tuple(windows)
        receipts = tuple(
            chunk_receipt(chunk, ordinal=ordinal)
            for ordinal, chunk in enumerate(chunk for window in windows for chunk in window.chunks)
        )
        return RangeExecutionResult(descriptor.range_id, rows_total, bytes_total, eof_confirmed=True, chunks=receipts)

    try:
        summary = BoundedRangeExecutor(session_factory=session_factory, worker=worker).execute(plan)
    except BaseException as error:
        observation.fail(
            error,
            plan=plan,
            produced=produced,
            upload_lane=upload_lane,
            object_client=object_client,
            prefix=prefix,
        )
        raise
    upload_lane.close(cancel=False)
    ordered_windows = tuple(window for ordinal in range(len(partitions)) for window in produced.get(ordinal, ()))
    execution = success_evidence(
        plan=plan,
        results=summary.ranges,
        reader=summary.observed_reader_concurrency,
        upload=upload_lane.observed,
        budget=summary.budget,
    )
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
    upload_guard: BoundedColumnarUploadLane | None = None,
    cleanup_callback: Callable[[], None] | None = None,
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

    def upload() -> Any:
        return object_client.put_file(
            local_path,
            window_prefix.child("chunk-00000.parquet"),
            content_type="application/vnd.apache.parquet",
        )

    uploaded = upload_guard.run(upload) if upload_guard is not None else upload()
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
            cleanup_callback=cleanup_callback,
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
