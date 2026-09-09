"""MSSQL columnar object-storage window builder."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from dpone.runtime.columnar_fast_path_models import ObjectStorageChunk
from dpone.runtime.columnar_object_storage_windows import ObjectStorageChunkWindow
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest
from dpone.runtime.sources.strategies.mssql import mssql_columnar_chunks as chunks
from dpone.storage import ObjectStorageUri

WriteChunkFile = Callable[[Path, ColumnarSnapshotRequest, Sequence[tuple[object, ...]]], tuple[int, int, str]]


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
) -> tuple[ObjectStorageChunkWindow | None, int]:
    local_path = tmp_dir / f"window-{index:06d}-chunk-00000.parquet"
    write_started = _now(clock)
    row_count, size_bytes, digest = write_chunk_file(local_path, request, rows)
    write_finished = _now(clock)
    if row_count == 0:
        return None, chunks.next_chunk_rows(request, chunks.initial_chunk_rows(request), row_count, size_bytes)
    window_prefix = prefix.child(f"window-{index + 1:06d}").prefix()
    upload_started = write_finished
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


__all__ = ["build_object_window", "upload_manifest_chunk"]
