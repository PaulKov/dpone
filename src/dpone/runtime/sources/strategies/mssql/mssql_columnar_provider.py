from __future__ import annotations

import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dpone.runtime.columnar_fast_path_models import (
    LocalColumnarChunk,
    LocalColumnarStagingManifest,
    ObjectStorageChunk,
    ObjectStorageStagingManifest,
)
from dpone.runtime.columnar_object_storage_windows import ObjectStorageChunkWindow
from dpone.runtime.columnar_parquet_writer import ParquetChunkWriter, PyArrowParquetChunkWriter
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotCapability, ColumnarSnapshotRequest
from dpone.runtime.object_storage_access_models import ObjectStorageReadContract, read_contract_from_options
from dpone.runtime.sources.strategies.mssql import mssql_columnar_chunks as chunks
from dpone.runtime.sources.strategies.mssql.mssql_columnar_markers import write_columnar_run_marker
from dpone.runtime.sources.strategies.mssql.mssql_columnar_reader import iter_columnar_batches
from dpone.runtime.sources.strategies.mssql.mssql_columnar_schema import (
    read_contract_options as _read_contract_options,
)
from dpone.runtime.sources.strategies.mssql.mssql_columnar_schema import (
    schema_blockers as _schema_blockers,
)
from dpone.runtime.sources.strategies.mssql.mssql_columnar_windows import build_object_window, upload_manifest_chunk
from dpone.storage import ObjectStorageUri
from dpone.storage.protocols import ObjectStorageClient


class MssqlColumnarSnapshotProvider:
    """Write MSSQL streaming row batches as Parquet chunks in object storage."""

    provider_id = "mssql_odbc_arrow_parquet"

    def __init__(
        self,
        *,
        connector: Any,
        object_client: ObjectStorageClient | None,
        parquet_writer: ParquetChunkWriter | None = None,
        read_contract: ObjectStorageReadContract | None = None,
        temp_dir: str | Path | None = None,
    ) -> None:
        self._connector = connector
        self._object_client = object_client
        self._parquet_writer = parquet_writer or PyArrowParquetChunkWriter()
        self._read_contract = read_contract
        self._temp_dir = Path(temp_dir) if temp_dir else None

    def capabilities(self, request: ColumnarSnapshotRequest) -> ColumnarSnapshotCapability:
        blockers: list[str] = []
        if not hasattr(self._connector, "get_records_streaming"):
            blockers.append("mssql_streaming_reader_unavailable")
        if not self._parquet_writer.is_available():
            blockers.append("parquet_writer_unavailable")
        if request.format.lower() != "parquet":
            blockers.append("columnar_format_not_supported")
        blockers.extend(_schema_blockers(request.schema))
        return ColumnarSnapshotCapability(
            provider_id=self.provider_id,
            certified=not blockers,
            blockers=tuple(blockers),
        )

    def snapshot(self, request: ColumnarSnapshotRequest) -> ObjectStorageStagingManifest:
        capability = self.capabilities(request)
        if not capability.supports(request):
            raise RuntimeError(", ".join(capability.blockers or ("mssql_columnar_provider_uncertified",)))
        if self._object_client is None:
            raise RuntimeError("object_storage_client_missing")

        prefix = ObjectStorageUri.parse(request.uri_prefix.format(run_id=request.run_id)).prefix()
        uploaded_chunks: list[ObjectStorageChunk] = []
        schema_hash = chunks.schema_hash(request.schema)
        row_target = chunks.initial_chunk_rows(request)
        buffered_rows: list[tuple[object, ...]] = []
        chunk_index = 0
        try:
            write_columnar_run_marker(self._object_client, prefix, request)
            with tempfile.TemporaryDirectory(prefix="dpone-mssql-columnar-", dir=self._temp_dir) as tmp_dir:
                for rows in self._iter_batches(request):
                    buffered_rows.extend(rows)
                    if len(buffered_rows) < row_target:
                        continue
                    row_target = upload_manifest_chunk(
                        tmp_dir=Path(tmp_dir),
                        index=chunk_index,
                        rows=buffered_rows,
                        request=request,
                        prefix=prefix,
                        schema_hash=schema_hash,
                        object_client=self._object_client,
                        write_chunk_file=self._write_chunk_file_for_window,
                        uploaded_chunks=uploaded_chunks,
                    )
                    buffered_rows = []
                    chunk_index += 1
                if buffered_rows:
                    upload_manifest_chunk(
                        tmp_dir=Path(tmp_dir),
                        index=chunk_index,
                        rows=buffered_rows,
                        request=request,
                        prefix=prefix,
                        schema_hash=schema_hash,
                        object_client=self._object_client,
                        write_chunk_file=self._write_chunk_file_for_window,
                        uploaded_chunks=uploaded_chunks,
                    )
        except Exception:
            self._object_client.delete_prefix(prefix)
            raise

        return ObjectStorageStagingManifest(
            uri_prefix=str(prefix),
            columns=tuple(column for column, _ in request.schema),
            chunks=tuple(uploaded_chunks),
            read_contract=self._resolve_read_contract(request),
            schema_hash=schema_hash,
            format=request.format,
            object_client=self._object_client,
            cleanup_policy=str((request.options or {}).get("cleanup_policy") or "eager"),
            estimated_rows=sum(chunk.row_count for chunk in uploaded_chunks),
        )

    def snapshot_local(self, request: ColumnarSnapshotRequest) -> LocalColumnarStagingManifest:
        capability = self.capabilities(request)
        if not capability.supports(request):
            raise RuntimeError(", ".join(capability.blockers or ("mssql_columnar_provider_uncertified",)))

        base_dir = Path(
            tempfile.mkdtemp(
                prefix=f"dpone-mssql-columnar-{chunks.safe_run_id(request.run_id)}-",
                dir=self._temp_dir,
            )
        )
        local_chunks: list[LocalColumnarChunk] = []
        schema_hash = chunks.schema_hash(request.schema)
        row_target = chunks.initial_chunk_rows(request)
        buffered_rows: list[tuple[object, ...]] = []
        chunk_index = 0
        try:
            for rows in self._iter_batches(request):
                buffered_rows.extend(rows)
                if len(buffered_rows) < row_target:
                    continue
                row_target = self._write_local_chunk(
                    base_dir=base_dir,
                    index=chunk_index,
                    rows=buffered_rows,
                    request=request,
                    schema_hash=schema_hash,
                    local_chunks=local_chunks,
                )
                buffered_rows = []
                chunk_index += 1
            if buffered_rows:
                self._write_local_chunk(
                    base_dir=base_dir,
                    index=chunk_index,
                    rows=buffered_rows,
                    request=request,
                    schema_hash=schema_hash,
                    local_chunks=local_chunks,
                )
        except Exception:
            for path in base_dir.glob("*"):
                path.unlink(missing_ok=True)
            base_dir.rmdir()
            raise

        return LocalColumnarStagingManifest(
            base_dir=base_dir,
            columns=tuple(column for column, _ in request.schema),
            chunks=tuple(local_chunks),
            schema_hash=schema_hash,
            format=request.format,
            cleanup_policy=str((request.options or {}).get("cleanup_policy") or "eager"),
            estimated_rows=sum(chunk.row_count for chunk in local_chunks),
        )

    def iter_local_chunks(self, request: ColumnarSnapshotRequest):
        capability = self.capabilities(request)
        if not capability.supports(request):
            raise RuntimeError(", ".join(capability.blockers or ("mssql_columnar_provider_uncertified",)))

        base_dir = Path(
            tempfile.mkdtemp(
                prefix=f"dpone-mssql-columnar-{chunks.safe_run_id(request.run_id)}-",
                dir=self._temp_dir,
            )
        )
        active_chunks: list[LocalColumnarChunk] = []
        max_inflight = chunks.max_inflight_chunks(request)
        schema_hash = chunks.schema_hash(request.schema)
        row_target = chunks.initial_chunk_rows(request)
        buffered_rows: list[tuple[object, ...]] = []
        chunk_index = 0
        try:
            for rows in self._iter_batches(request):
                buffered_rows.extend(rows)
                if len(buffered_rows) < row_target:
                    continue
                chunk, row_target = self._build_local_chunk(
                    base_dir=base_dir,
                    index=chunk_index,
                    rows=buffered_rows,
                    request=request,
                    schema_hash=schema_hash,
                )
                buffered_rows = []
                chunk_index += 1
                if chunk is not None:
                    active_chunks.append(chunk)
                    chunks.cleanup_retired_chunks(active_chunks, max_inflight)
                    yield chunk
                    chunks.cleanup_retired_chunks(active_chunks, max(max_inflight - 1, 0))
            if buffered_rows:
                chunk, _ = self._build_local_chunk(
                    base_dir=base_dir,
                    index=chunk_index,
                    rows=buffered_rows,
                    request=request,
                    schema_hash=schema_hash,
                )
                if chunk is not None:
                    active_chunks.append(chunk)
                    chunks.cleanup_retired_chunks(active_chunks, max_inflight)
                    yield chunk
                    chunks.cleanup_retired_chunks(active_chunks, max(max_inflight - 1, 0))
        finally:
            for chunk in active_chunks:
                chunks.cleanup_chunk(chunk)
            chunks.cleanup_dir(base_dir)

    def iter_object_storage_windows(self, request: ColumnarSnapshotRequest):
        capability = self.capabilities(request)
        if not capability.supports(request):
            raise RuntimeError(", ".join(capability.blockers or ("mssql_columnar_provider_uncertified",)))
        if self._object_client is None:
            raise RuntimeError("object_storage_client_missing")

        prefix = ObjectStorageUri.parse(request.uri_prefix.format(run_id=request.run_id)).prefix()
        schema_hash = chunks.schema_hash(request.schema)
        row_target = chunks.initial_chunk_rows(request)
        buffered_rows: list[tuple[object, ...]] = []
        window_index = 0
        active_windows: list[ObjectStorageChunkWindow] = []
        completed = False
        try:
            write_columnar_run_marker(self._object_client, prefix, request)
            with tempfile.TemporaryDirectory(prefix="dpone-mssql-columnar-", dir=self._temp_dir) as tmp_dir:
                tmp_path = Path(tmp_dir)
                for rows in self._iter_batches(request):
                    buffered_rows.extend(rows)
                    if len(buffered_rows) < row_target:
                        continue
                    window, row_target = build_object_window(
                        tmp_dir=tmp_path,
                        index=window_index,
                        rows=buffered_rows,
                        request=request,
                        prefix=prefix,
                        schema_hash=schema_hash,
                        object_client=self._object_client,
                        write_chunk_file=self._write_chunk_file_for_window,
                        read_contract=self._resolve_read_contract(request),
                    )
                    buffered_rows = []
                    window_index += 1
                    if window is not None:
                        active_windows.append(window)
                        yield window
                        active_windows.remove(window)
                if buffered_rows:
                    window, _ = build_object_window(
                        tmp_dir=tmp_path,
                        index=window_index,
                        rows=buffered_rows,
                        request=request,
                        prefix=prefix,
                        schema_hash=schema_hash,
                        object_client=self._object_client,
                        write_chunk_file=self._write_chunk_file_for_window,
                        read_contract=self._resolve_read_contract(request),
                    )
                    if window is not None:
                        active_windows.append(window)
                        yield window
                        active_windows.remove(window)
                completed = True
        finally:
            for window in active_windows:
                window.cleanup()
            if completed:
                self._cleanup_object_storage_run_prefix(prefix, request)

    def _iter_batches(self, request: ColumnarSnapshotRequest):
        yield from iter_columnar_batches(
            self._connector,
            query=request.query,
            schema=request.schema,
            batch_size=chunks.batch_size(request),
        )

    def _write_local_chunk(
        self,
        *,
        base_dir: Path,
        index: int,
        rows: Sequence[tuple[object, ...]],
        request: ColumnarSnapshotRequest,
        schema_hash: str,
        local_chunks: list[LocalColumnarChunk],
    ) -> int:
        chunk, next_rows = self._build_local_chunk(
            base_dir=base_dir,
            index=index,
            rows=rows,
            request=request,
            schema_hash=schema_hash,
        )
        if chunk is not None:
            local_chunks.append(chunk)
        return next_rows

    def _build_local_chunk(
        self,
        *,
        base_dir: Path,
        index: int,
        rows: Sequence[tuple[object, ...]],
        request: ColumnarSnapshotRequest,
        schema_hash: str,
    ) -> tuple[LocalColumnarChunk | None, int]:
        local_path = base_dir / f"chunk-{index:05d}.parquet"
        row_count, size_bytes, digest = self._write_chunk_file(local_path, request=request, rows=rows)
        if row_count == 0:
            return None, chunks.next_chunk_rows(request, chunks.initial_chunk_rows(request), row_count, size_bytes)
        return (
            LocalColumnarChunk(
                path=local_path,
                index=index,
                row_count=row_count,
                size_bytes=size_bytes,
                sha256=digest,
                schema_hash=schema_hash,
            ),
            chunks.next_chunk_rows(request, len(rows), row_count, size_bytes),
        )

    def _write_chunk_file(
        self,
        local_path: Path,
        *,
        request: ColumnarSnapshotRequest,
        rows: Sequence[tuple[object, ...]],
    ) -> tuple[int, int, str]:
        row_count = self._parquet_writer.write_chunk(
            local_path=local_path,
            schema=request.schema,
            rows=rows,
            compression=request.compression,
        )
        size_bytes = local_path.stat().st_size
        if size_bytes > request.max_chunk_bytes:
            raise RuntimeError("columnar_chunk_exceeds_max_bytes")
        return row_count, size_bytes, chunks.file_sha256(local_path)

    def _write_chunk_file_for_window(
        self,
        local_path: Path,
        request: ColumnarSnapshotRequest,
        rows: Sequence[tuple[object, ...]],
    ) -> tuple[int, int, str]:
        return self._write_chunk_file(local_path, request=request, rows=rows)

    def _resolve_read_contract(self, request: ColumnarSnapshotRequest) -> ObjectStorageReadContract:
        if self._read_contract is not None:
            return self._read_contract
        return read_contract_from_options(_read_contract_options(request))

    def _cleanup_object_storage_run_prefix(self, prefix: ObjectStorageUri, request: ColumnarSnapshotRequest) -> None:
        cleanup_policy = str((request.options or {}).get("cleanup_policy") or "eager")
        if cleanup_policy not in {"eager", "on_success"}:
            return
        if self._object_client is not None:
            self._object_client.delete_prefix(prefix)


__all__ = ["MssqlColumnarSnapshotProvider"]
