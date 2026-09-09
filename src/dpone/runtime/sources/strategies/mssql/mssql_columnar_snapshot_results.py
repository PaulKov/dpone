from __future__ import annotations

import tempfile
from pathlib import Path

from dpone.runtime.columnar_fast_path_models import (
    LocalColumnarChunk,
    LocalColumnarStagingManifest,
    ObjectStorageChunk,
    ObjectStorageStagingManifest,
)
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest
from dpone.runtime.object_storage_access_models import ObjectStorageReadContract
from dpone.runtime.sources.strategies.mssql import mssql_columnar_chunks as chunks
from dpone.storage import ObjectStorageUri
from dpone.storage.protocols import ObjectStorageClient


def object_prefix(request: ColumnarSnapshotRequest) -> ObjectStorageUri:
    return ObjectStorageUri.parse(request.uri_prefix.format(run_id=request.run_id)).prefix()


def local_base_dir(*, run_id: str, temp_dir: str | Path | None) -> Path:
    return Path(
        tempfile.mkdtemp(
            prefix=f"dpone-mssql-columnar-{chunks.safe_run_id(run_id)}-",
            dir=Path(temp_dir) if temp_dir else None,
        )
    )


def cleanup_local_base_dir(base_dir: Path) -> None:
    for path in base_dir.glob("*"):
        path.unlink(missing_ok=True)
    base_dir.rmdir()


def object_manifest(
    *,
    request: ColumnarSnapshotRequest,
    prefix: ObjectStorageUri,
    uploaded_chunks: list[ObjectStorageChunk],
    read_contract: ObjectStorageReadContract,
    schema_hash: str,
    object_client: ObjectStorageClient,
) -> ObjectStorageStagingManifest:
    return ObjectStorageStagingManifest(
        uri_prefix=str(prefix),
        columns=tuple(column for column, _ in request.schema),
        chunks=tuple(uploaded_chunks),
        read_contract=read_contract,
        schema_hash=schema_hash,
        format=request.format,
        object_client=object_client,
        cleanup_policy=_cleanup_policy(request),
        estimated_rows=sum(chunk.row_count for chunk in uploaded_chunks),
    )


def local_manifest(
    *,
    request: ColumnarSnapshotRequest,
    base_dir: Path,
    local_chunks: list[LocalColumnarChunk],
    schema_hash: str,
) -> LocalColumnarStagingManifest:
    return LocalColumnarStagingManifest(
        base_dir=base_dir,
        columns=tuple(column for column, _ in request.schema),
        chunks=tuple(local_chunks),
        schema_hash=schema_hash,
        format=request.format,
        cleanup_policy=_cleanup_policy(request),
        estimated_rows=sum(chunk.row_count for chunk in local_chunks),
    )


def _cleanup_policy(request: ColumnarSnapshotRequest) -> str:
    return str((request.options or {}).get("cleanup_policy") or "eager")


__all__ = [
    "cleanup_local_base_dir",
    "local_base_dir",
    "local_manifest",
    "object_manifest",
    "object_prefix",
]
