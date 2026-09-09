"""Columnar fast-path decisions and object-storage chunk artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.native_transfer_row_authority import sum_completed_chunk_rows

COLUMNAR_DECISION_SCHEMA = "dpone.native_transfer.columnar_fast_path.v1"
COLUMNAR_CHUNKS_SCHEMA = "dpone.native_transfer.columnar_chunks.v1"


@dataclass(frozen=True, slots=True)
class ColumnarFastPathDecision:
    requested_mode: str
    requested_provider: str
    selected_provider: str
    current_provider: str
    fallback_allowed: bool
    should_start_source_io: bool
    fallback_reason: str | None = None
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    details: dict[str, object] = field(default_factory=dict)
    schema_version: str = COLUMNAR_DECISION_SCHEMA

    @property
    def blocked(self) -> bool:
        return bool(self.blockers) and self.selected_provider == "blocked"

    def to_evidence(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "decision_id": "native_transfer.columnar_fast_path",
            "requested_mode": self.requested_mode,
            "requested_provider": self.requested_provider,
            "selected_provider": self.selected_provider,
            "current_provider": self.current_provider,
            "fallback_allowed": self.fallback_allowed,
            "fallback_reason": self.fallback_reason,
            "should_start_source_io": self.should_start_source_io,
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
            "details": self.details,
        }


@dataclass(frozen=True, slots=True)
class ObjectStorageChunk:
    uri: str
    index: int
    row_count: int
    size_bytes: int
    sha256: str
    schema_hash: str
    format: str = "parquet"

    def to_dict(self) -> dict[str, object]:
        return {
            "uri": self.uri,
            "index": self.index,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "schema_hash": self.schema_hash,
            "format": self.format,
        }


@dataclass(frozen=True, slots=True)
class LocalColumnarChunk:
    path: Path
    index: int
    row_count: int
    size_bytes: int
    sha256: str
    schema_hash: str
    format: str = "parquet"

    def to_dict(self) -> dict[str, object]:
        return {
            "path": str(self.path),
            "index": self.index,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "schema_hash": self.schema_hash,
            "format": self.format,
        }


class LocalColumnarStagingManifest(BaseExtractionArtifact):
    """Run-scoped local columnar chunks ready for direct sink push."""

    def __init__(
        self,
        *,
        base_dir: str | Path,
        columns: Sequence[str],
        chunks: Sequence[LocalColumnarChunk],
        schema_hash: str,
        format: str = "parquet",
        cleanup_policy: str = "eager",
        estimated_rows: int | None = None,
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self.base_dir = Path(base_dir)
        self.columns = tuple(columns)
        self.chunks = tuple(sorted(chunks, key=lambda chunk: chunk.index))
        self.schema_hash = schema_hash
        self.format = format.lower()
        self.cleanup_policy = cleanup_policy

    @property
    def row_count(self) -> int:
        return sum_completed_chunk_rows(self.chunks)

    @property
    def size_bytes(self) -> int:
        return sum(chunk.size_bytes for chunk in self.chunks)

    def materialize(
        self,
        staging_manager: Any,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        loader = getattr(staging_manager, "load_from_local_columnar_manifest", None)
        if not callable(loader):
            raise TypeError("Sink does not support local columnar direct push")
        inserted = int(loader(load_config, self, list(schema)) or 0)
        return StagingTableArtifact(
            schema=load_config.target_schema,
            table=load_config.target_table,
            columns=[column for column, _ in schema],
            staging_manager=staging_manager,
            row_count=inserted,
            database=getattr(load_config, "target_database", None),
            target_schema=load_config.target_schema,
        )

    def cleanup(self) -> None:
        if self.cleanup_policy not in {"eager", "on_success"}:
            return
        for chunk in self.chunks:
            Path(chunk.path).unlink(missing_ok=True)
        try:
            self.base_dir.rmdir()
        except OSError:
            return

    def to_evidence(self) -> dict[str, object]:
        return {
            "schema_version": COLUMNAR_CHUNKS_SCHEMA,
            "base_dir": str(self.base_dir),
            "format": self.format,
            "columns": list(self.columns),
            "schema_hash": self.schema_hash,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "cleanup_policy": self.cleanup_policy,
            "chunks": [chunk.to_dict() for chunk in self.chunks],
        }


class LocalColumnarChunkedArtifact(BaseExtractionArtifact):
    """Lazy local columnar chunks loaded by the sink as each chunk is produced."""

    def __init__(
        self,
        *,
        provider: Any,
        request: Any,
        columns: Sequence[str],
        schema_hash: str,
        format: str = "parquet",
        cleanup_policy: str = "eager",
        estimated_rows: int | None = None,
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self.provider = provider
        self.request = request
        self.columns = tuple(columns)
        self.schema_hash = schema_hash
        self.format = format.lower()
        self.cleanup_policy = cleanup_policy

    def iter_chunks(self) -> Any:
        iterator = getattr(self.provider, "iter_local_chunks", None)
        if not callable(iterator):
            raise TypeError("Columnar provider does not support chunked local files")
        yield from iterator(self.request)

    def materialize(
        self,
        staging_manager: Any,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        loader = getattr(staging_manager, "load_from_local_columnar_chunked", None)
        if not callable(loader):
            raise TypeError("Sink does not support local columnar chunked files")
        inserted = int(loader(load_config, self, list(schema)) or 0)
        return StagingTableArtifact(
            schema=load_config.target_schema,
            table=load_config.target_table,
            columns=[column for column, _ in schema],
            staging_manager=staging_manager,
            row_count=inserted,
            database=getattr(load_config, "target_database", None),
            target_schema=load_config.target_schema,
        )

    def to_evidence(self) -> dict[str, object]:
        return {
            "schema_version": COLUMNAR_CHUNKS_SCHEMA,
            "execution_mode": "chunked",
            "format": self.format,
            "columns": list(self.columns),
            "schema_hash": self.schema_hash,
            "cleanup_policy": self.cleanup_policy,
        }

    def cleanup(self) -> None:
        return


class ObjectStorageStagingManifest(BaseExtractionArtifact):
    """Run-scoped columnar objects ready for sink-side staging pull."""

    def __init__(
        self,
        *,
        uri_prefix: str,
        columns: Sequence[str],
        chunks: Sequence[ObjectStorageChunk],
        read_contract: Any,
        schema_hash: str,
        format: str = "parquet",
        object_client: Any | None = None,
        cleanup_policy: str = "eager",
        estimated_rows: int | None = None,
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self.uri_prefix = uri_prefix
        self.columns = tuple(columns)
        self.chunks = tuple(sorted(chunks, key=lambda chunk: chunk.index))
        self.read_contract = read_contract
        self.schema_hash = schema_hash
        self.format = format.lower()
        self.object_client = object_client
        self.cleanup_policy = cleanup_policy

    @property
    def row_count(self) -> int:
        return sum_completed_chunk_rows(self.chunks)

    @property
    def size_bytes(self) -> int:
        return sum(chunk.size_bytes for chunk in self.chunks)

    def object_key_pattern(self) -> str:
        from dpone.storage import ObjectStorageUri

        prefix = ObjectStorageUri.parse(self.uri_prefix).prefix()
        suffix = self.format if self.format.startswith(".") else f".{self.format}"
        return f"{prefix.key}*{suffix}"

    def object_uri_pattern(self) -> str:
        from dpone.storage import ObjectStorageUri

        prefix = ObjectStorageUri.parse(self.uri_prefix).prefix()
        return str(ObjectStorageUri(prefix.provider, prefix.bucket, self.object_key_pattern(), account=prefix.account))

    def materialize(
        self,
        staging_manager: Any,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        loader = getattr(staging_manager, "load_from_object_storage_manifest", None)
        if not callable(loader):
            raise TypeError("Sink does not support object-storage columnar staging")
        inserted = int(loader(load_config, self, list(schema)) or 0)
        return StagingTableArtifact(
            schema=load_config.target_schema,
            table=load_config.target_table,
            columns=[column for column, _ in schema],
            staging_manager=staging_manager,
            row_count=inserted,
            database=getattr(load_config, "target_database", None),
            target_schema=load_config.target_schema,
        )

    def cleanup(self) -> None:
        if self.cleanup_policy not in {"eager", "on_success"} or self.object_client is None:
            return
        from dpone.storage import ObjectStorageUri

        prefix = ObjectStorageUri.parse(self.uri_prefix).prefix()
        if _unsafe_cleanup_prefix(prefix):
            raise ValueError("Refusing to cleanup unsafe object-storage prefix")
        self.object_client.delete_prefix(prefix)

    def to_evidence(self) -> dict[str, object]:
        return {
            "schema_version": COLUMNAR_CHUNKS_SCHEMA,
            "uri_prefix": self.uri_prefix,
            "format": self.format,
            "columns": list(self.columns),
            "schema_hash": self.schema_hash,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "cleanup_policy": self.cleanup_policy,
            "read_access": self.read_contract.to_evidence(),
            "chunks": [chunk.to_dict() for chunk in self.chunks],
        }


def preflight_details(preflight: Any | None) -> dict[str, object]:
    if preflight is None:
        return {"object_storage_preflight": "missing"}
    return {
        "object_storage_preflight": "passed" if preflight.passed else "failed",
        "preflight_schema": preflight.schema_version,
        "preflight_checks": list(preflight.checks),
        "preflight_warnings": list(preflight.warnings),
        "preflight_blockers": list(preflight.blockers),
    }


def _unsafe_cleanup_prefix(prefix: Any) -> bool:
    return len([part for part in prefix.key.split("/") if part]) < 2


__all__ = [
    "COLUMNAR_CHUNKS_SCHEMA",
    "COLUMNAR_DECISION_SCHEMA",
    "ColumnarFastPathDecision",
    "LocalColumnarChunkedArtifact",
    "LocalColumnarChunk",
    "LocalColumnarStagingManifest",
    "ObjectStorageChunk",
    "ObjectStorageStagingManifest",
    "preflight_details",
]
