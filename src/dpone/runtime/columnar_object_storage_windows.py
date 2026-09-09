"""Lazy object-storage windows for columnar fast paths."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.columnar_fast_path_models import COLUMNAR_CHUNKS_SCHEMA, ObjectStorageChunk
from dpone.runtime.native_transfer_artifacts import append_export_slice_evidence
from dpone.runtime.native_transfer_row_authority import canonical_non_negative_int, sum_completed_chunk_rows


class ObjectStorageChunkWindow(BaseExtractionArtifact):
    """One bounded object-storage window ready for sink-side pull."""

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
        producer_metrics: Mapping[str, object] | None = None,
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
        self.producer_metrics = dict(producer_metrics or {})

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
            "execution_mode": "chunked",
            "uri_prefix": self.uri_prefix,
            "format": self.format,
            "columns": list(self.columns),
            "schema_hash": self.schema_hash,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "cleanup_policy": self.cleanup_policy,
            "read_access": self.read_contract.to_evidence(),
            "producer_metrics": dict(self.producer_metrics),
            "chunks": [chunk.to_dict() for chunk in self.chunks],
        }


class ObjectStorageColumnarChunkedArtifact(BaseExtractionArtifact):
    """Lazy object-storage windows loaded by the sink as each window is produced."""

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
        self.window_metrics: list[dict[str, object]] = []
        self.slice_evidence: list[dict[str, object]] = []

    def iter_windows(self) -> Any:
        iterator = getattr(self.provider, "iter_object_storage_windows", None)
        if not callable(iterator):
            raise TypeError("Columnar provider does not support chunked object-storage windows")
        for window_index, window in enumerate(iterator(self.request)):
            self._publish_window_export_evidence(window, window_index=window_index)
            yield window

    def _publish_window_export_evidence(self, window: object, *, window_index: int) -> None:
        rows_exported = canonical_non_negative_int(getattr(window, "row_count", None))
        if rows_exported is None:
            return
        append_export_slice_evidence(
            self.slice_evidence,
            partition_index=0,
            slice_index=window_index,
            rows_exported=rows_exported,
            transport="columnar_object_storage_window",
            uri_prefix=str(getattr(window, "uri_prefix", "")),
            bytes=int(getattr(window, "size_bytes", 0) or 0),
        )

    def record_window_metric(self, metric: Mapping[str, object]) -> None:
        self.window_metrics.append(dict(metric))

    def materialize(
        self,
        staging_manager: Any,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        loader = getattr(staging_manager, "load_from_object_storage_chunked", None)
        if not callable(loader):
            raise TypeError("Sink does not support object-storage columnar chunked windows")
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
            "window_metrics": list(self.window_metrics),
            "slice_evidence": list(self.slice_evidence),
        }

    def cleanup(self) -> None:
        return


def _unsafe_cleanup_prefix(prefix: Any) -> bool:
    return len([part for part in prefix.key.split("/") if part]) < 2


__all__ = ["ObjectStorageChunkWindow", "ObjectStorageColumnarChunkedArtifact"]
