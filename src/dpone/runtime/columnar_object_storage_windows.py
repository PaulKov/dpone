"""Lazy object-storage windows for columnar fast paths."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from threading import Lock
from typing import Any

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.columnar_fast_path_models import COLUMNAR_CHUNKS_SCHEMA, ObjectStorageChunk
from dpone.runtime.columnar_range_evidence_lifecycle import ColumnarRangeEvidenceLifecycle
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
        range_id: str | None = None,
        range_ordinal: int = 0,
        range_window_ordinal: int | None = None,
        cleanup_callback: Callable[[], None] | None = None,
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
        self.range_id = range_id
        self.range_ordinal = range_ordinal
        self.range_window_ordinal = range_window_ordinal
        self._cleanup_callback = cleanup_callback
        self._window_cleanup_lock = Lock()
        self._window_cleanup_complete = False

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

        with self._window_cleanup_lock:
            if self._window_cleanup_complete:
                return
            prefix = ObjectStorageUri.parse(self.uri_prefix).prefix()
            if _unsafe_cleanup_prefix(prefix):
                raise ValueError("Refusing to cleanup unsafe object-storage prefix")
            self.object_client.delete_prefix(prefix)
            if self._cleanup_callback is not None:
                self._cleanup_callback()
            self._window_cleanup_complete = True

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
            "range_id": self.range_id,
            "range_ordinal": self.range_ordinal,
            "range_window_ordinal": self.range_window_ordinal,
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
        self.source_byte_measurement_complete = False
        self._range_evidence = ColumnarRangeEvidenceLifecycle()
        self.range_staging_metrics: list[dict[str, object]] = []
        self._cleanup_lock = Lock()
        self._cleanup_complete = False

    @property
    def range_parallelism_policy(self) -> object | None:
        plan = getattr(self.request, "range_plan", None)
        return getattr(plan, "policy", None)

    @property
    def range_execution_evidence(self) -> object | None:
        return self._range_evidence.current

    def iter_windows(self) -> Any:
        iterator = getattr(self.provider, "iter_object_storage_windows", None)
        if not callable(iterator):
            raise TypeError("Columnar provider does not support chunked object-storage windows")
        evidence_reader = getattr(self.provider, "range_execution_evidence", None)
        try:
            for window_index, window in enumerate(iterator(self.request)):
                self._publish_window_export_evidence(window, window_index=window_index)
                yield window
        except BaseException:
            if callable(evidence_reader) and (evidence := evidence_reader(self.request)) is not None:
                if getattr(evidence, "outcome_status", None) in {"failed", "cancelled"}:
                    self._range_evidence.bind_terminal_failure(evidence)
            raise
        if callable(evidence_reader) and (evidence := evidence_reader(self.request)) is not None:
            self._range_evidence.bind_extracted(evidence)

    def mark_source_byte_measurement_complete(self) -> None:
        """Admit the byte budget only after every emitted window has a digest."""

        if any(not _window_byte_identity(item) for item in self.slice_evidence):
            return
        self.source_byte_measurement_complete = True

    def _publish_window_export_evidence(self, window: object, *, window_index: int) -> None:
        rows_exported = canonical_non_negative_int(getattr(window, "row_count", None))
        if rows_exported is None:
            return
        digest = _window_digest(window)
        evidence = {
            "transport": "columnar_object_storage_window",
            "uri_prefix": str(getattr(window, "uri_prefix", "")),
            "bytes": int(getattr(window, "size_bytes", 0) or 0),
        }
        if digest is not None:
            evidence["sha256"] = digest
        range_window_ordinal = getattr(window, "range_window_ordinal", None)
        append_export_slice_evidence(
            self.slice_evidence,
            partition_index=int(getattr(window, "range_ordinal", 0) or 0),
            slice_index=window_index if range_window_ordinal is None else int(range_window_ordinal),
            rows_exported=rows_exported,
            **evidence,
        )

    def record_window_metric(self, metric: Mapping[str, object]) -> None:
        self.window_metrics.append(dict(metric))

    def record_range_staging_metric(self, metric: Mapping[str, object]) -> None:
        """Record sink-observed staging facts without inventing absent measurements."""

        self.range_staging_metrics.append(dict(metric))

    def advance_range_staging(
        self,
        groups: Sequence[Any],
        stage_configs: Sequence[Any],
        observed_load_concurrency: int,
        assembly_rows: int | None,
        authoritative_config: Any,
        observed_rows: Mapping[str, int],
    ) -> None:
        self._range_evidence.stage(
            groups=groups,
            stage_configs=stage_configs,
            observed_load_concurrency=observed_load_concurrency,
            assembly_rows=assembly_rows,
            authoritative_config=authoritative_config,
            observed_rows=observed_rows,
        )

    def mark_range_quality_passed(self, *, config: Any, validation_receipt: object) -> None:
        if self.range_execution_evidence is None:
            return
        self._range_evidence.quality_passed(config=config, validation_receipt=validation_receipt)

    def mark_range_governed_quality_passed(self, *, config: Any, quality_receipt: Mapping[str, object]) -> None:
        if self.range_execution_evidence is None:
            return
        self._range_evidence.governed_quality_passed(config=config, quality_receipt=quality_receipt)

    def mark_range_publication_unknown(self) -> None:
        if self.range_execution_evidence is None:
            return
        self._range_evidence.publication_unknown()

    def mark_range_publication_confirmed(self, *, result: Any, config: Any) -> None:
        if self.range_execution_evidence is None:
            return
        self._range_evidence.publication_confirmed(result=result, config=config)

    def mark_range_cleanup_succeeded(self) -> None:
        self._range_evidence.cleanup_succeeded()

    def mark_range_cleanup_failed(self) -> None:
        self._range_evidence.cleanup_failed()

    def mark_range_failed(self, **outcome: Any) -> None:
        self._range_evidence.failed(**outcome)

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
            "range_execution": self._range_evidence.to_dict(),
            "range_staging_metrics": list(self.range_staging_metrics),
        }

    def cleanup(self) -> None:
        self.cleanup_owned_object_storage()

    def cleanup_owned_object_storage(self) -> None:
        """Delete the run-owned prefix only at a reconciled staged boundary."""

        if self.cleanup_policy not in {"eager", "on_success"}:
            return
        with self._cleanup_lock:
            if self._cleanup_complete:
                return
            cleanup = getattr(self.provider, "cleanup_object_storage_run", None)
            if callable(cleanup):
                cleanup(self.request)
            self._cleanup_complete = True

    def staged_cleanup_owner(self) -> ObjectStorageColumnarChunkedArtifact:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> ObjectStorageColumnarChunkedArtifact:
        memo[id(self)] = self
        return self


def _window_digest(window: object) -> str | None:
    chunks = tuple(getattr(window, "chunks", ()) or ())
    digests = tuple(str(getattr(chunk, "sha256", "") or "") for chunk in chunks)
    if not digests or any(not digest for digest in digests):
        return None
    if len(digests) == 1:
        return digests[0]
    return hashlib.sha256("\n".join(digests).encode()).hexdigest()


def _window_byte_identity(item: dict[str, object]) -> bool:
    raw_bytes = item.get("bytes")
    digest = item.get("sha256")
    return (
        not isinstance(raw_bytes, bool)
        and isinstance(raw_bytes, int)
        and raw_bytes >= 0
        and isinstance(digest, str)
        and bool(digest)
    )


def _unsafe_cleanup_prefix(prefix: Any) -> bool:
    return len([part for part in prefix.key.split("/") if part]) < 2


__all__ = ["ObjectStorageChunkWindow", "ObjectStorageColumnarChunkedArtifact"]
