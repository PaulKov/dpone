from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.native_transfer_execution import NativeTransferResourcePolicy
    from dpone.runtime.native_transfer_transport import SliceTransportPlan
    from dpone.runtime.staging import StagingManager
    from dpone.runtime.transfer_store_models import TransferObjectRef
    from dpone.runtime.transfer_store_service import SliceTransferStore


from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.runtime.artifact_models import BaseExtractionArtifact
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.disk_budget import DiskBudget
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.native_transfer_row_authority import actual_export_rows, require_target_row_count
from dpone.runtime.native_transfer_slicing import RangeSlicePlanner, TransferSlice
from dpone.runtime.staging import owned_staging_handle

SliceExporter = Callable[[TransferSlice], FileExportArtifact]
SliceLoader = Callable[[FileExportArtifact], int]
StreamingSliceExporter = Callable[[TransferSlice], ByteStreamArtifact]
StreamingSliceLoader = Callable[[ByteStreamArtifact], int]


class PartitionedTransferPlanArtifact(BaseExtractionArtifact):
    """Lazy file transfer artifact: export one slice, load it, then clean it."""

    extraction_completion_mode = "lazy"

    def __init__(
        self,
        slices: Sequence[TransferSlice],
        columns: Sequence[str],
        *,
        exporter: SliceExporter,
        resource_policy: NativeTransferResourcePolicy,
        stream_exporter: StreamingSliceExporter | None = None,
        transport_plan: SliceTransportPlan | None = None,
        estimated_rows: int | None = None,
        format: str = "mssql-delimited",
        bulk_text_codec: Any | None = None,
        bulk_wire_contract: Any | None = None,
        transfer_store: SliceTransferStore | None = None,
        reusable_objects: Mapping[tuple[int, int], TransferObjectRef] | None = None,
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self.slices = list(slices)
        self.columns = list(columns)
        self.exporter = exporter
        self.stream_exporter = stream_exporter
        self.transport_plan = transport_plan
        self.resource_policy = resource_policy
        self.format = format
        self.bulk_text_codec = bulk_text_codec
        self.bulk_wire_contract = bulk_wire_contract
        self.transfer_store = transfer_store
        self.reusable_objects = dict(reusable_objects or {})
        self.slice_evidence: list[dict[str, Any]] = []

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            load_from_file = getattr(staging_manager, "load_from_file")
            inserted = self.load_with(
                lambda file_artifact: require_target_row_count(load_from_file(handle, file_artifact))
            )
            handle.row_count = inserted
            return handle

    def load_with(self, loader: SliceLoader, *, stream_loader: StreamingSliceLoader | None = None) -> int:
        lifecycle = self.extraction_lifecycle
        if lifecycle is not None and lifecycle.receipt is None:
            lifecycle.acquire()
        if self.transport_plan is not None and self.transport_plan.transport == "stream":
            if stream_loader is None:
                raise RuntimeError("native_transfer_stream_loader_required")
            if self.stream_exporter is None:
                raise RuntimeError("native_transfer_stream_exporter_required")
            loaded = sum(self._stream_load_cleanup(item, stream_loader) for item in self.slices)
        else:
            budget = DiskBudget(
                max_active_files=self.resource_policy.max_active_files,
                max_active_bytes=self.resource_policy.max_active_bytes,
            )
            loaded = sum(self._export_load_cleanup(item, loader, budget) for item in self.slices)
        if lifecycle is not None:
            lifecycle.complete()
        return loaded

    def cleanup(self) -> None:
        return None

    def _export_load_cleanup(self, item: TransferSlice, loader: SliceLoader, budget: DiskBudget) -> int:
        expected_bytes = min(self.resource_policy.target_file_bytes, self.resource_policy.max_active_bytes)
        if not budget.try_acquire(expected_bytes=expected_bytes):
            raise RuntimeError("native_transfer_active_file_budget_exceeded")
        file_artifact: FileExportArtifact | None = None
        object_ref: TransferObjectRef | None = None
        reused_object = False
        try:
            reusable = self.reusable_objects.get((item.partition_index, item.slice_index))
            if reusable is not None:
                if self.transfer_store is None:
                    raise RuntimeError("native_transfer_object_store_not_configured")
                file_artifact = self._hydrate_reusable_object(item, reusable)
                object_ref = reusable
                reused_object = True
            else:
                file_artifact = self.exporter(item)
            file_artifact.bulk_text_codec = file_artifact.bulk_text_codec or self.bulk_text_codec
            if self.bulk_wire_contract is not None and not hasattr(file_artifact, "bulk_wire_contract"):
                setattr(file_artifact, "bulk_wire_contract", self.bulk_wire_contract)
            file_bytes = Path(file_artifact.file_path).stat().st_size
            if file_bytes > self.resource_policy.max_file_bytes:
                file_artifact.cleanup()
                file_artifact = None
                budget.release(actual_bytes=expected_bytes)
                return self._handle_oversized_slice(item, loader, budget)
            if object_ref is None and self.transfer_store is not None:
                object_ref = self.transfer_store.stage_file(
                    file_artifact.file_path,
                    partition_index=item.partition_index,
                    slice_index=item.slice_index,
                    content_type=_content_type(self.format),
                )
            rows_exported = actual_export_rows(file_artifact)
            rows_loaded = require_target_row_count(loader(file_artifact))
            self.slice_evidence.append(
                _slice_evidence(
                    item,
                    file_artifact,
                    rows_exported=rows_exported,
                    rows_loaded=rows_loaded,
                    file_bytes=file_bytes,
                    object_ref=object_ref,
                    reused_object=reused_object,
                )
            )
            return rows_loaded
        finally:
            if file_artifact is not None:
                file_artifact.cleanup()
            budget.release(actual_bytes=expected_bytes)

    def _hydrate_reusable_object(self, item: TransferSlice, ref: TransferObjectRef) -> FileExportArtifact:
        assert self.transfer_store is not None
        path = self.transfer_store.hydrate_temp(ref)
        artifact = FileExportArtifact(
            str(path),
            self.columns,
            format=self.format,
            estimated_rows=item.estimated_rows,
            bulk_text_codec=self.bulk_text_codec,
        )
        if self.bulk_wire_contract is not None:
            setattr(artifact, "bulk_wire_contract", self.bulk_wire_contract)
        return artifact

    def _handle_oversized_slice(
        self,
        item: TransferSlice,
        loader: SliceLoader,
        budget: DiskBudget,
    ) -> int:
        if self.resource_policy.oversize_policy != "split_and_retry":
            raise RuntimeError("slice_oversized_unsplittable")
        children = RangeSlicePlanner(self.resource_policy).split(item)
        if not children:
            raise RuntimeError("slice_oversized_unsplittable")
        return sum(self._export_load_cleanup(child, loader, budget) for child in children)

    def _stream_load_cleanup(self, item: TransferSlice, loader: StreamingSliceLoader) -> int:
        assert self.stream_exporter is not None
        stream_artifact = self.stream_exporter(item)
        try:
            stream_artifact.bulk_text_codec = getattr(stream_artifact, "bulk_text_codec", None) or self.bulk_text_codec
            # BCP pipe publishes rows_exported only after the producer exits; that
            # happens while the stream is consumed by the loader.
            actual_export_rows(stream_artifact)
            rows_loaded = require_target_row_count(loader(stream_artifact))
            rows_exported = actual_export_rows(stream_artifact)
            self.slice_evidence.append(
                _stream_evidence(
                    item,
                    stream_artifact,
                    rows_exported=rows_exported,
                    rows_loaded=rows_loaded,
                )
            )
            return rows_loaded
        finally:
            stream_artifact.cleanup()


def append_export_slice_evidence(
    slice_evidence: list[dict[str, Any]],
    *,
    partition_index: int,
    slice_index: int,
    rows_exported: int,
    **fields: Any,
) -> None:
    """Append one completed export authority record for quality scope aggregation."""

    evidence: dict[str, Any] = {
        "partition_index": partition_index,
        "slice_index": slice_index,
        "rows_exported": rows_exported,
        **fields,
    }
    slice_evidence.append(evidence)


def _slice_evidence(
    item: TransferSlice,
    artifact: FileExportArtifact,
    *,
    rows_exported: int | None,
    rows_loaded: int,
    file_bytes: int,
    object_ref: TransferObjectRef | None,
    reused_object: bool,
) -> dict[str, Any]:
    slice_info = item.to_dict()
    evidence: dict[str, Any] = {
        "transport": "file",
        "partition_index": slice_info["partition_index"],
        "slice_index": slice_info["slice_index"],
        "lower_bound": slice_info["lower_bound"],
        "upper_bound": slice_info["upper_bound"],
        "include_upper": slice_info["include_upper"],
        "is_null_partition": slice_info["is_null_partition"],
        "rows_loaded": rows_loaded,
        "bytes": file_bytes,
        "file_name": Path(artifact.file_path).name,
    }
    if rows_exported is not None:
        evidence["rows_exported"] = rows_exported
    if object_ref is not None:
        transfer_object = object_ref.to_dict()
        transfer_object["reused"] = reused_object
        evidence["transfer_object"] = transfer_object
    return evidence


def _stream_evidence(
    item: TransferSlice,
    artifact: ByteStreamArtifact,
    *,
    rows_exported: int | None,
    rows_loaded: int,
) -> dict[str, Any]:
    stats = artifact.stats
    slice_info = item.to_dict()
    evidence: dict[str, Any] = {
        "transport": "stream",
        "partition_index": slice_info["partition_index"],
        "slice_index": slice_info["slice_index"],
        "lower_bound": slice_info["lower_bound"],
        "upper_bound": slice_info["upper_bound"],
        "include_upper": slice_info["include_upper"],
        "is_null_partition": slice_info["is_null_partition"],
        "rows_loaded": rows_loaded,
        "bytes": stats.size_bytes,
        "sha256": stats.sha256,
        "chunks": stats.chunks,
        "format": artifact.format,
    }
    if rows_exported is not None:
        evidence["rows_exported"] = rows_exported
    return evidence


def _content_type(format_name: str) -> str | None:
    return "text/tab-separated-values" if format_name == "mssql-delimited" else None


__all__ = [
    "PartitionedTransferPlanArtifact",
    "SliceExporter",
    "SliceLoader",
    "StreamingSliceExporter",
    "StreamingSliceLoader",
    "append_export_slice_evidence",
]
