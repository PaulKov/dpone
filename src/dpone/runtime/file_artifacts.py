from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.staging import StagingManager


import logging
import os
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from dpone.runtime.artifact_integrity import (
    ArtifactIntegrityError,
    CompletedFileWrite,
    FileArtifactReceipt,
    FileIdentity,
    FileWireContract,
)
from dpone.runtime.artifact_models import BaseExtractionArtifact
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.file_artifact_authority import FileIntegrityAuthority, FileReleaseAuthority
from dpone.runtime.native_transfer_row_authority import require_target_row_count
from dpone.runtime.staging import owned_staging_handle

logger = logging.getLogger(__name__)


class PartitionedArtifactReleaseError(RuntimeError):
    """One or more partition resources could not be released."""


@dataclass
class FileExportArtifact(BaseExtractionArtifact):
    """Артефакт, представляющий путь к выгруженному CSV (может быть gzip)."""

    file_path: str = field(init=False)
    columns: Sequence[str] = field(init=False)
    compressed: bool = field(default=False, init=False)
    format: str = field(default="csv", init=False)
    bulk_text_codec: Any | None = field(default=None, init=False)
    integrity_receipt: FileArtifactReceipt | None = field(default=None, init=False)
    contract_validation_receipt: Any | None = field(default=None, init=False)
    has_header: bool = field(default=False, init=False)

    def __init__(
        self,
        file_path: str,
        columns: Sequence[str],
        *,
        compressed: bool = False,
        format: str = "csv",
        estimated_rows: int | None = None,
        bulk_text_codec: Any | None = None,
        rows_exported: int | None = None,
        has_header: bool = False,
        _release_authority: FileReleaseAuthority | None = None,
        _integrity_receipt: FileArtifactReceipt | None = None,
        _completed_write: CompletedFileWrite | None = None,
        _integrity_authority: FileIntegrityAuthority | None = None,
    ):
        super().__init__(estimated_rows=estimated_rows)
        self.file_path = file_path
        self.columns = columns
        self.compressed = compressed
        self.format = format
        self.bulk_text_codec = bulk_text_codec
        self.has_header = bool(has_header)
        self._release_authority = _release_authority or FileReleaseAuthority(file_path)
        self._integrity_authority = _integrity_authority
        if sum(value is not None for value in (_integrity_receipt, _completed_write, _integrity_authority)) > 1:
            raise ArtifactIntegrityError("artifact_integrity.receipt_authority_ambiguous")
        if _integrity_authority is not None:
            self.integrity_receipt = _integrity_authority.capture_integrity_receipt(
                self.wire_contract(),
                rows_exported,
            )
        elif _completed_write is not None:
            self.integrity_receipt = FileArtifactReceipt.capture_completed_write(
                file_path,
                wire_contract=self.wire_contract(),
                sha256=_completed_write.sha256,
                size_bytes=_completed_write.size_bytes,
                rows_exported=_completed_write.rows_exported,
            )
        elif _integrity_receipt is not None:
            if rows_exported is not None and _integrity_receipt.rows_exported != rows_exported:
                raise ArtifactIntegrityError("artifact_integrity.rows_exported_immutable")
            if _integrity_receipt.wire_contract_sha256 != self.wire_contract().sha256:
                raise ArtifactIntegrityError("artifact_integrity.wire_contract_mismatch")
            self.integrity_receipt = _integrity_receipt
        else:
            self.integrity_receipt = (
                FileArtifactReceipt.capture(
                    file_path,
                    wire_contract=self.wire_contract(),
                    rows_exported=rows_exported,
                )
                if os.path.exists(file_path)
                else None
            )
        self.contract_validation_receipt = None

    def rebind(
        self,
        *,
        file_path: str | None = None,
        columns: Sequence[str] | None = None,
        rows_exported: int | None = None,
        has_header: bool | None = None,
    ) -> FileExportArtifact:
        """Return a byte-receipted replacement while preserving lineage metadata.

        Strategy enrichment replaces the physical file, but partition identity
        and transfer evidence still describe the same source slice.  Keeping
        that metadata on one artifact operation avoids lossy hand-written
        reconstruction at each transformer.
        """

        replacement_path = file_path or self.file_path
        replacement = FileExportArtifact(
            file_path=replacement_path,
            columns=list(columns) if columns is not None else list(self.columns),
            compressed=self.compressed,
            format=self.format,
            estimated_rows=self.estimated_rows,
            bulk_text_codec=self.bulk_text_codec,
            rows_exported=self.rows_exported if rows_exported is None else rows_exported,
            has_header=self.has_header if has_header is None else has_header,
            _release_authority=self._release_authority if replacement_path == self.file_path else None,
            _integrity_authority=self._integrity_authority if replacement_path == self.file_path else None,
        )
        if replacement_path == self.file_path:
            replacement.bind_terminal_authority(self.terminal_authority)
        core = {
            "_estimated_rows",
            "_terminal_lock",
            "_terminal_authority",
            "_release_authority",
            "_integrity_authority",
            "file_path",
            "columns",
            "compressed",
            "format",
            "bulk_text_codec",
            "integrity_receipt",
            "has_header",
        }
        for name, value in vars(self).items():
            if name not in core:
                setattr(replacement, name, value)
        return replacement

    @property
    def rows_exported(self) -> int | None:
        return self.integrity_receipt.rows_exported if self.integrity_receipt is not None else None

    @rows_exported.setter
    def rows_exported(self, value: int) -> None:
        if self._integrity_authority is not None:
            self.integrity_receipt = self._integrity_authority.capture_integrity_receipt(
                self.wire_contract(),
                value,
            )
            return
        receipt = self.integrity_receipt
        if receipt is None:
            receipt = FileArtifactReceipt.capture(self.file_path, wire_contract=self.wire_contract())
        self.integrity_receipt = receipt.with_rows_exported(
            self.file_path,
            value,
            wire_contract=self.wire_contract(),
        )

    def wire_contract(self) -> FileWireContract:
        """Return the current semantic interpretation of the file bytes."""

        return FileWireContract.resolve(
            columns=tuple(str(column) for column in self.columns),
            format=self.format,
            compressed=self.compressed,
            has_header=self.has_header,
            bulk_text_codec=self.bulk_text_codec,
        )

    def require_integrity_receipt(self) -> FileArtifactReceipt:
        """Return a complete receipt and revalidate immutable source bytes."""

        if self.integrity_receipt is None:
            raise ArtifactIntegrityError("artifact_integrity.receipt_missing")
        if self._integrity_authority is None:
            self.integrity_receipt.verify(self.file_path, wire_contract=self.wire_contract())
        else:
            self._integrity_authority.verify_integrity_receipt(
                self.integrity_receipt,
                self.wire_contract(),
            )
        self.integrity_receipt.require_rows_exported()
        return self.integrity_receipt

    def rebind_columns(self, columns: Sequence[str]) -> FileExportArtifact:
        """Rebind positional column identities without touching immutable bytes."""

        return self.rebind(columns=columns)

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            inserted = getattr(staging_manager, "load_from_file")(handle, self)
            handle.row_count = inserted
            return handle

    def cleanup(self) -> None:
        """Fail-closed compatibility entrypoint for an uncommitted file."""

        receipt = self.terminate(ArtifactTerminalOutcome.ABORT)
        if not receipt.cleanup_succeeded:
            logger.warning("Failed to delete temporary file %s: %s", self.file_path, receipt.cleanup_error_code)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        """Use a strict path so terminal evidence observes filesystem failure."""

        del outcome
        self._release_authority.release()


class PartitionedFileExportArtifact(BaseExtractionArtifact):
    """A set of file exports produced by independent range partitions.

    The artifact keeps the same Source -> Sink contract as a single
    ``FileExportArtifact`` while allowing sources and sinks to fan out native
    export/import work. Every partition owns one physical file, so failed
    partitions can be retried by higher-level orchestration without rebuilding
    the full export.
    """

    def __init__(
        self,
        partitions: Sequence[FileExportArtifact],
        columns: Sequence[str],
        *,
        max_workers: int = 1,
        estimated_rows: int | None = None,
        manifest: Any | None = None,
        manifest_store: Any | None = None,
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self.partitions = list(partitions)
        self.columns = list(columns)
        self.max_workers = max(1, int(max_workers))
        self.manifest = manifest
        self.manifest_store = manifest_store

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            load_from_file = getattr(staging_manager, "load_from_file")
            inserted = self.load_with(lambda file_artifact: load_from_file(handle, file_artifact))
            handle.row_count = inserted
            return handle

    def load_with(self, loader: Callable[[FileExportArtifact], int]) -> int:
        """Load every partition while retaining source evidence to the terminal boundary."""

        def load_one(index_and_artifact: tuple[int, FileExportArtifact]) -> int:
            partition_index, file_artifact = index_and_artifact
            setattr(file_artifact, "consumed_part_index", partition_index)
            try:
                if self.manifest is not None:
                    self.manifest.mark_running(partition_index)
                    self._save_manifest()
                inserted = require_target_row_count(loader(file_artifact))
                setattr(file_artifact, "rows_loaded", inserted)
                if self.manifest is not None:
                    self.manifest.mark_success(
                        partition_index,
                        rows_loaded=inserted,
                        artifact_path=file_artifact.file_path,
                    )
                    self._save_manifest()
                return inserted
            except Exception as exc:
                if self.manifest is not None:
                    self.manifest.mark_failed(partition_index, str(exc))
                    self._save_manifest()
                raise

        if self.max_workers == 1 or len(self.partitions) <= 1:
            return sum(load_one(item) for item in enumerate(self.partitions))

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            return sum(executor.map(load_one, enumerate(self.partitions)))

    def rebind_columns(self, columns: Sequence[str]) -> PartitionedFileExportArtifact:
        """Rebind every positional partition view to the same target identities."""

        rebound_columns = tuple(str(column) for column in columns)
        rebound = PartitionedFileExportArtifact(
            [partition.rebind_columns(rebound_columns) for partition in self.partitions],
            rebound_columns,
            max_workers=self.max_workers,
            estimated_rows=self.estimated_rows,
            manifest=self.manifest,
            manifest_store=self.manifest_store,
        )
        lifecycle = self.extraction_lifecycle
        if lifecycle is not None:
            rebound.bind_extraction_lifecycle(lifecycle)
        rebound.bind_terminal_authority(self.terminal_authority)
        core = {
            "_estimated_rows",
            "_extraction_lifecycle",
            "_terminal_lock",
            "_terminal_authority",
            "columns",
            "partitions",
            "max_workers",
            "manifest",
            "manifest_store",
        }
        for name, value in vars(self).items():
            if name not in core:
                setattr(rebound, name, value)
        return rebound

    def rebind_partitions(
        self,
        partitions: Sequence[FileExportArtifact],
        columns: Sequence[str],
    ) -> PartitionedFileExportArtifact:
        """Return a resource-preserving view over rewritten partition bytes."""

        rebound = PartitionedFileExportArtifact(
            partitions,
            columns,
            max_workers=self.max_workers,
            estimated_rows=self.estimated_rows,
            manifest=self.manifest,
            manifest_store=self.manifest_store,
        )
        lifecycle = self.extraction_lifecycle
        if lifecycle is not None:
            rebound.bind_extraction_lifecycle(lifecycle)
        rebound.bind_terminal_authority(self.terminal_authority)
        return rebound

    def cleanup(self) -> None:
        self.terminate(ArtifactTerminalOutcome.ABORT)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        failures = []
        for partition in self.partitions:
            receipt = partition.terminate(outcome)
            if not receipt.cleanup_succeeded:
                failures.append(receipt.cleanup_error_code or "artifact.release_failed")
        if failures:
            raise PartitionedArtifactReleaseError("partitioned_artifact.release_failed:" + ",".join(failures))

    def _should_release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> bool:
        """Freeze RETAIN on every child while preserving its evidence bytes."""

        del outcome
        return True

    def _save_manifest(self) -> None:
        if self.manifest_store is not None and self.manifest is not None:
            self.manifest_store.save(self.manifest)


from dpone.runtime.batched_file_artifact import BatchedFileExportArtifact  # noqa: E402

__all__ = [
    "ArtifactIntegrityError",
    "BatchedFileExportArtifact",
    "FileExportArtifact",
    "FileIdentity",
    "PartitionedFileExportArtifact",
]
