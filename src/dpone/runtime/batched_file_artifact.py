"""Lazy batched file extraction artifact."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.staging import owned_staging_handle

if TYPE_CHECKING:
    from dpone.runtime.staging import StagingManager


@dataclass
class BatchedFileExportArtifact(BaseExtractionArtifact):
    """Generate COPY files lazily and append every batch to one staging table."""

    extraction_completion_mode = "lazy"

    batch_generator: Callable[[], Any] = field(init=False)
    columns: Sequence[str] = field(init=False)
    batch_size: int = field(init=False)
    format: str = field(default="csv", init=False)
    compressed: bool = field(default=False, init=False)

    def __init__(
        self,
        batch_generator: Callable[[], Any],
        columns: Sequence[str],
        batch_size: int,
        *,
        format: str = "csv",
        compressed: bool = False,
        estimated_rows: int | None = None,
        on_success: Callable[[], None] | None = None,
        on_abort: Callable[[], None] | None = None,
        generator_owns_lifecycle_acquisition: bool = False,
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self.batch_generator = batch_generator
        self.columns = columns
        self.batch_size = batch_size
        self.format = format
        self.compressed = compressed
        self._on_success = on_success
        self._on_abort = on_abort
        self._generator_owns_lifecycle_acquisition = bool(generator_owns_lifecycle_acquisition)
        self._generated_batches: list[Any] = []

    def rebind_generator(
        self,
        batch_generator: Callable[[], Any],
        *,
        columns: Sequence[str] | None = None,
    ) -> BatchedFileExportArtifact:
        """Replace a lazy transform without changing resource ownership."""

        with self._terminal_lock:
            if self.terminal_receipt is not None:
                raise ValueError("batched_file_artifact.already_terminated")
            if self._generated_batches:
                raise ValueError("batched_file_artifact.already_materialized")
            self.batch_generator = batch_generator
            if columns is not None:
                self.columns = tuple(str(column) for column in columns)
        return self

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        """Load every lazy batch, close its generator, and publish row count."""

        with owned_staging_handle(staging_manager, load_config, schema) as staging_artifact:
            return self._materialize_into(staging_manager, staging_artifact)

    def _materialize_into(
        self,
        staging_manager: StagingManager,
        staging_artifact: StagingTableArtifact,
    ) -> StagingTableArtifact:
        """Populate one already-owned staging handle from all lazy batches."""

        from dpone.runtime.artifact_logging import etl_logger

        total_loaded = 0
        batch_num = 1
        etl_logger.log_etl_progress(
            "BATCHED_LOAD_START",
            {
                "Target_Staging": staging_artifact.qualified_name(),
                "Batch_Size": self.batch_size,
                "Format": self.format,
                "Compressed": self.compressed,
            },
        )
        lifecycle = self.extraction_lifecycle
        if lifecycle is not None and lifecycle.receipt is None and not self._generator_owns_lifecycle_acquisition:
            lifecycle.acquire()
        generator = self.batch_generator()
        try:
            for batch_artifact in generator:
                self._generated_batches.append(batch_artifact)
                etl_logger.log_etl_progress(
                    "BATCH_LOAD_START",
                    {
                        "Batch_Num": batch_num,
                        "File": batch_artifact.file_path,
                        "Staging": staging_artifact.qualified_name(),
                    },
                )
                loader = getattr(staging_manager, "load_from_file_batched", None)
                if not callable(loader):
                    loader = getattr(staging_manager, "load_from_file")
                inserted = int(loader(staging_artifact, batch_artifact))
                total_loaded += inserted
                etl_logger.log_etl_progress(
                    "BATCH_LOADED",
                    {"Batch_Num": batch_num, "Rows": inserted, "Total_Rows": total_loaded},
                )
                batch_num += 1
        except Exception as exc:
            etl_logger.log_etl_error(
                f"Batched load failed at batch {batch_num}",
                {"Batch_Num": batch_num, "Total_Loaded": total_loaded, "Error": str(exc)},
            )
            raise
        finally:
            close = getattr(generator, "close", None)
            if callable(close):
                close()

        staging_artifact.row_count = total_loaded
        if lifecycle is not None:
            lifecycle.complete()
        etl_logger.log_etl_progress(
            "BATCHED_LOAD_COMPLETE",
            {
                "Total_Batches": batch_num,
                "Total_Rows": total_loaded,
                "Staging": staging_artifact.qualified_name(),
            },
        )
        return staging_artifact

    def cleanup(self) -> None:
        """Fail-closed compatibility entrypoint for a lazy source snapshot."""

        self.terminate(ArtifactTerminalOutcome.ABORT)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        callback = self._on_success if outcome is ArtifactTerminalOutcome.SUCCESS else self._on_abort
        self._on_success = None
        self._on_abort = None
        errors: list[BaseException] = []
        for artifact in self._generated_batches:
            terminate = getattr(artifact, "terminate", None)
            if callable(terminate):
                receipt = terminate(outcome)
                if getattr(receipt, "cleanup_succeeded", True) is False:
                    errors.append(
                        RuntimeError(str(getattr(receipt, "cleanup_error_code", None) or "batch.release_failed"))
                    )
            elif outcome is not ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN:
                cleanup = getattr(artifact, "cleanup", None)
                if callable(cleanup):
                    try:
                        cleanup()
                    except BaseException as exc:
                        errors.append(exc)
        if callback is not None:
            try:
                callback()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise errors[0]

    def _should_release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> bool:
        del outcome
        return True


__all__ = ["BatchedFileExportArtifact"]
