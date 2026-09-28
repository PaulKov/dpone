"""Physical chunk lifecycle for single-scan native transfers."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.physical_chunk_policy import PhysicalChunkLimitExceeded, PhysicalChunkPolicy
from dpone.runtime.physical_chunk_writer import PhysicalTransferChunk, RowBoundaryChunkWriter
from dpone.runtime.process_io import add_cleanup_failure_note
from dpone.runtime.staging import owned_staging_handle

PHYSICAL_CHUNKS_SCHEMA_VERSION = "dpone.native_transfer.physical_chunks.v1"


class PhysicalChunkedFileExportArtifact(BaseExtractionArtifact):
    """Lazy file artifact stream loaded into staging one chunk at a time."""

    extraction_completion_mode = "lazy"

    def __init__(
        self,
        *,
        chunk_generator: Callable[[], Iterable[PhysicalTransferChunk]],
        columns: Sequence[str],
        evidence_path: Path,
        format: str = "mssql-delimited",
        estimated_rows: int | None = None,
        bulk_text_codec: Any | None = None,
        bulk_wire_contract: Any | None = None,
        native_wire_contract: Any | None = None,
        source_scan_decision: Any | None = None,
        cleanup_policy: str = "on_success",
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self.chunk_generator = chunk_generator
        self.columns = tuple(columns)
        self.evidence_path = Path(evidence_path)
        self.format = format
        self.bulk_text_codec = bulk_text_codec
        self.bulk_wire_contract = bulk_wire_contract
        self.native_wire_contract = native_wire_contract
        self.source_scan_decision = source_scan_decision
        if cleanup_policy not in {"eager", "on_success", "keep_on_failure"}:
            raise ValueError("physical_chunk_artifact.cleanup_policy_invalid")
        self.cleanup_policy = cleanup_policy
        self._consumption_started = False
        self.rows_exported: int | None = None
        self._events: list[dict[str, Any]] = []
        self._generation_failure: dict[str, Any] | None = None
        self._owned_chunk_paths: set[Path] = set()
        self.source_byte_measurement_complete = False

    def rebind_generator(
        self,
        chunk_generator: Callable[[], Iterable[PhysicalTransferChunk]],
        *,
        columns: Sequence[str] | None = None,
    ) -> PhysicalChunkedFileExportArtifact:
        """Replace a lazy transform while preserving terminal ownership."""

        with self._terminal_lock:
            if self.terminal_receipt is not None:
                raise ValueError("physical_chunk_artifact.already_terminated")
            if self._consumption_started:
                raise ValueError("physical_chunk_artifact.already_materialized")
            self.chunk_generator = chunk_generator
            if columns is not None:
                self.columns = tuple(str(column) for column in columns)
        return self

    def load_with(self, loader: Callable[[FileExportArtifact], int]) -> int:
        with self._terminal_lock:
            if self.terminal_receipt is not None:
                raise ValueError("physical_chunk_artifact.already_terminated")
            if self._consumption_started:
                raise ValueError("physical_chunk_artifact.already_materialized")
            self._consumption_started = True
        lifecycle = self.extraction_lifecycle
        if lifecycle is not None and lifecycle.receipt is None:
            lifecycle.acquire()
        total_rows = 0
        chunks: Iterator[PhysicalTransferChunk] | None = None
        try:
            try:
                chunks = iter(self.chunk_generator())
            except Exception as exc:
                self._record_generation_failure(exc)
                raise
            while True:
                try:
                    chunk = next(chunks)
                except StopIteration:
                    break
                except Exception as exc:
                    self._record_generation_failure(exc)
                    raise
                self._owned_chunk_paths.add(Path(chunk.file_path))
                file_artifact = chunk.to_file_artifact(
                    self.columns,
                    bulk_text_codec=self.bulk_text_codec,
                    bulk_wire_contract=self.bulk_wire_contract,
                    native_wire_contract=self.native_wire_contract,
                )
                try:
                    loaded = int(loader(file_artifact) or 0)
                    total_rows += chunk.row_count if chunk.row_count is not None else loaded
                    self._record(chunk, "loaded_to_staging", rows_loaded=loaded)
                except Exception:
                    self._record(chunk, "failed", error_code="physical_chunk_staging_load_failed")
                    raise
                else:
                    if self.cleanup_policy == "eager":
                        chunk.cleanup()
                        self._owned_chunk_paths.discard(Path(chunk.file_path))
                        self._record(chunk, "released_after_staging_ack")
                    else:
                        self._record(chunk, "retained_until_terminal")
        except BaseException as error:
            self._close_chunks(chunks, parent_error=error)
            raise
        else:
            self._close_chunks(chunks)
            if lifecycle is not None:
                lifecycle.complete()
            self.rows_exported = total_rows
            self.source_byte_measurement_complete = True
            return total_rows
        finally:
            self._write_evidence()

    @staticmethod
    def _close_chunks(
        chunks: Iterator[PhysicalTransferChunk] | None,
        *,
        parent_error: BaseException | None = None,
    ) -> None:
        close_chunks = getattr(chunks, "close", None)
        if not callable(close_chunks):
            return
        try:
            close_chunks()
        except Exception as cleanup_error:
            if parent_error is None:
                raise
            add_cleanup_failure_note(
                parent_error,
                context="physical chunk iterator cleanup",
                cleanup_error=cleanup_error,
            )

    def materialize(
        self, staging_manager: Any, load_config: Any, schema: Sequence[tuple[str, str]]
    ) -> StagingTableArtifact:
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            inserted = self.load_with(lambda file_artifact: staging_manager.load_from_file(handle, file_artifact))
            handle.row_count = inserted
            return handle

    def cleanup(self) -> None:
        self.terminate(ArtifactTerminalOutcome.ABORT)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        del outcome
        for path in self._owned_chunk_paths:
            path.unlink(missing_ok=True)

    def _record(self, chunk: PhysicalTransferChunk, status: str, **extra: Any) -> None:
        payload = {
            "chunk_index": chunk.chunk_index,
            "status": status,
            "file_path": chunk.file_path,
            "rows": chunk.row_count,
            "bytes": chunk.byte_count,
            "checksum": chunk.checksum,
            "timestamp": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        }
        payload.update(extra)
        self._events.append(payload)
        self._write_evidence()

    def _record_generation_failure(self, error: Exception) -> None:
        safe_limit_error = _safe_limit_error(error)
        payload: dict[str, Any] = {
            "status": "generation_failed",
            "error_code": (
                PhysicalChunkLimitExceeded.code if safe_limit_error is not None else "physical_chunk_generation_failed"
            ),
            "timestamp": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        }
        if safe_limit_error is not None:
            payload.update(safe_limit_error)
        self._generation_failure = payload
        self._write_evidence()

    def _write_evidence(self) -> None:
        self.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        decision = self.source_scan_decision
        source_scan = decision.to_evidence() if decision is not None and hasattr(decision, "to_evidence") else None
        payload = {
            "schema_version": PHYSICAL_CHUNKS_SCHEMA_VERSION,
            "source_scan": source_scan,
            "chunks": self._events,
        }
        if self._generation_failure is not None:
            payload["generation_failure"] = self._generation_failure
        self.evidence_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_limit_error(error: Exception) -> dict[str, int] | None:
    if type(error) is not PhysicalChunkLimitExceeded:
        return None
    chunk_index, row_bytes, max_chunk_bytes = error.chunk_index, error.row_bytes, error.max_chunk_bytes
    values = (chunk_index, row_bytes, max_chunk_bytes)
    if any(type(value) is not int for value in values):
        return None
    if chunk_index < 0 or row_bytes <= 0 or max_chunk_bytes <= 0:
        return None
    return {
        "chunk_index": chunk_index,
        "row_bytes": row_bytes,
        "max_chunk_bytes": max_chunk_bytes,
    }


__all__ = [
    "PHYSICAL_CHUNKS_SCHEMA_VERSION",
    "PhysicalChunkLimitExceeded",
    "PhysicalChunkPolicy",
    "PhysicalChunkedFileExportArtifact",
    "PhysicalTransferChunk",
    "RowBoundaryChunkWriter",
]
