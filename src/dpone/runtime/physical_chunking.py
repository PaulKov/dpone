"""Physical chunk lifecycle for single-scan native transfers."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.physical_chunk_policy import PhysicalChunkLimitExceeded, PhysicalChunkPolicy
from dpone.runtime.process_io import add_cleanup_failure_note
from dpone.runtime.staging import owned_staging_handle

PHYSICAL_CHUNKS_SCHEMA_VERSION = "dpone.native_transfer.physical_chunks.v1"


@dataclass(slots=True)
class PhysicalTransferChunk:
    """One runtime-owned physical chunk produced from a parent source scan."""

    file_path: str
    chunk_index: int
    byte_count: int
    row_count: int
    checksum: str
    format: str

    def to_file_artifact(
        self,
        columns: Sequence[str],
        *,
        bulk_text_codec: Any | None = None,
        bulk_wire_contract: Any | None = None,
    ) -> FileExportArtifact:
        artifact = FileExportArtifact(
            file_path=self.file_path,
            columns=columns,
            format=self.format,
            estimated_rows=self.row_count,
            rows_exported=self.row_count,
            bulk_text_codec=bulk_text_codec,
        )
        if bulk_wire_contract is not None:
            setattr(artifact, "bulk_wire_contract", bulk_wire_contract)
        return artifact

    def cleanup(self) -> None:
        Path(self.file_path).unlink(missing_ok=True)


class RowBoundaryChunkWriter:
    """Write bytes into chunk files, sealing chunks only after row boundaries."""

    def __init__(
        self,
        *,
        policy: PhysicalChunkPolicy,
        columns: Sequence[str],
        directory: Path,
        format: str,
        row_terminator: bytes = b"\n",
    ) -> None:
        self.policy = policy
        self.columns = tuple(columns)
        self.directory = Path(directory)
        self.format = format
        self.row_terminator = row_terminator or b"\n"

    def write(self, byte_chunks: Iterable[bytes]) -> Iterator[PhysicalTransferChunk]:
        self.directory.mkdir(parents=True, exist_ok=True)
        state = _OpenChunkState(self.directory, self.format)
        pending = b""
        try:
            for data in byte_chunks:
                if not data:
                    continue
                parts = (pending + data).split(self.row_terminator)
                for row_body in parts[:-1]:
                    row = row_body + self.row_terminator
                    state, sealed = self._prepare_row(state, row)
                    if sealed is not None:
                        yield sealed
                    state.write(row)
                    if state.byte_count >= self.policy.target_chunk_bytes:
                        sealed = state.seal()
                        state = _OpenChunkState(self.directory, self.format, state.next_index)
                        yield sealed
                pending = parts[-1]
                self._check_row_limit(state, pending)
            if pending:
                state, sealed = self._prepare_row(state, pending)
                if sealed is not None:
                    yield sealed
                state.write(pending)
            if state.byte_count:
                sealed = state.seal()
                state = _OpenChunkState(self.directory, self.format, state.next_index)
                yield sealed
        finally:
            state.discard()

    def _prepare_row(
        self,
        state: _OpenChunkState,
        row: bytes,
    ) -> tuple[_OpenChunkState, PhysicalTransferChunk | None]:
        self._check_row_limit(state, row)
        if state.byte_count and state.byte_count + len(row) > self.policy.max_chunk_bytes:
            sealed = state.seal()
            state = _OpenChunkState(self.directory, self.format, state.next_index)
            return state, sealed
        return state, None

    def _check_row_limit(self, state: _OpenChunkState, row: bytes) -> None:
        if len(row) > self.policy.max_chunk_bytes:
            raise PhysicalChunkLimitExceeded(
                chunk_index=state.chunk_index,
                row_bytes=len(row),
                max_chunk_bytes=self.policy.max_chunk_bytes,
            )


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
        source_scan_decision: Any | None = None,
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self.chunk_generator = chunk_generator
        self.columns = tuple(columns)
        self.evidence_path = Path(evidence_path)
        self.format = format
        self.bulk_text_codec = bulk_text_codec
        self.bulk_wire_contract = bulk_wire_contract
        self.source_scan_decision = source_scan_decision
        self._events: list[dict[str, Any]] = []
        self._generation_failure: dict[str, Any] | None = None
        self._owned_chunk_paths: set[Path] = set()

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
            if self._owned_chunk_paths:
                raise ValueError("physical_chunk_artifact.already_materialized")
            self.chunk_generator = chunk_generator
            if columns is not None:
                self.columns = tuple(str(column) for column in columns)
        return self

    def load_with(self, loader: Callable[[FileExportArtifact], int]) -> int:
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
                )
                try:
                    loaded = int(loader(file_artifact) or 0)
                    total_rows += chunk.row_count if chunk.row_count is not None else loaded
                    self._record(chunk, "loaded_to_staging", rows_loaded=loaded)
                except Exception:
                    self._record(chunk, "failed", error_code="physical_chunk_staging_load_failed")
                    raise
                finally:
                    self._record(chunk, "retained_until_terminal")
        except BaseException as error:
            self._close_chunks(chunks, parent_error=error)
            raise
        else:
            self._close_chunks(chunks)
            if lifecycle is not None:
                lifecycle.complete()
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


class _OpenChunkState:
    def __init__(self, directory: Path, format: str, index: int = 0) -> None:
        self.directory = directory
        self.format = format
        self.chunk_index = index
        self.next_index = index + 1
        self.file_path: str | None = None
        self._handle: Any | None = None
        self._digest = hashlib.sha256()
        self.byte_count = 0
        self.row_count = 0

    def write(self, row: bytes) -> None:
        if self._handle is None:
            fd, path = tempfile.mkstemp(
                prefix=f"dpone_physical_chunk_{self.chunk_index:06d}_",
                suffix=".bcp",
                dir=self.directory,
            )
            self.file_path = path
            self._handle = os.fdopen(fd, "wb")
        self._handle.write(row)
        self._digest.update(row)
        self.byte_count += len(row)
        self.row_count += 1

    def seal(self) -> PhysicalTransferChunk:
        self.close()
        if self.file_path is None:
            raise RuntimeError("physical_chunk_empty_seal")
        return PhysicalTransferChunk(
            file_path=self.file_path,
            chunk_index=self.chunk_index,
            byte_count=self.byte_count,
            row_count=self.row_count,
            checksum=f"sha256:{self._digest.hexdigest()}",
            format=self.format,
        )

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def discard(self) -> None:
        """Close and remove an incomplete chunk owned by this state."""

        self.close()
        if self.file_path is not None:
            Path(self.file_path).unlink(missing_ok=True)
            self.file_path = None


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
