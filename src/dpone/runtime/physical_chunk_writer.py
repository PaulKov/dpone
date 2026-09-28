"""Row-boundary file writing for bounded physical transfer chunks."""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.physical_chunk_policy import PhysicalChunkLimitExceeded, PhysicalChunkPolicy


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
