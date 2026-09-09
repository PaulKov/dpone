"""Artifact-to-row adapter for nested normalization."""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from dpone.runtime.file_artifacts import (
    BatchedFileExportArtifact,
    FileExportArtifact,
    PartitionedFileExportArtifact,
)
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.normalization.file_rows import FileRowReader
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class ArtifactRowsReader:
    """Materialize supported extraction artifacts as row mappings."""

    def __init__(self, file_row_reader: FileRowReader | None = None) -> None:
        self._file_row_reader = file_row_reader or FileRowReader()

    def rows(self, artifact: object) -> Iterator[Mapping[str, object]]:
        if isinstance(artifact, InMemoryRowsArtifact):
            yield from (dict(row) for row in artifact._rows)
            return
        if isinstance(artifact, StreamingRowsArtifact):
            try:
                yield from (dict(row) for row in artifact._iterator)
            finally:
                artifact.cleanup()
            return
        if isinstance(artifact, (FileExportArtifact, PartitionedFileExportArtifact, BatchedFileExportArtifact)):
            yield from self._file_row_reader.rows(artifact)
            return
        raise TypeError(f"Nested normalization cannot materialize artifact type `{type(artifact).__name__}`")
