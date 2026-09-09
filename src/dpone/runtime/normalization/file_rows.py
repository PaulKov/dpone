"""Row readers for local file export artifacts used by nested normalization."""

from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any

from dpone.runtime.file_artifacts import BatchedFileExportArtifact, FileExportArtifact, PartitionedFileExportArtifact


class FileRowReader:
    """Streams rows from local file artifacts and cleans temporary files."""

    def rows(self, artifact: object) -> Iterator[Mapping[str, object]]:
        if isinstance(artifact, FileExportArtifact):
            yield from self._file_rows(artifact)
            return
        if isinstance(artifact, PartitionedFileExportArtifact):
            for partition in artifact.partitions:
                yield from self._file_rows(partition)
            return
        if isinstance(artifact, BatchedFileExportArtifact):
            for batch in artifact.batch_generator():
                yield from self._file_rows(batch)
            return
        raise TypeError(f"unsupported file artifact for nested normalization: {type(artifact).__name__}")

    def _file_rows(self, artifact: FileExportArtifact) -> Iterator[Mapping[str, object]]:
        try:
            file_format = str(artifact.format or "csv").strip().lower()
            if file_format in {"jsonl", "ndjson"}:
                yield from self._jsonl_rows(artifact)
            elif file_format == "json":
                yield from self._json_rows(artifact)
            elif file_format in {"csv", "tsv", "tab", "tabseparated", "tab_separated"}:
                yield from self._delimited_rows(artifact, delimiter=_delimiter(file_format))
            else:
                raise ValueError(
                    "nested normalization supports local csv, tsv, jsonl, ndjson and json file exports; "
                    f"got format={artifact.format!r}"
                )
        finally:
            artifact.cleanup()

    def _delimited_rows(self, artifact: FileExportArtifact, *, delimiter: str) -> Iterator[Mapping[str, object]]:
        columns = [str(column) for column in artifact.columns]
        if not columns:
            raise ValueError("file export artifact must expose columns for nested normalization")
        with _open_text(artifact) as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            first = True
            for record in reader:
                if first and _is_header(record, columns):
                    first = False
                    continue
                first = False
                if len(record) != len(columns):
                    raise ValueError(
                        f"file row has {len(record)} fields but artifact exposes {len(columns)} columns: {artifact.file_path}"
                    )
                yield {column: _parse_nested_cell(value) for column, value in zip(columns, record, strict=True)}

    def _jsonl_rows(self, artifact: FileExportArtifact) -> Iterator[Mapping[str, object]]:
        with _open_text(artifact) as handle:
            for line_number, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                value = json.loads(stripped)
                if not isinstance(value, Mapping):
                    raise ValueError(f"JSONL line {line_number} must be an object: {artifact.file_path}")
                yield dict(value)

    def _json_rows(self, artifact: FileExportArtifact) -> Iterator[Mapping[str, object]]:
        with _open_text(artifact) as handle:
            value = json.load(handle)
        if isinstance(value, Mapping):
            yield dict(value)
            return
        if isinstance(value, list):
            for index, item in enumerate(value):
                if not isinstance(item, Mapping):
                    raise ValueError(f"JSON array item {index} must be an object: {artifact.file_path}")
                yield dict(item)
            return
        raise ValueError(f"JSON export must contain an object or array of objects: {artifact.file_path}")


@contextmanager
def _open_text(artifact: FileExportArtifact) -> Iterator[IO[str]]:
    path = Path(artifact.file_path)
    if artifact.compressed or path.suffix == ".gz":
        handle = gzip.open(path, mode="rt", encoding="utf-8", newline="")
    else:
        handle = path.open(mode="r", encoding="utf-8", newline="")
    try:
        yield handle
    finally:
        handle.close()


def _delimiter(file_format: str) -> str:
    return "\t" if file_format in {"tsv", "tab", "tabseparated", "tab_separated"} else ","


def _is_header(record: Sequence[str], columns: Sequence[str]) -> bool:
    return [cell.strip().lower() for cell in record] == [column.strip().lower() for column in columns]


def _parse_nested_cell(value: str) -> object:
    stripped = value.strip()
    if not stripped:
        return value
    if not (stripped.startswith("{") or stripped.startswith("[")):
        return value
    parsed: Any = json.loads(stripped)
    return parsed if isinstance(parsed, Mapping | list) else value
