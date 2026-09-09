"""Bounded-memory external sorting for exact runtime quality records."""

from __future__ import annotations

import heapq
import json
import os
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import ExitStack
from pathlib import Path
from typing import BinaryIO

_DEFAULT_CHUNK_BYTES = 16 * 1024 * 1024


class ExternalRecordSpool:
    """Append records sequentially and expose one bounded-memory sorted pass."""

    def __init__(self, directory: Path, *, prefix: str, chunk_bytes: int = _DEFAULT_CHUNK_BYTES) -> None:
        if chunk_bytes < 1:
            raise ValueError("external_record_sort.chunk_bytes_invalid")
        descriptor, raw_path = tempfile.mkstemp(prefix=prefix, suffix=".jsonl", dir=directory)
        os.close(descriptor)
        self._source_path = Path(raw_path)
        self._source_path.chmod(0o600)
        self._writer: BinaryIO | None = self._source_path.open("wb", buffering=_DEFAULT_CHUNK_BYTES)
        self._chunk_bytes = chunk_bytes
        self._run_paths: list[Path] = []
        self._consumed = False

    def append(self, fields: Sequence[str]) -> None:
        """Append one exact string tuple without retaining it in memory."""

        if self._writer is None or self._consumed:
            raise RuntimeError("external_record_sort.spool_closed")
        encoded = json.dumps(tuple(fields), ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        self._writer.write(encoded + b"\n")

    def sorted_records(self) -> Iterator[tuple[str, ...]]:
        """Yield all records once in deterministic lexical order."""

        if self._consumed:
            raise RuntimeError("external_record_sort.spool_already_consumed")
        self._consumed = True
        self._close_writer()
        try:
            self._create_sorted_runs()
            with ExitStack() as stack:
                streams = [
                    stack.enter_context(path.open("rb", buffering=_DEFAULT_CHUNK_BYTES)) for path in self._run_paths
                ]
                for line in heapq.merge(*streams):
                    value = json.loads(line)
                    if not isinstance(value, list) or any(not isinstance(field, str) for field in value):
                        raise RuntimeError("external_record_sort.record_invalid")
                    yield tuple(value)
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Remove source and run files after success or failure."""

        self._close_writer()
        for path in (self._source_path, *self._run_paths):
            path.unlink(missing_ok=True)

    def _close_writer(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def _create_sorted_runs(self) -> None:
        with self._source_path.open("rb", buffering=_DEFAULT_CHUNK_BYTES) as source:
            chunk: list[bytes] = []
            size = 0
            for line in source:
                chunk.append(line)
                size += len(line)
                if size >= self._chunk_bytes:
                    self._write_run(chunk)
                    chunk = []
                    size = 0
            if chunk:
                self._write_run(chunk)

    def _write_run(self, lines: list[bytes]) -> None:
        lines.sort()
        descriptor, raw_path = tempfile.mkstemp(
            prefix=".dpone-external-sort-run-",
            suffix=".jsonl",
            dir=self._source_path.parent,
        )
        path = Path(raw_path)
        try:
            with os.fdopen(descriptor, "wb", buffering=_DEFAULT_CHUNK_BYTES) as target:
                target.writelines(lines)
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        path.chmod(0o600)
        self._run_paths.append(path)


__all__ = ["ExternalRecordSpool"]
