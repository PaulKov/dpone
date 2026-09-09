"""Shared native chunk pipeline contracts for route refresh executors."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

TAIL_LIMIT = 4000


@dataclass(frozen=True, slots=True)
class NativeChunkPrepareResult:
    """Result returned by a bounded target-window prepare step."""

    rows_deleted: int = 0
    redacted_command: tuple[str, ...] = ()
    stdout_tail: str = ""
    stderr_tail: str = ""


@dataclass(frozen=True, slots=True)
class NativeChunkExportResult:
    """Result returned by a native source chunk exporter."""

    rows_read: int
    redacted_command: tuple[str, ...] = ()
    stdout_tail: str = ""
    stderr_tail: str = ""


@dataclass(frozen=True, slots=True)
class NativeChunkLoadResult:
    """Result returned by a native target chunk loader."""

    rows_written: int
    redacted_command: tuple[str, ...] = ()
    stdout_tail: str = ""
    stderr_tail: str = ""


class NativeChunkExporter(Protocol):
    """Export one bounded source query to a local transfer file."""

    def export_chunk(self, *, query: str, output_path: Path) -> object: ...


class NativeChunkLoader(Protocol):
    """Prepare the target window and load one local transfer file."""

    def prepare_chunk(
        self, *, table: str, boundary_column: str, start: str, end: str, idempotency_key: str
    ) -> object: ...

    def load_chunk(
        self,
        *,
        table: str,
        columns: tuple[str, ...],
        input_path: Path,
        idempotency_key: str,
    ) -> object: ...


def normalize_prepare(value: object) -> NativeChunkPrepareResult:
    if isinstance(value, NativeChunkPrepareResult):
        return value
    return NativeChunkPrepareResult(
        rows_deleted=_int(_read(value, "rows_deleted"), default=0),
        redacted_command=string_tuple(_read(value, "redacted_command")),
        stdout_tail=str(_read(value, "stdout_tail", "")),
        stderr_tail=str(_read(value, "stderr_tail", "")),
    )


def normalize_export(value: object) -> NativeChunkExportResult:
    if isinstance(value, NativeChunkExportResult):
        return value
    return NativeChunkExportResult(
        rows_read=_int(_read(value, "rows_read"), default=0),
        redacted_command=string_tuple(_read(value, "redacted_command")),
        stdout_tail=str(_read(value, "stdout_tail", "")),
        stderr_tail=str(_read(value, "stderr_tail", "")),
    )


def normalize_load(value: object) -> NativeChunkLoadResult:
    if isinstance(value, NativeChunkLoadResult):
        return value
    return NativeChunkLoadResult(
        rows_written=_int(_read(value, "rows_written"), default=0),
        redacted_command=string_tuple(_read(value, "redacted_command")),
        stdout_tail=str(_read(value, "stdout_tail", "")),
        stderr_tail=str(_read(value, "stderr_tail", "")),
    )


def line_count(path: Path) -> int:
    if not path.is_file():
        return 0
    with path.open("rb") as handle:
        return sum(1 for _ in handle)


def tail(value: str) -> str:
    return value[-TAIL_LIMIT:]


def string_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,) if value else tuple()
    if isinstance(value, tuple | list):
        return tuple(str(item) for item in value if str(item))
    return tuple()


def _read(value: object, name: str, default: object = None) -> object:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _int(value: object, *, default: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return default
    return default


__all__ = [
    "NativeChunkExporter",
    "NativeChunkExportResult",
    "NativeChunkLoadResult",
    "NativeChunkLoader",
    "NativeChunkPrepareResult",
    "line_count",
    "normalize_export",
    "normalize_load",
    "normalize_prepare",
    "tail",
]
