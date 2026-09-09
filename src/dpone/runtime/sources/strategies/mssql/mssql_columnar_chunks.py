"""Reusable MSSQL columnar chunk sizing and cleanup helpers."""

from __future__ import annotations

from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.runtime.columnar_fast_path_models import LocalColumnarChunk
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest


def batch_size(request: ColumnarSnapshotRequest) -> int:
    raw_value = _option(request, "batch_size", 10_000)
    if isinstance(raw_value, int | str):
        return int(raw_value)
    raise TypeError("batch_size must be an integer")


def initial_chunk_rows(request: ColumnarSnapshotRequest) -> int:
    raw_target = _option(request, "target_chunk_rows")
    if isinstance(raw_target, int | str) and int(raw_target) > 0:
        return clamp_rows(request, int(raw_target))
    return clamp_rows(request, max(batch_size(request) * 25, 250_000))


def next_chunk_rows(
    request: ColumnarSnapshotRequest,
    current_target_rows: int,
    row_count: int,
    size_bytes: int,
) -> int:
    if row_count <= 0 or size_bytes <= 0:
        return clamp_rows(request, current_target_rows)
    bytes_per_row = max(size_bytes / row_count, 1)
    return clamp_rows(request, int(request.target_chunk_bytes / bytes_per_row))


def cleanup_chunk(chunk: LocalColumnarChunk | None) -> None:
    if chunk is not None:
        chunk.path.unlink(missing_ok=True)


def cleanup_dir(path: Path) -> None:
    for child in path.glob("*"):
        child.unlink(missing_ok=True)
    try:
        path.rmdir()
    except OSError:
        return


def cleanup_retired_chunks(active_chunks: list[LocalColumnarChunk], limit: int) -> None:
    while len(active_chunks) > limit:
        cleanup_chunk(active_chunks.pop(0))


def file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def schema_hash(schema: Sequence[tuple[str, str]]) -> str:
    return f"sha256:{sha256(repr(tuple(schema)).encode('utf-8')).hexdigest()}"


def safe_run_id(run_id: Any) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in str(run_id))[:80] or "run"


def clamp_rows(request: ColumnarSnapshotRequest, value: int) -> int:
    return max(batch_size(request), min(int(value), max_chunk_rows(request)))


def max_chunk_rows(request: ColumnarSnapshotRequest) -> int:
    raw_value = _option(request, "max_chunk_rows", 1_000_000)
    if isinstance(raw_value, int | str) and int(raw_value) > 0:
        return int(raw_value)
    raise TypeError("max_chunk_rows must be a positive integer")


def max_inflight_chunks(request: ColumnarSnapshotRequest) -> int:
    execution = (request.options or {}).get("execution")
    execution_options = dict(execution) if isinstance(execution, dict) else {}
    direct_push = (request.options or {}).get("direct_push")
    direct_options = dict(direct_push) if isinstance(direct_push, dict) else {}
    raw_value = execution_options.get(
        "max_inflight_chunks",
        direct_options.get("max_inflight_chunks", (request.options or {}).get("max_inflight_chunks", 1)),
    )
    if isinstance(raw_value, int | str) and int(raw_value) > 0:
        return int(raw_value)
    raise TypeError("max_inflight_chunks must be a positive integer")


def _option(request: ColumnarSnapshotRequest, key: str, default: Any | None = None) -> Any:
    options = dict(request.options or {})
    execution = options.get("execution")
    execution_options = dict(execution) if isinstance(execution, dict) else {}
    return execution_options.get(key, options.get(key, default))


__all__ = [
    "batch_size",
    "cleanup_chunk",
    "cleanup_dir",
    "cleanup_retired_chunks",
    "file_sha256",
    "initial_chunk_rows",
    "max_inflight_chunks",
    "next_chunk_rows",
    "safe_run_id",
    "schema_hash",
]
