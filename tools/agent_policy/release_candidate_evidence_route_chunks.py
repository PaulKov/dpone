"""Closed chunk and state-projection validators for release route evidence."""

from __future__ import annotations

import importlib.util
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


codec = _load_sibling("dpone_release_candidate_codec_route_chunks", "release_candidate_evidence_codec.py")
EXECUTION_KEYS = frozenset(
    {
        "ordinal",
        "idempotency_key",
        "status",
        "passed",
        "rows_read",
        "rows_written",
        "artifact_path",
        "summary",
        "blockers",
        "warnings",
        "duration_seconds",
        "start",
        "end",
        "partition",
        "source_boundary",
        "sink_boundary",
    }
)
VERIFICATION_KEYS = frozenset(
    {"ordinal", "idempotency_key", "status", "passed", "source", "sink", "summary", "blockers", "warnings"}
)
SIDE_KEYS = frozenset(
    {"row_count", "min_boundary", "max_boundary", "typed_hash", "duplicate_keys", "null_keys", "sample_rows", "summary"}
)
_LOWER_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def validate_execution_chunks(chunks: Sequence[Any], *, route: str) -> tuple[int, int]:
    """Validate exact execution chunks and return reconciled row totals."""

    rows_read = rows_written = 0
    identities: list[tuple[int, str]] = []
    for index, value in enumerate(chunks):
        field = f"{route}.execution.chunks[{index}]"
        chunk = _mapping(value, field)
        codec.require_exact_keys(chunk, EXECUTION_KEYS, field=field)
        identity = _identity(chunk, field=field)
        current_read = codec.require_nonnegative_int(chunk.get("rows_read"), field=f"{field}.rows_read")
        current_written = codec.require_nonnegative_int(chunk.get("rows_written"), field=f"{field}.rows_written")
        if (
            chunk.get("status") != "succeeded"
            or chunk.get("passed") is not True
            or chunk.get("blockers") != []
            or current_read != current_written
        ):
            raise ValueError(f"{route} execution chunk {index} is not an exact success")
        rows_read += current_read
        rows_written += current_written
        identities.append(identity)
    if len(set(identities)) != len(identities):
        raise ValueError(f"{route} execution contains duplicate chunk identities")
    return rows_read, rows_written


def validate_verification_chunks(
    chunks: Sequence[Any],
    *,
    execution_chunks: Sequence[Any],
    route: str,
) -> tuple[int, int]:
    """Validate exact verification snapshots and bind them to execution chunks."""

    execution_identities = [
        _execution_identity(value, route=route, index=index) for index, value in enumerate(execution_chunks)
    ]
    source_rows = sink_rows = 0
    verification_identities: list[tuple[int, str]] = []
    for index, value in enumerate(chunks):
        field = f"{route}.verification.chunks[{index}]"
        chunk = _mapping(value, field)
        codec.require_exact_keys(chunk, VERIFICATION_KEYS, field=field)
        source = _side(chunk.get("source"), field=f"{field}.source")
        sink = _side(chunk.get("sink"), field=f"{field}.sink")
        comparable = ("row_count", "min_boundary", "max_boundary", "typed_hash")
        if (
            chunk.get("status") != "verified"
            or chunk.get("passed") is not True
            or chunk.get("blockers") != []
            or any(source[key] != sink[key] for key in comparable)
            or source["duplicate_keys"] != 0
            or sink["duplicate_keys"] != 0
            or source["null_keys"] != 0
            or sink["null_keys"] != 0
        ):
            raise ValueError(f"{route} verification chunk {index} is not an exact match")
        source_rows += int(source["row_count"])
        sink_rows += int(sink["row_count"])
        verification_identities.append(_identity(chunk, field=field))
    if verification_identities != execution_identities:
        raise ValueError(f"{route} verification chunk identities do not match execution")
    return source_rows, sink_rows


def _execution_identity(value: Any, *, route: str, index: int) -> tuple[int, str]:
    field = f"{route}.execution.chunks[{index}]"
    chunk = _mapping(value, field)
    codec.require_exact_keys(chunk, EXECUTION_KEYS, field=field)
    return _identity(chunk, field=field)


def _identity(chunk: Mapping[str, Any], *, field: str) -> tuple[int, str]:
    return (
        codec.require_positive_int(chunk.get("ordinal"), field=f"{field}.ordinal"),
        codec.require_string(chunk.get("idempotency_key"), field=f"{field}.idempotency_key"),
    )


def _side(value: Any, *, field: str) -> Mapping[str, Any]:
    snapshot = _mapping(value, field)
    codec.require_exact_keys(snapshot, SIDE_KEYS, field=field)
    codec.require_nonnegative_int(snapshot.get("row_count"), field=f"{field}.row_count")
    codec.require_string(snapshot.get("min_boundary"), field=f"{field}.min_boundary")
    codec.require_string(snapshot.get("max_boundary"), field=f"{field}.max_boundary")
    typed_hash = codec.require_string(snapshot.get("typed_hash"), field=f"{field}.typed_hash")
    if _LOWER_SHA256.fullmatch(typed_hash) is None:
        raise ValueError(f"{field}.typed_hash must be 64 lowercase hexadecimal characters")
    codec.require_nonnegative_int(snapshot.get("duplicate_keys"), field=f"{field}.duplicate_keys")
    codec.require_nonnegative_int(snapshot.get("null_keys"), field=f"{field}.null_keys")
    codec.require_nonnegative_int(snapshot.get("sample_rows"), field=f"{field}.sample_rows")
    if not isinstance(snapshot.get("summary"), str):
        raise ValueError(f"{field}.summary must be a string")
    return snapshot


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


__all__ = ["validate_execution_chunks", "validate_verification_chunks"]
