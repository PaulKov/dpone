"""Typed row readers for nested spill artifacts."""

from __future__ import annotations

import csv
import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path

from dpone.runtime.normalization.semantic_spill import read_semantic_rows
from dpone.runtime.normalization.spill import SpilledNormalizationResult


def spilled_rows(
    path: Path,
    output_format: str,
    schema: Sequence[tuple[str, str]] = (),
) -> Iterator[Mapping[str, object]]:
    """Read one operator-facing spill file with schema-aware scalar values."""

    if output_format == "tsv":
        yield from _tsv_rows(path, schema)
        return
    yield from _json_rows(path, schema)


def semantic_rows(
    spill: SpilledNormalizationResult,
    table_name: str,
    native_path: Path,
) -> Iterator[Mapping[str, object]]:
    """Read the lossless semantic sidecar, with a legacy native fallback."""

    semantic_path = spill.semantic_files.get(table_name)
    if semantic_path is not None:
        return read_semantic_rows(semantic_path)
    return spilled_rows(
        native_path,
        spill.formats.get(table_name, "jsonl"),
        spill.schemas[table_name],
    )


def _json_rows(
    path: Path,
    schema: Sequence[tuple[str, str]] = (),
) -> Iterator[Mapping[str, object]]:
    logical_types = dict(schema)
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                payload = json.loads(line)
                if isinstance(payload, dict):
                    yield {
                        key: _typed_native_value(value, logical_types.get(key, "string"))
                        for key, value in payload.items()
                    }


def _tsv_rows(
    path: Path,
    schema: Sequence[tuple[str, str]],
) -> Iterator[Mapping[str, object]]:
    logical_types = dict(schema)
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, dialect="excel-tab")
        expected_columns = [name for name, _logical_type in schema]
        if expected_columns and reader.fieldnames != expected_columns:
            raise ValueError(
                f"nested spill TSV header does not match schema: expected {expected_columns}, got {reader.fieldnames}"
            )
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"nested spill TSV row does not match header in {path}")
            yield {
                key: _typed_tsv_value(value, logical_types.get(key, "string"))
                for key, value in row.items()
                if key is not None and value is not None
            }


def _typed_tsv_value(value: str, logical_type: str) -> object:
    if value == r"\N":
        return None
    if logical_type == "boolean":
        normalized = value.strip().lower()
        if normalized not in {"true", "false"}:
            raise ValueError(f"invalid boolean in nested spill TSV: {value!r}")
        return normalized == "true"
    if logical_type == "integer":
        return int(value)
    if logical_type == "number":
        return float(value)
    if logical_type == "json":
        return json.loads(value)
    if logical_type == "date":
        return date.fromisoformat(value)
    if logical_type == "timestamp":
        return datetime.fromisoformat(value)
    if logical_type == "bytes":
        return bytes.fromhex(value)
    if logical_type == "null":
        raise ValueError(f"non-null value in null-only nested spill TSV column: {value!r}")
    return value


def _typed_native_value(value: object, logical_type: str) -> object:
    if not isinstance(value, str):
        return value
    if logical_type == "date":
        return date.fromisoformat(value)
    if logical_type == "timestamp":
        return datetime.fromisoformat(value)
    if logical_type == "bytes":
        return bytes.fromhex(value)
    return value


__all__ = ["semantic_rows", "spilled_rows"]
