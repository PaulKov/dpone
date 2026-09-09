"""Native spill file writers for nested normalization."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

_NATIVE_FORMATS = {"jsonl", "json_each_row", "ndjson", "tsv"}


class NativeSpillWriter:
    """Write normalized rows in target-friendly local formats."""

    def __init__(self, output_format: str = "jsonl") -> None:
        normalized = str(output_format or "jsonl").lower()
        if normalized not in _NATIVE_FORMATS:
            raise ValueError(f"Unsupported nested spill output_format `{output_format}`")
        self.output_format = normalized

    @property
    def suffix(self) -> str:
        return ".tsv" if self.output_format == "tsv" else ".jsonl"

    def write_rows(self, handle, rows: Sequence[Mapping[str, object]], schema: Sequence[tuple[str, str]]) -> None:
        if self.output_format == "tsv":
            if handle.tell() == 0:
                handle.write("\t".join(name for name, _ in schema) + "\n")
            for row in rows:
                handle.write("\t".join(_tsv_cell(row.get(name), logical_type) for name, logical_type in schema) + "\n")
            return
        for row in rows:
            encoded = {name: _json_cell(row.get(name), logical_type) for name, logical_type in schema}
            handle.write(json.dumps(encoded, ensure_ascii=False) + "\n")

    def render_file(
        self,
        rows: Iterable[Mapping[str, object]],
        target_path: Path,
        schema: Sequence[tuple[str, str]],
    ) -> None:
        """Render already-decoded rows into one final-schema native file."""

        with target_path.open("w", encoding="utf-8") as target:
            for row in rows:
                self.write_rows(target, (row,), schema)


def _tsv_cell(value: object, logical_type: str) -> str:
    if value is None:
        return "\\N"
    if logical_type == "bytes":
        if not isinstance(value, bytes):
            raise ValueError(f"Nested spill bytes column received {type(value).__name__}")
        rendered = value.hex()
    elif isinstance(value, (dict, list)):
        _reject_nested_non_json_scalars(value)
        rendered = json.dumps(value, ensure_ascii=False)
    else:
        rendered = str(value)
    if logical_type == "string" and rendered == r"\N":
        raise ValueError("Nested spill TSV string value conflicts with reserved null marker `\\N`")
    if "\t" in rendered or "\n" in rendered or "\r" in rendered:
        raise ValueError(f"Nested spill TSV value contains unsafe delimiter: {rendered!r}")
    return rendered


def _json_cell(value: object, logical_type: str) -> object:
    if value is None:
        return None
    if logical_type == "bytes":
        if not isinstance(value, bytes):
            raise ValueError(f"Nested spill bytes column received {type(value).__name__}")
        return value.hex()
    if isinstance(value, (dict, list)):
        _reject_nested_non_json_scalars(value)
    if hasattr(value, "isoformat") and logical_type in {"date", "timestamp"}:
        return value.isoformat()
    return value


def _reject_nested_non_json_scalars(value: object) -> None:
    if isinstance(value, bytes):
        raise ValueError("Nested spill JSON values cannot contain bytes losslessly; split the bytes into a column")
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_nested_non_json_scalars(item)
    elif isinstance(value, list):
        for item in value:
            _reject_nested_non_json_scalars(item)
