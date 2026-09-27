"""Closed, explainable layout authority for MSSQL target-local verification."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any


@dataclass(frozen=True, slots=True)
class TargetLocalRawLayout:
    """One exact native-wire layout admitted by the v1 SQL digest kernel."""

    source_type: str
    storage_type: str
    prefix_widths: tuple[int, ...]
    fixed_length: int | None
    precision: int | None = None
    scale: int | None = None
    encoding: str | None = None

    def admits(self, column: Any) -> bool:
        return (
            column.source_type.strip().lower().removesuffix(" nullable") == self.source_type
            and column.prefix_width in self.prefix_widths
            and all(
                getattr(column, field) == value
                for field, value in asdict(self).items()
                if field not in {"source_type", "prefix_widths"}
            )
        )


@dataclass(frozen=True, slots=True)
class TargetLocalLayoutMatrixV1:
    """Content-addressed intersection implemented by the P1 raw verifier."""

    raw_layouts: tuple[TargetLocalRawLayout, ...]
    max_business_columns: int = 100
    prepared_framework_types: tuple[str, ...] = (
        "datetime2(7)",
        "int",
        "nvarchar(max)",
        "varchar(26)",
        "varchar(32)",
        "varchar(64)",
    )

    def admits(self, column: Any) -> bool:
        return any(layout.admits(column) for layout in self.raw_layouts)

    def to_payload(self) -> dict[str, Any]:
        base: dict[str, Any] = {
            "schema_version": 1,
            "kind": "dpone.mssql-target-local-layout-matrix",
            "max_business_columns": self.max_business_columns,
            "raw_layouts": [
                {**asdict(layout), "prefix_widths": list(layout.prefix_widths)} for layout in self.raw_layouts
            ],
            "prepared_framework_types": list(self.prepared_framework_types),
        }
        digest = sha256(json.dumps(base, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return {**base, "capability_digest": digest}

    def artifact(self, source_commit: str) -> dict[str, Any]:
        if re.fullmatch(r"[0-9a-f]{40}", source_commit) is None:
            raise ValueError("mssql_native.layout_matrix_source_commit")
        return {**self.to_payload(), "source_commit": source_commit}


TARGET_LOCAL_LAYOUT_MATRIX_V1 = TargetLocalLayoutMatrixV1(
    raw_layouts=(
        TargetLocalRawLayout("bigint", "bigint", (0, 1), 8),
        TargetLocalRawLayout("float(53)", "float", (0, 1), 8, precision=53),
        TargetLocalRawLayout("nvarchar(max)", "nvarchar", (8,), None, encoding="utf-16le"),
        TargetLocalRawLayout("datetime2(6)", "datetime2", (0, 1), 8, scale=7),
    )
)


__all__ = ["TARGET_LOCAL_LAYOUT_MATRIX_V1", "TargetLocalLayoutMatrixV1", "TargetLocalRawLayout"]
