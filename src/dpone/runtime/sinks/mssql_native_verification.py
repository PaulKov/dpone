"""Finite native verification allowance for generated metadata and framing."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NativeVerificationAllowance:
    """Business byte budget plus schema-derived internal-only overhead.

    Unbounded framework columns are generated NULL placeholders. Accepting
    non-NULL values would turn this finite verification bound into a fiction.
    """

    overhead_bytes: int
    null_metadata: tuple[str, ...]

    def require_null_metadata(self, row: Any) -> None:
        if any(row[name] is not None for name in self.null_metadata):
            raise ValueError("mssql_native.unbounded_metadata_changed")


def verification_allowance(contract: Any, business: Any) -> NativeVerificationAllowance:
    """Derive native framing deltas and finite framework field capacities."""

    width = len(business.columns)
    prefix = contract.columns[:width]
    if tuple(column.name for column in prefix) != tuple(column.name for column in business.columns):
        raise ValueError("mssql_native.verification_column_mismatch")
    overhead = 0
    for column, original in zip(prefix, business.columns, strict=True):
        for field in ("storage_type", "fixed_length", "encoding", "precision", "scale"):
            if getattr(column, field) != getattr(original, field):
                raise ValueError("mssql_native.verification_type_mismatch")
        overhead += max(0, column.prefix_width - original.prefix_width)
    null_metadata = []
    for column in contract.columns[width:]:
        if not column.name.startswith("__dpone__"):
            raise ValueError("mssql_native.unowned_verification_column")
        overhead += column.prefix_width
        if column.fixed_length is not None:
            overhead += column.fixed_length
            continue
        bound = re.search(r"\((\d+)\)", column.source_type)
        if bound:
            overhead += int(bound[1]) * (2 if column.storage_type in {"nvarchar", "nchar"} else 1)
        else:
            null_metadata.append(column.name)
    return NativeVerificationAllowance(overhead, tuple(null_metadata))
