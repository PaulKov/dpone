"""Fail-closed checks for streaming dict-row extracts.

Defense in depth for connector bugs that drop business keys while sinks still
attach lineage and pass row-count DQ. Empty mappings would otherwise load as
all-NULL business columns (observed on ClickHouse → MSSQL BCP routes).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def assert_dict_rows_preserve_columns(
    rows: Sequence[Mapping[str, Any]],
    *,
    expected_columns: Sequence[str],
) -> None:
    """Ensure the first non-empty dict batch still carries business columns."""

    if not expected_columns or not rows:
        return

    sample = rows[0]
    if not sample:
        raise RuntimeError(
            "streaming extract produced empty row mappings; "
            "business columns would load as NULL while lineage may still attach"
        )

    missing = [column for column in expected_columns if column not in sample]
    if missing:
        raise RuntimeError(f"streaming extract missing expected columns: {missing!r}")


__all__ = ["assert_dict_rows_preserve_columns"]
