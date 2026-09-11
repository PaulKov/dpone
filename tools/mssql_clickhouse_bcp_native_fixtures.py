"""BCP-native wide type fixtures for MSSQL -> ClickHouse certification."""

from __future__ import annotations

import sys
from pathlib import Path

from dpone.adapters.nonproduction_bcp_fixture_recipe import bcp_fixture_columns

_TOOLS_DIR = Path(__file__).resolve().parent
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

import mssql_clickhouse_wide_type_certification as base  # noqa: E402


def build_bcp_native_columns(column_count: int) -> list[base.WideTypeColumn]:
    """Return the original tool DTOs from the canonical expression inventory."""
    return [
        base.WideTypeColumn(column.name, column.mssql_type, column.insert_expression)
        for column in bcp_fixture_columns(column_count)
    ]


__all__ = ["build_bcp_native_columns"]
