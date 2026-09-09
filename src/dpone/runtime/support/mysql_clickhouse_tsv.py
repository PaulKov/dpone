"""Python-side ClickHouse TabSeparated encoding for MySQL extracts."""

from __future__ import annotations

import json
from datetime import date, datetime
from datetime import time as dt_time
from decimal import Decimal
from typing import Any


def format_clickhouse_tsv_scalar(value: Any) -> str:
    """Render one MySQL cell as a ClickHouse TabSeparated field."""

    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float | Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.microsecond:
            return value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dt_time):
        return value.isoformat()
    if isinstance(value, bytes | bytearray | memoryview):
        return escape_clickhouse_tsv_text(bytes(value).hex())
    if isinstance(value, dict | list):
        return escape_clickhouse_tsv_text(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
    return escape_clickhouse_tsv_text(str(value))


def escape_clickhouse_tsv_text(value: str) -> str:
    """Escape TabSeparated special characters (backslash, tab, newline, CR)."""

    return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


__all__ = ["escape_clickhouse_tsv_text", "format_clickhouse_tsv_scalar"]
