"""SQL safety policy for data product assertions."""

from __future__ import annotations

import re

_FORBIDDEN_SQL = re.compile(
    r"\b(insert|alter|drop|truncate|delete|update|create|exchange|rename|attach|detach)\b", re.I
)


def safe_select_sql(sql: str) -> bool:
    normalized = sql.strip().lower()
    return (
        bool(normalized)
        and (normalized.startswith("select") or normalized.startswith("with"))
        and not _FORBIDDEN_SQL.search(normalized)
    )


__all__ = ["safe_select_sql"]
