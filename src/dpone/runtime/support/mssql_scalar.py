"""Strict SQL Server scalar decoders that preserve valid zero values."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def single_int(rows: Sequence[Any], field: str, *, error_code: str) -> int:
    """Decode exactly one integer cell without truthiness fallbacks."""

    if len(rows) != 1 or not isinstance(rows[0], Mapping) or field not in rows[0]:
        raise RuntimeError(error_code)
    value = rows[0][field]
    if value is None or isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(error_code)
    return int(value)


__all__ = ["single_int"]
