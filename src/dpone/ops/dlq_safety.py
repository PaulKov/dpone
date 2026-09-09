"""Bounded, allowlisted diagnostic projection for durable DLQ artifacts."""

from __future__ import annotations

import math
from collections.abc import Collection, Mapping


def safe_diagnostics(raw: Mapping[str, object], *, allowed_fields: Collection[str]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key in sorted(set(allowed_fields) & raw.keys()):
        value = raw[key]
        if value is None or isinstance(value, bool | int):
            result[key] = value
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("DPONE_DLQ_DIAGNOSTIC_INVALID: non-finite numbers are not allowed")
            result[key] = value
        else:
            result[key] = str(value)[:512]
    return result


__all__ = ["safe_diagnostics"]
