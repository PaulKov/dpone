"""Small strict-JSON boundary shared by parse- and task-time provider adapters."""

from __future__ import annotations

import json
import math
from typing import Any


def loads_strict_json_object(value: str) -> dict[str, Any]:
    """Parse one finite, duplicate-free JSON object."""

    payload = json.loads(
        value,
        object_pairs_hook=_unique_object,
        parse_constant=_finite_number,
        parse_float=_finite_number,
    )
    if not isinstance(payload, dict):
        raise ValueError("JSON payload must be an object")
    return payload


def _unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result = dict(items)
    if len(result) != len(items):
        raise ValueError("duplicate JSON key")
    return result


def _finite_number(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("non-finite JSON number")
    return result


__all__ = ["loads_strict_json_object"]
