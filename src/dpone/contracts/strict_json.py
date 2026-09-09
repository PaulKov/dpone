"""Dependency-free strict JSON primitives for identity and trust boundaries."""

from __future__ import annotations

import json
import math
from typing import Any


class StrictJsonError(ValueError):
    """JSON is ambiguous, non-finite, malformed, or has the wrong root type."""


def canonical_json_bytes(value: object) -> bytes:
    """Encode one value as deterministic, finite UTF-8 JSON bytes."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def strict_json_object(payload: bytes | str, *, allow_nonfinite: bool = False) -> dict[str, Any]:
    """Decode one duplicate-free JSON object.

    By default non-finite numbers (``NaN`` / ``Infinity``) are rejected. Pass
    ``allow_nonfinite=True`` for producer evidence that may contain metric
    extremes which a later sanitizer rewrites to ``null``.
    """

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise StrictJsonError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise StrictJsonError(f"non-finite JSON constant: {value}")

    def finite_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise StrictJsonError("non-finite JSON number")
        return parsed

    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        kwargs: dict[str, Any] = {"object_pairs_hook": unique_object}
        if not allow_nonfinite:
            kwargs["parse_constant"] = reject_constant
            kwargs["parse_float"] = finite_float
        value = json.loads(text, **kwargs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StrictJsonError("invalid JSON") from exc
    if not isinstance(value, dict):
        raise StrictJsonError("JSON root must be an object")
    return value


__all__ = ["StrictJsonError", "canonical_json_bytes", "strict_json_object"]
