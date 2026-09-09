from __future__ import annotations

from typing import Any


def estimated_read_bytes_schema(*, minimum: int = 0) -> dict[str, Any]:
    return {"type": ["integer", "null"], "minimum": minimum}


def full_scan_positive_estimate_guard() -> dict[str, Any]:
    return {
        "if": {
            "properties": {
                "status": {"const": "planned"},
                "mode": {"const": "full_scan"},
            },
            "required": ["status", "mode"],
        },
        "then": {
            "required": ["estimated_read_bytes"],
            "properties": {
                "estimated_read_bytes": {"type": "integer", "minimum": 1},
            },
        },
    }


__all__ = ["estimated_read_bytes_schema", "full_scan_positive_estimate_guard"]
