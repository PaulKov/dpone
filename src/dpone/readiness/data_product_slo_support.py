"""Small shared helpers for data product SLO contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def target(sink: Mapping[str, Any]) -> dict[str, Any]:
    return {"sink_type": sink.get("type"), "table": target_table(sink)}


def target_table(sink: Mapping[str, Any]) -> str | None:
    table = sink.get("table")
    if isinstance(table, Mapping):
        schema = table.get("schema")
        name = table.get("name")
        return f"{schema}.{name}" if schema and name else str(name or schema or "")
    return str(table) if table else None


def mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def optional(payload: Mapping[str, Any] | None, key: str) -> Any:
    return payload.get(key) if isinstance(payload, Mapping) else None


def optional_string(raw: object) -> str | None:
    return str(raw) if raw not in {None, ""} else None


def number(*raw_values: object) -> float | None:
    for raw in raw_values:
        if raw in {None, ""}:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def recommendations(blockers: Sequence[object], warnings: Sequence[object] = ()) -> list[str]:
    if blockers:
        return ["Resolve SLO blockers before closing the data product release."]
    if warnings:
        return ["Review SLO warnings and attach them to the release evidence bundle."]
    return ["Use this SLO evidence as a production closeout check."]


__all__ = [
    "mapping",
    "number",
    "optional",
    "optional_string",
    "recommendations",
    "target",
    "target_table",
]
