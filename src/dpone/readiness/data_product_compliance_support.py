"""Shared helpers for data product compliance contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

PASSING_STATUSES = {
    "allowed",
    "approved",
    "certified",
    "healthy",
    "passed",
    "ready",
    "rendered",
    "resolved",
    "signed",
    "stable",
    "usable",
    "verified",
    "waived",
    "warning",
}


def product(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    sink = manifest.get("sink")
    options = sink.get("options") if isinstance(sink, Mapping) else {}
    raw = options.get("data_product") if isinstance(options, Mapping) else {}
    return raw if isinstance(raw, Mapping) else {}


def product_ref(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in {
            "id": raw.get("id"),
            "owner": raw.get("owner"),
            "tier": raw.get("tier"),
            "criticality": raw.get("criticality"),
        }.items()
        if value is not None
    }


def product_id(payload: Mapping[str, Any]) -> str | None:
    raw = payload.get("product")
    if isinstance(raw, Mapping) and raw.get("id"):
        return str(raw["id"])
    return str(payload.get("product_id")) if payload.get("product_id") else None


def evidence_id(payload: Mapping[str, Any]) -> str | None:
    for key, value in payload.items():
        if str(key).endswith("_id") and value:
            return str(value)
    return None


def allowed_statuses(raw: Any, required: Sequence[str]) -> dict[str, list[str]]:
    source = raw if isinstance(raw, Mapping) else {}
    return {kind: list(strings(source.get(kind))) or sorted(PASSING_STATUSES) for kind in required}


def first_value(evidence: Mapping[str, Mapping[str, Any]], key: str) -> str | None:
    for payload in evidence.values():
        if isinstance(payload, Mapping) and payload.get(key):
            return str(payload[key])
    return None


def recommendations(status: str) -> list[str]:
    if status == "blocked":
        return ["Resolve failed compliance controls or attach valid waiver and authority evidence."]
    if status == "warning":
        return ["Review warning controls before regulated release closeout."]
    return ["Record compliance gate and audit package in the evidence registry."]


def mappings(raw: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in raw or () if isinstance(item, Mapping))


def strings(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    if isinstance(raw, Sequence) and not isinstance(raw, bytes):
        return tuple(str(item) for item in raw if str(item))
    return (str(raw),)


def bool_value(raw: Any, default: bool) -> bool:
    if raw is None:
        return default
    return raw if isinstance(raw, bool) else str(raw).strip().lower() in {"1", "true", "yes", "on"}


def optional_int(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def parse_time(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        value = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    return value if value.tzinfo else value.replace(tzinfo=UTC)


__all__ = [
    "PASSING_STATUSES",
    "allowed_statuses",
    "bool_value",
    "evidence_id",
    "first_value",
    "mappings",
    "optional_int",
    "parse_time",
    "product",
    "product_id",
    "product_ref",
    "recommendations",
    "strings",
]
