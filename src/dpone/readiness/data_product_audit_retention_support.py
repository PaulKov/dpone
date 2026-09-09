"""Shared helpers for data product audit retention contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any


def canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def evidence_id(payload: Mapping[str, Any]) -> str | None:
    for key, value in payload.items():
        if str(key).endswith("_id") and value:
            return str(value)
    return None


def hash_chain(refs: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    previous = ""
    chain: list[dict[str, str]] = []
    for ref in refs:
        digest = sha256(f"{previous}|{ref.get('kind')}|{ref.get('sha256')}".encode())
        chain.append({"kind": str(ref.get("kind")), "previous": previous, "hash": digest})
        previous = digest
    return chain


def merkle_root(digests: Sequence[str]) -> str:
    if not digests:
        return sha256(b"")
    layer = sorted(digests)
    while len(layer) > 1:
        layer = [sha256(f"{left}|{right}".encode()) for left, right in pairs(layer)]
    return layer[0]


def pairs(values: Sequence[str]) -> list[tuple[str, str]]:
    return [
        (values[index], values[index + 1] if index + 1 < len(values) else values[index])
        for index in range(0, len(values), 2)
    ]


def sha256(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def retention(min_days: int, delete_after_days: int, expired_policy: str, observed: datetime) -> dict[str, Any]:
    return {
        "min_days": min_days,
        "delete_after_days": delete_after_days,
        "retain_until": (observed + timedelta(days=min_days)).isoformat().replace("+00:00", "Z"),
        "delete_after": (observed + timedelta(days=delete_after_days)).isoformat().replace("+00:00", "Z"),
        "expired_policy": expired_policy,
    }


def product(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    sink = manifest.get("sink")
    options = sink.get("options") if isinstance(sink, Mapping) else {}
    raw = options.get("data_product") if isinstance(options, Mapping) else {}
    return raw if isinstance(raw, Mapping) else {}


def product_ref(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key in ("id", "owner", "tier", "criticality") if (value := raw.get(key)) is not None}


def product_id(payload: Mapping[str, Any]) -> str | None:
    raw = payload.get("product")
    if isinstance(raw, Mapping) and raw.get("id"):
        return str(raw["id"])
    return str(payload.get("product_id")) if payload.get("product_id") else None


def mappings(raw: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in raw or () if isinstance(item, Mapping))


def strings(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, str):
        return (raw,)
    return tuple(str(item) for item in raw or () if str(item))


def bool_value(raw: Any, default: bool) -> bool:
    if raw is None:
        return default
    return raw if isinstance(raw, bool) else str(raw).strip().lower() in {"1", "true", "yes", "on"}


def int_value(raw: Any, default: int) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def parse_time(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        value = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    return value if value.tzinfo else value.replace(tzinfo=UTC)


__all__ = [
    "bool_value",
    "canonical_bytes",
    "evidence_id",
    "hash_chain",
    "int_value",
    "mappings",
    "merkle_root",
    "parse_time",
    "product",
    "product_id",
    "product_ref",
    "retention",
    "sha256",
    "strings",
]
