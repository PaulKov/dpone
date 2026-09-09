"""Parse-safe validation and serialization of bounded mapping plans."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

AIRFLOW_MAPPING_ITEM_ENV = "DPONE_AIRFLOW_MAPPING_ITEM"
MAX_MAPPING_ITEMS = 200
MAX_MAPPING_ACTIVE = 64
MAX_MAPPING_ITEM_BYTES = 8 * 1024

_MODES = frozenset({"internal", "visible", "summary"})
_MAPPED_MODES = frozenset({"visible", "summary"})
_PLAN_FIELDS = frozenset(
    {
        "schema",
        "mode",
        "plan_fingerprint",
        "backfill_plan_hash",
        "chunks_total",
        "items_total",
        "limits",
        "items",
    }
)
_ITEM_FIELDS = frozenset({"item_index", "first_chunk_index", "last_chunk_index", "chunks_count"})
_POOL = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass(frozen=True, slots=True)
class ProviderMappingItem:
    item_index: int
    first_chunk_index: int
    last_chunk_index: int
    chunks_count: int

    def to_jsonable(self) -> dict[str, int]:
        return {
            "item_index": self.item_index,
            "first_chunk_index": self.first_chunk_index,
            "last_chunk_index": self.last_chunk_index,
            "chunks_count": self.chunks_count,
        }


@dataclass(frozen=True, slots=True)
class ProviderMappingPlan:
    mode: str
    plan_fingerprint: str
    backfill_plan_hash: str | None
    chunks_total: int
    max_items: int
    max_active: int
    pool: str
    items: tuple[ProviderMappingItem, ...]

    @property
    def is_mapped(self) -> bool:
        return self.mode in _MAPPED_MODES


def mapping_plan_from_pack(pack: Mapping[str, Any]) -> ProviderMappingPlan:
    """Validate the immutable plan or return the legacy internal default."""

    raw = pack.get("mapping_plan")
    if raw in (None, {}):
        return ProviderMappingPlan("internal", "", None, 0, MAX_MAPPING_ITEMS, 16, "dpone_backfill", ())
    if not isinstance(raw, Mapping) or set(raw) != _PLAN_FIELDS:
        raise _invalid("mapping plan fields do not match dpone.airflow-mapping-plan.v1")
    if raw.get("schema") != "dpone.airflow-mapping-plan.v1":
        raise _invalid("mapping plan schema is invalid")
    mode = str(raw.get("mode") or "")
    if mode not in _MODES:
        raise _invalid("mapping plan mode is invalid")
    fingerprint = _digest(raw.get("plan_fingerprint"), "plan_fingerprint")
    payload = {key: value for key, value in raw.items() if key != "plan_fingerprint"}
    if fingerprint != _fingerprint(payload):
        raise _invalid("mapping plan fingerprint does not match its canonical payload")

    limits = raw.get("limits")
    if not isinstance(limits, Mapping) or set(limits) != {"max_items", "max_active", "pool"}:
        raise _invalid("mapping plan limits are invalid")
    max_items = _bounded_int(limits.get("max_items"), "max_items", MAX_MAPPING_ITEMS)
    max_active = _bounded_int(limits.get("max_active"), "max_active", MAX_MAPPING_ACTIVE)
    if max_active > max_items:
        raise _invalid("mapping plan max_active exceeds max_items")
    pool = str(limits.get("pool") or "").strip()
    if mode in _MAPPED_MODES and not _POOL.fullmatch(pool):
        raise _invalid("mapped plan requires a valid Airflow pool")

    chunks_total = _non_negative_int(raw.get("chunks_total"), "chunks_total")
    items_total = _non_negative_int(raw.get("items_total"), "items_total")
    raw_items = raw.get("items")
    if not isinstance(raw_items, list) or len(raw_items) != items_total or items_total > max_items:
        raise _invalid("mapping plan item count is invalid")
    items = tuple(_mapping_item(item) for item in raw_items)
    _validate_coverage(mode=mode, chunks_total=chunks_total, items=items)
    backfill_hash = raw.get("backfill_plan_hash")
    if mode in _MAPPED_MODES:
        backfill_plan_hash = _digest(backfill_hash, "backfill_plan_hash")
    elif backfill_hash is None:
        backfill_plan_hash = None
    else:
        raise _invalid("internal mapping plan cannot carry a backfill plan hash")
    return ProviderMappingPlan(
        mode=mode,
        plan_fingerprint=fingerprint,
        backfill_plan_hash=backfill_plan_hash,
        chunks_total=chunks_total,
        max_items=max_items,
        max_active=max_active,
        pool=pool,
        items=items,
    )


def mapped_env_vars(base_env: Mapping[str, Any], plan: ProviderMappingPlan) -> list[dict[str, Any]]:
    """Build one complete environment mapping per bounded item."""

    if not plan.is_mapped or plan.backfill_plan_hash is None:
        raise _invalid("mapped environments require a visible or summary plan")
    environments: list[dict[str, Any]] = []
    for item in plan.items:
        payload = {
            "schema": "dpone.airflow-mapping-item.v1",
            "mode": plan.mode,
            "mapping_plan_fingerprint": plan.plan_fingerprint,
            "backfill_plan_hash": plan.backfill_plan_hash,
            **item.to_jsonable(),
        }
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        if len(encoded.encode("utf-8")) > MAX_MAPPING_ITEM_BYTES:
            raise _invalid("serialized mapping item exceeds 8 KiB")
        environment = dict(base_env)
        environment[AIRFLOW_MAPPING_ITEM_ENV] = encoded
        environments.append(environment)
    return environments


def _mapping_item(raw: object) -> ProviderMappingItem:
    if not isinstance(raw, Mapping) or set(raw) != _ITEM_FIELDS:
        raise _invalid("mapping plan item fields are invalid")
    item = ProviderMappingItem(
        item_index=_non_negative_int(raw.get("item_index"), "item_index"),
        first_chunk_index=_positive_int(raw.get("first_chunk_index"), "first_chunk_index"),
        last_chunk_index=_positive_int(raw.get("last_chunk_index"), "last_chunk_index"),
        chunks_count=_positive_int(raw.get("chunks_count"), "chunks_count"),
    )
    if item.last_chunk_index < item.first_chunk_index:
        raise _invalid("mapping plan item range is inverted")
    if item.chunks_count != item.last_chunk_index - item.first_chunk_index + 1:
        raise _invalid("mapping plan item count does not match its range")
    return item


def _validate_coverage(*, mode: str, chunks_total: int, items: tuple[ProviderMappingItem, ...]) -> None:
    if mode == "internal":
        if chunks_total or items:
            raise _invalid("internal mapping plan must not contain chunks or items")
        return
    expected = 1
    for item_index, item in enumerate(items):
        if item.item_index != item_index or item.first_chunk_index != expected:
            raise _invalid("mapping plan items are not ordered and contiguous")
        if mode == "visible" and item.chunks_count != 1:
            raise _invalid("visible mapping plan must contain one chunk per item")
        expected = item.last_chunk_index + 1
    if not items or expected - 1 != chunks_total:
        raise _invalid("mapping plan items do not cover every chunk exactly once")


def _fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _digest(value: object, field: str) -> str:
    text = str(value or "")
    digest = text.removeprefix("sha256:")
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise _invalid(f"mapping plan {field} is not a canonical SHA-256 digest")
    return "sha256:" + digest


def _bounded_int(value: object, field: str, maximum: int) -> int:
    parsed = _positive_int(value, field)
    if parsed > maximum:
        raise _invalid(f"mapping plan {field} exceeds {maximum}")
    return parsed


def _positive_int(value: object, field: str) -> int:
    parsed = _non_negative_int(value, field)
    if parsed < 1:
        raise _invalid(f"mapping plan {field} must be positive")
    return parsed


def _non_negative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _invalid(f"mapping plan {field} must be a non-negative integer")
    return value


def _invalid(message: str) -> ValueError:
    return ValueError(f"DPONE_AIRFLOW_MAPPING_PLAN_MISMATCH: {message}")


__all__ = [
    "AIRFLOW_MAPPING_ITEM_ENV",
    "ProviderMappingItem",
    "ProviderMappingPlan",
    "mapped_env_vars",
    "mapping_plan_from_pack",
]
