"""Pure contracts for bounded Airflow mapping over a backfill plan.

This module deliberately has no Airflow, connector, state-store or environment
dependencies. Build tooling and the runtime use the same deterministic plan,
while adapters only serialize or transport the selected item.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.backfill.execution_policy import normalize_backfill_execution_policy, plan_hash
from dpone.backfill.planner import backfill_run_key, plan_chunks
from dpone.backfill.portable_scope_runtime import is_mssql_backfill_route
from dpone.config.load_strategy import LoadStrategy
from dpone.config.mssql_strategy_contract import normalize_mssql_backfill_campaign_strategy
from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.connector_declarations import canonical_connector_id

AIRFLOW_MAPPING_ITEM_ENV = "DPONE_AIRFLOW_MAPPING_ITEM"
AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY = "airflow_mapping_selection"
AIRFLOW_MAPPING_ITEM_MAX_BYTES = 8 * 1024
DEFAULT_MAPPING_MAX_ITEMS = 200
DEFAULT_MAPPING_MAX_ACTIVE = 16
DEFAULT_MAPPING_POOL = "dpone_backfill"
MAX_MAPPING_ITEMS = 200
MAX_MAPPING_ACTIVE = 64

_MAPPED_MODES = frozenset({"visible", "summary"})
_SUPPORTED_MODES = frozenset({"internal", *_MAPPED_MODES})
_DISTRIBUTED_STATE_SINKS = frozenset({"mssql", "postgres"})
_POOL_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
_ITEM_FIELDS = frozenset(
    {
        "schema",
        "mode",
        "mapping_plan_fingerprint",
        "backfill_plan_hash",
        "item_index",
        "first_chunk_index",
        "last_chunk_index",
        "chunks_count",
    }
)


class AirflowBackfillMappingViolation(ValueError):
    """Stable, secret-free contract violation for bounded mapping."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class AirflowMappingPolicy:
    """Normalized orchestration policy declared for one workload."""

    mode: str = "internal"
    max_items: int = DEFAULT_MAPPING_MAX_ITEMS
    max_active: int = DEFAULT_MAPPING_MAX_ACTIVE
    pool: str = DEFAULT_MAPPING_POOL

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "max_items": self.max_items,
            "max_active": self.max_active,
            "pool": self.pool,
        }


@dataclass(frozen=True, slots=True)
class AirflowMappingItemRange:
    """One ordered and contiguous range in a static mapping plan."""

    item_index: int
    first_chunk_index: int
    last_chunk_index: int
    chunks_count: int

    @property
    def chunk_indexes(self) -> tuple[int, ...]:
        return tuple(range(self.first_chunk_index, self.last_chunk_index + 1))

    def to_jsonable(self) -> dict[str, int]:
        return {
            "item_index": self.item_index,
            "first_chunk_index": self.first_chunk_index,
            "last_chunk_index": self.last_chunk_index,
            "chunks_count": self.chunks_count,
        }


@dataclass(frozen=True, slots=True)
class AirflowBackfillMappingPlan:
    """Fingerprint-addressed static mapping plan stored in a workload pack."""

    mode: str
    plan_fingerprint: str
    backfill_plan_hash: str | None
    chunks_total: int
    items_total: int
    limits: AirflowMappingPolicy
    items: tuple[AirflowMappingItemRange, ...]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "schema": "dpone.airflow-mapping-plan.v1",
            "mode": self.mode,
            "plan_fingerprint": self.plan_fingerprint,
            "backfill_plan_hash": self.backfill_plan_hash,
            "chunks_total": self.chunks_total,
            "items_total": self.items_total,
            "limits": {
                "max_items": self.limits.max_items,
                "max_active": self.limits.max_active,
                "pool": self.limits.pool,
            },
            "items": [item.to_jsonable() for item in self.items],
        }


@dataclass(frozen=True, slots=True)
class BackfillExecutionSelection:
    """Validated runtime scope derived from one provider mapping item."""

    mode: str
    mapping_plan_fingerprint: str
    backfill_plan_hash: str
    item_index: int
    first_chunk_index: int
    last_chunk_index: int
    chunks_count: int

    @property
    def chunk_indexes(self) -> tuple[int, ...]:
        return tuple(range(self.first_chunk_index, self.last_chunk_index + 1))

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "schema": "dpone.airflow-mapping-item.v1",
            "mode": self.mode,
            "mapping_plan_fingerprint": self.mapping_plan_fingerprint,
            "backfill_plan_hash": self.backfill_plan_hash,
            "item_index": self.item_index,
            "first_chunk_index": self.first_chunk_index,
            "last_chunk_index": self.last_chunk_index,
            "chunks_count": self.chunks_count,
        }


def mapping_policy_from_mapping(raw: Mapping[str, Any] | None) -> AirflowMappingPolicy:
    """Normalize and validate the public ``airflow.mapping`` declaration."""

    mapping = raw or {}
    if not isinstance(mapping, Mapping):
        raise _policy_violation("airflow.mapping must be a mapping")
    unknown = sorted(set(str(key) for key in mapping) - {"mode", "max_items", "max_active", "pool"})
    if unknown:
        raise _policy_violation("airflow.mapping contains unsupported fields: " + ", ".join(unknown))

    mode = str(mapping.get("mode") or "internal").strip().lower()
    if mode not in _SUPPORTED_MODES:
        raise _policy_violation("airflow.mapping.mode must be one of: internal, summary, visible")
    max_items = _bounded_int(mapping.get("max_items", DEFAULT_MAPPING_MAX_ITEMS), "max_items", MAX_MAPPING_ITEMS)
    max_active = _bounded_int(mapping.get("max_active", DEFAULT_MAPPING_MAX_ACTIVE), "max_active", MAX_MAPPING_ACTIVE)
    if max_active > max_items:
        raise _policy_violation("airflow.mapping.max_active must be <= max_items")
    pool = str(mapping.get("pool") if "pool" in mapping else DEFAULT_MAPPING_POOL).strip()
    if mode in _MAPPED_MODES and (not pool or not _POOL_PATTERN.fullmatch(pool)):
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_POOL_REQUIRED",
            "airflow.mapping.pool must be a non-empty Airflow identifier for visible or summary mode",
        )
    if pool and not _POOL_PATTERN.fullmatch(pool):
        raise _policy_violation("airflow.mapping.pool must contain only letters, numbers, dot, dash or underscore")
    return AirflowMappingPolicy(mode=mode, max_items=max_items, max_active=max_active, pool=pool)


def build_airflow_mapping_plan(
    load_config: LoadConfig,
    raw_mapping: Mapping[str, Any] | None,
) -> AirflowBackfillMappingPlan:
    """Compile a bounded, deterministic plan without connector or state I/O."""

    policy = mapping_policy_from_mapping(raw_mapping)
    if load_config.load_strategy is LoadStrategy.BACKFILL and is_mssql_backfill_route(load_config):
        normalize_mssql_backfill_campaign_strategy(load_config)
    if policy.mode == "internal":
        return _build_plan(policy=policy, backfill_plan_hash=None, chunks_total=0, items=())

    backfill = _mapped_backfill_options(load_config)
    execution_policy = normalize_backfill_execution_policy(backfill)
    spec = execution_policy.chunk
    if spec is None:
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_REQUIRES_CHUNKED_BACKFILL",
            "visible or summary mapping requires backfill.chunk",
        )
    if execution_policy.parallel_workers != 1:
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_PARALLELISM_MULTIPLIER",
            "visible or summary mapping requires backfill.parallel_workers: 1",
        )
    _require_distributed_state(load_config, backfill)

    dataset = f"{load_config.target_schema}.{load_config.target_table}"
    inner_mode = execution_policy.inner_mode
    run_key = str(backfill.get("backfill_id") or backfill_run_key(dataset=dataset, spec=spec, inner_mode=inner_mode))
    chunks = plan_chunks(spec, run_key=run_key)
    if inner_mode == "full_refresh" and len(chunks) > 1:
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_REQUIRES_CHUNKED_BACKFILL",
            "multi-chunk backfill cannot use full_refresh as its inner mode",
        )
    if policy.mode == "visible" and len(chunks) > policy.max_items:
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_TASK_LIMIT_EXCEEDED",
            f"visible mapping produces {len(chunks)} items above max_items={policy.max_items}",
        )

    ranges = (
        _visible_ranges(len(chunks)) if policy.mode == "visible" else _summary_ranges(len(chunks), policy.max_items)
    )
    return _build_plan(
        policy=policy,
        backfill_plan_hash="sha256:" + plan_hash(chunks, execution_policy=execution_policy),
        chunks_total=len(chunks),
        items=ranges,
    )


def serialize_airflow_mapping_item(plan: AirflowBackfillMappingPlan, item: AirflowMappingItemRange) -> str:
    """Serialize one bounded item for ``DPONE_AIRFLOW_MAPPING_ITEM``."""

    if plan.mode not in _MAPPED_MODES or item not in plan.items or plan.backfill_plan_hash is None:
        raise _item_violation("mapping item does not belong to a visible or summary plan")
    payload = BackfillExecutionSelection(
        mode=plan.mode,
        mapping_plan_fingerprint=plan.plan_fingerprint,
        backfill_plan_hash=plan.backfill_plan_hash,
        item_index=item.item_index,
        first_chunk_index=item.first_chunk_index,
        last_chunk_index=item.last_chunk_index,
        chunks_count=item.chunks_count,
    ).to_jsonable()
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) > AIRFLOW_MAPPING_ITEM_MAX_BYTES:
        raise _item_violation("serialized Airflow mapping item exceeds 8 KiB")
    return encoded


def parse_airflow_mapping_item_json(raw: str) -> BackfillExecutionSelection:
    """Parse an untrusted runtime mapping item and reject extra fields."""

    if not isinstance(raw, str) or not raw.strip():
        raise _item_violation("Airflow mapping item must be non-empty JSON")
    if len(raw.encode("utf-8")) > AIRFLOW_MAPPING_ITEM_MAX_BYTES:
        raise _item_violation("Airflow mapping item exceeds 8 KiB")
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise _item_violation("Airflow mapping item is not valid JSON") from exc
    if not isinstance(payload, dict) or set(payload) != _ITEM_FIELDS:
        raise _item_violation("Airflow mapping item fields do not match dpone.airflow-mapping-item.v1")
    if payload.get("schema") != "dpone.airflow-mapping-item.v1" or payload.get("mode") not in _MAPPED_MODES:
        raise _item_violation("Airflow mapping item schema or mode is invalid")
    for field in ("mapping_plan_fingerprint", "backfill_plan_hash"):
        if not is_canonical_sha256_digest(payload.get(field)):
            raise _item_violation(f"Airflow mapping item {field} must be a canonical SHA-256 digest")
    values = {name: _positive_or_zero_int(payload.get(name), name) for name in _ITEM_FIELDS & _INTEGER_FIELDS}
    first = values["first_chunk_index"]
    last = values["last_chunk_index"]
    count = values["chunks_count"]
    if first < 1 or last < first or count != last - first + 1:
        raise _item_violation("Airflow mapping item chunk range is invalid")
    return BackfillExecutionSelection(
        mode=str(payload["mode"]),
        mapping_plan_fingerprint=str(payload["mapping_plan_fingerprint"]),
        backfill_plan_hash=str(payload["backfill_plan_hash"]),
        item_index=values["item_index"],
        first_chunk_index=first,
        last_chunk_index=last,
        chunks_count=count,
    )


_INTEGER_FIELDS = frozenset({"item_index", "first_chunk_index", "last_chunk_index", "chunks_count"})


def _mapped_backfill_options(load_config: LoadConfig) -> Mapping[str, Any]:
    if load_config.load_strategy is not LoadStrategy.BACKFILL:
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_REQUIRES_CHUNKED_BACKFILL",
            "visible or summary mapping requires sink.strategy.mode: backfill",
        )
    backfill = (load_config.options or {}).get("backfill")
    if not isinstance(backfill, Mapping):
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_REQUIRES_CHUNKED_BACKFILL",
            "visible or summary mapping requires a backfill declaration",
        )
    return backfill


def _require_distributed_state(load_config: LoadConfig, backfill: Mapping[str, Any]) -> None:
    state = backfill.get("state")
    sink_type = canonical_connector_id(str((load_config.options or {}).get("sink_type") or ""))
    valid = (
        isinstance(state, Mapping)
        and state.get("backend") == "audit_schema"
        and state.get("require_distributed_lock") is True
        and sink_type in _DISTRIBUTED_STATE_SINKS
    )
    if not valid:
        raise AirflowBackfillMappingViolation(
            "DPONE_AIRFLOW_MAPPING_STATE_UNSUPPORTED",
            "visible or summary mapping requires certified Postgres or MSSQL distributed audit state",
        )


def _visible_ranges(chunks_total: int) -> tuple[AirflowMappingItemRange, ...]:
    return tuple(AirflowMappingItemRange(index - 1, index, index, 1) for index in range(1, chunks_total + 1))


def _summary_ranges(chunks_total: int, max_items: int) -> tuple[AirflowMappingItemRange, ...]:
    items_total = min(chunks_total, max_items)
    size, remainder = divmod(chunks_total, items_total)
    cursor = 1
    items: list[AirflowMappingItemRange] = []
    for item_index in range(items_total):
        chunks_count = size + (1 if item_index < remainder else 0)
        last = cursor + chunks_count - 1
        items.append(AirflowMappingItemRange(item_index, cursor, last, chunks_count))
        cursor = last + 1
    return tuple(items)


def _build_plan(
    *,
    policy: AirflowMappingPolicy,
    backfill_plan_hash: str | None,
    chunks_total: int,
    items: tuple[AirflowMappingItemRange, ...],
) -> AirflowBackfillMappingPlan:
    payload = {
        "schema": "dpone.airflow-mapping-plan.v1",
        "mode": policy.mode,
        "backfill_plan_hash": backfill_plan_hash,
        "chunks_total": chunks_total,
        "items_total": len(items),
        "limits": {
            "max_items": policy.max_items,
            "max_active": policy.max_active,
            "pool": policy.pool,
        },
        "items": [item.to_jsonable() for item in items],
    }
    return AirflowBackfillMappingPlan(
        mode=policy.mode,
        plan_fingerprint=canonical_fingerprint(payload),
        backfill_plan_hash=backfill_plan_hash,
        chunks_total=chunks_total,
        items_total=len(items),
        limits=policy,
        items=items,
    )


def _bounded_int(value: object, field: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise _policy_violation(f"airflow.mapping.{field} must be an integer from 1 to {maximum}")
    return value


def _positive_or_zero_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _item_violation(f"Airflow mapping item {field} must be a non-negative integer")
    return value


def _policy_violation(message: str) -> AirflowBackfillMappingViolation:
    return AirflowBackfillMappingViolation("DPONE_AIRFLOW_MAPPING_POLICY_INVALID", message)


def _item_violation(message: str) -> AirflowBackfillMappingViolation:
    return AirflowBackfillMappingViolation("DPONE_AIRFLOW_MAPPING_ITEM_INVALID", message)


__all__ = [
    "AIRFLOW_MAPPING_ITEM_ENV",
    "AIRFLOW_MAPPING_ITEM_MAX_BYTES",
    "AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY",
    "AirflowBackfillMappingPlan",
    "AirflowBackfillMappingViolation",
    "AirflowMappingItemRange",
    "AirflowMappingPolicy",
    "BackfillExecutionSelection",
    "build_airflow_mapping_plan",
    "mapping_policy_from_mapping",
    "parse_airflow_mapping_item_json",
    "serialize_airflow_mapping_item",
]
