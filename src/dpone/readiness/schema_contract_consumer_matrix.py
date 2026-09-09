"""Schema contract consumer compatibility matrix and gate rules."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

CONSUMER_MATRIX_SCHEMA = "dpone.schema_contract_consumer_matrix.v1"
CONSUMER_GATE_SCHEMA = "dpone.schema_contract_consumer_gate.v1"


@dataclass(frozen=True, slots=True)
class SchemaConsumerMatrixBuilder:
    def build(
        self,
        *,
        base: Mapping[str, Any],
        head: Mapping[str, Any],
        inventory: Mapping[str, Any],
        unknown_consumer: str,
        lineage: Mapping[str, Any] | None = None,
        low_confidence_major_change: str = "block",
    ) -> dict[str, Any]:
        plan = _contract_comparator().compare(base=base, head=head, compatibility=str(head.get("compatibility")))
        removed = {str(item.get("column")) for item in plan["changes"] if item.get("change_type") == "column_removed"}
        major = plan.get("required_bump") == "major"
        rows: list[dict[str, Any]] = []
        blockers: list[str] = []
        warnings: list[str] = list(inventory.get("warnings", []))
        if lineage:
            warnings.extend(str(item) for item in lineage.get("warnings", []) if str(item))
        for consumer in _consumers(inventory, lineage):
            if not isinstance(consumer, Mapping):
                continue
            row = _consumer_row(consumer, version=str(head.get("version") or ""), removed=removed)
            if major and not row["reads"]["columns"] and unknown_consumer in {"warn", "block"}:
                signal = f"schema_contract.consumer_table_only_major:{row['id']}"
                (row["blockers"] if unknown_consumer == "block" else warnings).append(signal)
            if major and _low_confidence(row):
                signal = f"schema_contract.consumer_low_confidence_major:{row['id']}"
                (row["blockers"] if low_confidence_major_change == "block" else warnings).append(signal)
            row["status"] = "blocked" if row["blockers"] else "compatible"
            blockers.extend(row["blockers"])
            rows.append(row)
        blockers.extend(str(item) for item in inventory.get("blockers", []) if str(item))
        if lineage:
            blockers.extend(str(item) for item in lineage.get("blockers", []) if str(item))
        lineage_confidence = _lineage_module().lineage_confidence(lineage)
        payload: dict[str, Any] = {
            "schema_version": CONSUMER_MATRIX_SCHEMA,
            "status": "blocked" if blockers else "warning" if warnings else "compatible",
            "contract_id": head.get("contract_id"),
            "base_version": base.get("version"),
            "head_version": head.get("version"),
            "head_contract_version_id": head.get("contract_version_id"),
            "compatibility_plan_id": plan.get("compatibility_plan_id"),
            "required_bump": plan.get("required_bump"),
            "consumers": rows,
            "summary": {
                "consumers_count": len(rows),
                "blocked_consumers": sum(1 for row in rows if row["status"] == "blocked"),
                "lineage_confidence": lineage_confidence,
            },
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "reviewer_actions": _reviewer_actions(blockers, warnings),
        }
        payload["consumer_matrix_id"] = stable_fingerprint(payload)
        return payload


@dataclass(frozen=True, slots=True)
class SchemaConsumerMatrixGate:
    def evaluate(self, *, pack: Mapping[str, Any], matrix: Mapping[str, Any], mode: str) -> dict[str, Any]:
        blockers = list(str(item) for item in matrix.get("blockers", []) if str(item))
        warnings = list(str(item) for item in matrix.get("warnings", []) if str(item))
        blockers.extend(_stale_matrix_blockers(pack, matrix))
        if mode == "observe":
            warnings.extend(blockers)
            blockers = []
        payload: dict[str, Any] = {
            "schema_version": CONSUMER_GATE_SCHEMA,
            "status": "blocked" if blockers else "warning" if warnings else "allowed",
            "pack_id": pack.get("pack_id"),
            "contract_id": matrix.get("contract_id"),
            "consumer_matrix_id": matrix.get("consumer_matrix_id"),
            "head_contract_version_id": matrix.get("head_contract_version_id"),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
        }
        payload["consumer_gate_id"] = stable_fingerprint(payload)
        return payload


def _consumer_row(consumer: Mapping[str, Any], *, version: str, removed: set[str]) -> dict[str, Any]:
    consumer_id = str(consumer.get("id") or "unknown")
    reads = sorted(str(item) for item in _mapping(consumer.get("reads")).get("columns", []) if str(item))
    blockers: list[str] = []
    if not _constraint_allows(str(consumer.get("version_constraint") or ""), version):
        blockers.append(f"schema_contract.consumer_version_incompatible:{consumer_id}")
    blockers.extend(
        f"schema_contract.consumer_column_removed:{consumer_id}:{column}" for column in reads if column in removed
    )
    return {**dict(consumer), "id": consumer_id, "reads": {"columns": reads}, "blockers": blockers}


def _consumers(inventory: Mapping[str, Any], lineage: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], ...]:
    consumers = {
        str(item.get("id")): dict(item)
        for item in inventory.get("consumers", [])
        if isinstance(item, Mapping) and item.get("id")
    }
    for consumer in _lineage_module().consumers_from_lineage(lineage):
        current = consumers.get(str(consumer["id"]))
        consumers[str(consumer["id"])] = _merge_consumer(current, consumer) if current else consumer
    return tuple(sorted(consumers.values(), key=lambda item: str(item.get("id"))))


def _merge_consumer(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    columns = sorted(
        set(_mapping(left.get("reads")).get("columns", [])) | set(_mapping(right.get("reads")).get("columns", []))
    )
    sources = sorted(set(left.get("lineage_sources", [])) | set(right.get("lineage_sources", [])))
    return {
        **dict(left),
        "reads": {"columns": columns},
        "confidence": right.get("confidence"),
        "lineage_sources": sources,
    }


def _low_confidence(row: Mapping[str, Any]) -> bool:
    return bool(row.get("lineage_sources")) and _lineage_module().is_low_confidence(str(row.get("confidence") or ""))


def _lineage_module() -> Any:
    return import_module("dpone.readiness.schema_contract_consumer_lineage")


def _contract_comparator() -> Any:
    return import_module("dpone.readiness.schema_contract_registry").SchemaContractComparator()


def _stale_matrix_blockers(pack: Mapping[str, Any], matrix: Mapping[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    if matrix.get("schema_version") != CONSUMER_MATRIX_SCHEMA:
        blockers.append("schema_contract.consumer_matrix_invalid_schema")
    contract_summary = _mapping(pack.get("contract_compatibility_summary"))
    if contract_summary.get("contract_version_id") and matrix.get("head_contract_version_id") != contract_summary.get(
        "contract_version_id"
    ):
        blockers.append("schema_contract.consumer_matrix_contract_version_mismatch")
    consumer_summary = _mapping(pack.get("consumer_matrix_summary"))
    if consumer_summary.get("consumer_matrix_id") and matrix.get("consumer_matrix_id") != consumer_summary.get(
        "consumer_matrix_id"
    ):
        blockers.append("schema_contract.consumer_matrix_stale")
    return tuple(blockers)


def _constraint_allows(constraint: str, version: str) -> bool:
    if not constraint:
        return True
    value = _semver(version)
    if constraint.endswith(".x"):
        return value[0] == int(constraint[:-2])
    predicates = (
        (">=", lambda other: value >= other),
        (">", lambda other: value > other),
        ("<=", lambda other: value <= other),
        ("<", lambda other: value < other),
        ("==", lambda other: value == other),
    )
    for part in [item.strip() for item in constraint.split(",") if item.strip()]:
        for prefix, predicate in predicates:
            if not part.startswith(prefix):
                continue
            if not predicate(_semver(part[len(prefix) :])):
                return False
            break
    return True


def _semver(raw: str) -> tuple[int, int, int]:
    parts = [int(part) for part in raw.split(".")[:3]]
    return tuple((parts + [0, 0, 0])[:3])  # type: ignore[return-value]


def _reviewer_actions(blockers: Sequence[str], warnings: Sequence[str]) -> list[str]:
    if blockers:
        return ["Update affected consumers, widen their version constraints, or approve a major contract migration."]
    if warnings:
        return ["Review table-only or unknown consumer evidence before promotion."]
    return ["Consumer compatibility evidence is clean."]


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = [
    "CONSUMER_GATE_SCHEMA",
    "CONSUMER_MATRIX_SCHEMA",
    "SchemaConsumerMatrixBuilder",
    "SchemaConsumerMatrixGate",
]
