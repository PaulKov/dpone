"""Phase and shadow helpers for schema migration facade."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.readiness.migration_control import ArtifactMigrationLedgerStore, MigrationLedgerRecord, MigrationPack
from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.shadow_migration import ShadowMigrationPlanner, ShadowMigrationStrategy


def resolve_migration_strategy(plan: dict[str, Any], cli_strategy: str | None) -> str:
    if cli_strategy:
        return cli_strategy
    options = plan.get("options", {})
    if isinstance(options, dict):
        migration = options.get("migration", {})
        if isinstance(migration, dict) and migration.get("strategy"):
            return str(migration["strategy"])
    return str(ShadowMigrationStrategy.BLOCK)


def normalize_actual_state(raw: dict[str, Any]) -> dict[str, Any]:
    actual = raw.get("actual", raw)
    if not isinstance(actual, dict):
        raise ValueError("actual physical design must be a JSON object")
    return PhysicalTableState.from_mapping(actual).to_dict()


def build_shadow_plan(
    *,
    desired_plan: dict[str, Any],
    actual: dict[str, Any] | None,
    diff: dict[str, Any],
    strategy: str,
) -> dict[str, Any] | None:
    if strategy != str(ShadowMigrationStrategy.SHADOW) or not actual or not diff:
        return None
    desired_raw = diff.get("desired")
    actual_raw = diff.get("actual") or actual
    if not isinstance(desired_raw, dict) or not isinstance(actual_raw, dict):
        return None
    sink_type = str(desired_plan.get("sink_type", ""))
    plan = ShadowMigrationPlanner(dialect=_shadow_dialect(sink_type)).plan(
        desired=PhysicalTableState.from_mapping(desired_raw),
        actual=PhysicalTableState.from_mapping(actual_raw),
        strategy=ShadowMigrationStrategy.SHADOW,
        changes=tuple(item for item in diff.get("changes", []) if isinstance(item, dict)),
    )
    return plan.to_dict()


def merge_shadow_blockers(existing: tuple[str, ...], shadow: dict[str, Any]) -> tuple[str, ...]:
    if not shadow.get("phases"):
        return (*existing, *(str(item) for item in shadow.get("blockers", [])))
    covered = _covered_shadow_paths(shadow)
    return tuple(blocker for blocker in existing if _blocker_path(blocker) not in covered) + tuple(
        str(item) for item in shadow.get("blockers", [])
    )


def phase_blockers(
    *,
    pack: MigrationPack,
    ledger_path: str | None,
    phase: str | None,
) -> tuple[str, ...]:
    if not phase:
        return ("migration.phase_required",)
    names = [str(item.get("name")) for item in pack.phases]
    if phase not in names:
        return (f"migration.unknown_phase:{phase}",)
    applied = applied_phases(pack_id=pack.pack_id, ledger_path=ledger_path)
    if phase in applied:
        return ()
    expected = next((name for name in names if name not in applied), None)
    if phase != expected:
        return (f"migration.phase_order_violation:{expected}",)
    return ()


def apply_phase_record(
    *,
    pack: MigrationPack,
    ledger_path: str,
    phase: str,
    executed_operations: tuple[dict[str, Any], ...] | None = None,
    environment: str | None = None,
    promotion_id: str | None = None,
) -> tuple[int, dict[str, Any]]:
    if phase in applied_phases(pack_id=pack.pack_id, ledger_path=ledger_path):
        return 0, _phase_already_applied_payload(pack, phase)
    phase_payload = migration_phase_payload(pack, phase)
    operations = executed_operations if executed_operations is not None else _phase_operations(phase_payload)
    record = MigrationLedgerRecord(
        pack_id=pack.pack_id,
        status="phase_applied",
        target=pack.target,
        desired_fingerprint=pack.desired_fingerprint,
        actual_fingerprint=pack.actual_fingerprint,
        phase=phase,
        environment=environment,
        promotion_id=promotion_id,
        warnings=pack.warnings,
        operations=operations,
    )
    ArtifactMigrationLedgerStore(Path(ledger_path)).append(record)
    payload = record.to_dict()
    payload["schema_version"] = "dpone.schema_migration_result.v1"
    payload["command"] = "apply"
    payload["phase"] = phase
    return 0, payload


def _phase_already_applied_payload(pack: MigrationPack, phase: str) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_migration_result.v1",
        "command": "apply",
        "status": "phase_already_applied",
        "pack_id": pack.pack_id,
        "target": pack.target.to_dict(),
        "phase": phase,
        "warnings": list(pack.warnings),
        "operations": [],
    }


def applied_phases(*, pack_id: str, ledger_path: str | None) -> tuple[str, ...]:
    if not ledger_path:
        return ()
    records = ArtifactMigrationLedgerStore(Path(ledger_path)).records()
    return tuple(
        str(record.get("phase"))
        for record in records
        if record.get("pack_id") == pack_id and record.get("status") == "phase_applied" and record.get("phase")
    )


def last_phase(*, pack_id: str, ledger_path: str | None) -> str | None:
    phases = applied_phases(pack_id=pack_id, ledger_path=ledger_path)
    return phases[-1] if phases else None


def _covered_shadow_paths(shadow: dict[str, Any]) -> set[str]:
    projection = shadow.get("projection", {})
    if not isinstance(projection, dict):
        return {"engine", "partition_by", "order_by", "primary_key", "ttl"}
    columns = projection.get("items", [])
    column_paths = {
        f"columns.{item.get('column')}.target_type"
        for item in columns
        if isinstance(item, dict) and item.get("decision") == "cast"
    }
    return {"engine", "partition_by", "order_by", "primary_key", "ttl", *column_paths}


def migration_phase_payload(pack: MigrationPack, phase: str) -> dict[str, Any]:
    for item in pack.phases:
        if item.get("name") == phase:
            return dict(item)
    raise ValueError(f"unknown migration phase: {phase}")


def _phase_operations(phase_payload: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    return tuple(
        dict(item)
        for key in ("operations", "validations")
        for item in phase_payload.get(key, [])
        if isinstance(item, dict)
    )


def _blocker_path(blocker: str) -> str:
    return blocker.split(":", 1)[1] if ":" in blocker else blocker


def _shadow_dialect(sink_type: str) -> Any:
    if str(sink_type).lower() == "clickhouse":
        module = import_module("dpone.runtime.sinks.clickhouse_shadow_migration")
        return module.ClickHouseShadowMigrationDialect()
    raise ValueError(f"shadow migration is implemented for clickhouse in v1, got {sink_type}")


__all__ = [
    "applied_phases",
    "apply_phase_record",
    "build_shadow_plan",
    "last_phase",
    "merge_shadow_blockers",
    "migration_phase_payload",
    "normalize_actual_state",
    "phase_blockers",
    "resolve_migration_strategy",
]
