"""Pure helper functions for schema migration facade orchestration."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import Any

from dpone.readiness.migration_control import MigrationPack, MigrationTarget, stable_fingerprint
from dpone.services.schema_migration_approval import approval_required, phased_approval_required
from dpone.services.schema_migration_bundle_gate import bundle_gate_apply_blockers
from dpone.services.schema_migration_impact import embedded_impact_blockers
from dpone.services.schema_migration_phases import phase_blockers


def target_from_plan(plan: dict[str, Any]) -> MigrationTarget:
    return MigrationTarget(sink_type=str(plan.get("sink_type", "unknown")), table=str(plan.get("table", "")))


def default_changes(plan: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    return (
        {
            "change_type": "create_or_verify",
            "path": "target_table",
            "desired": plan.get("table"),
            "risk": "plan_only",
            "recommendation": "review the generated physical DDL before applying in production",
        },
    )


def reconciliation_mode_override(*, cli_strategy: str | None, resolved_strategy: str) -> str | None:
    if cli_strategy == "block":
        return "block"
    if resolved_strategy in {"online_safe", "shadow"}:
        return "auto_safe"
    return None


def attach_contract_compatibility_summary(*, payload: dict[str, Any], manifest_path: str) -> dict[str, Any]:
    module = import_module("dpone.services.schema_contract_registry")
    return module.attach_contract_compatibility_summary(payload=payload, manifest_path=manifest_path)


def migration_apply_blockers(
    *,
    load_actual: Callable[[str | None], dict[str, Any] | None],
    pack: MigrationPack,
    plan_payload: dict[str, Any],
    actual_path: str | None,
    approval_path: str | None,
    ledger_path: str | None,
    phase: str | None,
    bundle_gate_path: str | None,
) -> tuple[str, ...]:
    current = load_actual(actual_path)
    if current is not None and pack.actual_fingerprint != stable_fingerprint(current):
        return ("migration.actual_fingerprint_mismatch",)
    if pack.blockers:
        return tuple(pack.blockers)
    gate_blockers = bundle_gate_apply_blockers(pack=pack, gate_path=bundle_gate_path)
    if gate_blockers:
        return gate_blockers
    impact_blockers = embedded_impact_blockers(pack=pack, plan_payload=plan_payload, approval_path=approval_path)
    if impact_blockers:
        return impact_blockers
    contract_blockers = schema_contract_apply_blockers(plan_payload)
    if contract_blockers:
        return contract_blockers
    if pack.phases and phased_approval_required(pack) and not approval_path:
        return ("migration.approval_required",)
    if pack.phases:
        return phase_blockers(pack=pack, ledger_path=ledger_path, phase=phase)
    if approval_required(pack) and not approval_path:
        return ("migration.approval_required",)
    return ()


def schema_contract_apply_blockers(plan_payload: dict[str, Any]) -> tuple[str, ...]:
    module = import_module("dpone.services.schema_contract_registry")
    return module.schema_contract_apply_blockers(plan_payload)


__all__ = [
    "attach_contract_compatibility_summary",
    "default_changes",
    "migration_apply_blockers",
    "reconciliation_mode_override",
    "target_from_plan",
]
