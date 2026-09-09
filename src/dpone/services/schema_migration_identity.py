"""Schema identity integration helpers for migration packs."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any


@dataclass(frozen=True, slots=True)
class IdentityMigrationPlan:
    payload: dict[str, Any]
    changes: tuple[dict[str, Any], ...] = ()
    phases: tuple[dict[str, Any], ...] = ()
    warnings: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()


def build_identity_migration_plan(
    *,
    identity_plan: dict[str, Any],
    sink_type: str,
    table: str,
) -> IdentityMigrationPlan:
    decisions = tuple(item for item in identity_plan.get("identity_decisions", []) if isinstance(item, dict))
    if not decisions:
        return IdentityMigrationPlan(payload=identity_plan)
    options = identity_plan.get("options", {})
    rename = options.get("rename", {}) if isinstance(options, dict) else {}
    strategy = str(rename.get("strategy", "expand_contract")) if isinstance(rename, dict) else "expand_contract"
    target_aliases = tuple(item for item in decisions if item.get("action") == "target_alias_only")
    warnings = list(str(item) for item in identity_plan.get("warnings", []) if str(item))
    blockers = list(str(item) for item in identity_plan.get("blockers", []) if str(item))
    changes = tuple(
        _change(item, strategy) for item in decisions if item.get("action") in {"rename_alias", "target_alias_only"}
    )
    phases: tuple[dict[str, Any], ...] = ()
    if strategy == "expand_contract" and target_aliases:
        phases = _dialect(sink_type).render_expand_contract_phases(table=table, decisions=target_aliases)
    elif strategy == "direct_rename" and target_aliases:
        warnings.append("schema_identity.direct_rename_unsafe: downstream consumers may break")
        phases = _dialect(sink_type).render_direct_rename_phases(table=table, decisions=target_aliases)
    elif strategy == "runtime_alias":
        warnings.append("schema_identity.runtime_alias_only: no target DDL will be generated")
    return IdentityMigrationPlan(
        payload={**identity_plan, "strategy": strategy},
        changes=changes,
        phases=phases,
        warnings=tuple(dict.fromkeys(warnings)),
        blockers=tuple(dict.fromkeys(blockers)),
    )


def _change(decision: dict[str, Any], strategy: str) -> dict[str, Any]:
    risk = "unsafe_direct_rename" if strategy == "direct_rename" else "schema_identity"
    return {
        "change_type": str(decision.get("action", "schema_identity")),
        "path": f"columns.{decision.get('canonical_name')}",
        "desired": decision.get("canonical_name"),
        "actual": decision.get("observed_name"),
        "risk": risk,
        "recommendation": _recommendation(strategy),
    }


def _recommendation(strategy: str) -> str:
    if strategy == "direct_rename":
        return "direct rename is unsafe for downstream consumers; prefer expand_contract"
    if strategy == "runtime_alias":
        return "runtime alias is temporary; plan expand/contract cleanup"
    return "apply expand/backfill/validate/contract rename lifecycle"


def _dialect(sink_type: str) -> Any:
    if str(sink_type).lower() == "clickhouse":
        module = import_module("dpone.runtime.sinks.clickhouse_schema_identity")
        return module.ClickHouseIdentityMigrationDialect()
    raise ValueError(f"schema identity migration is implemented for clickhouse in v1, got {sink_type}")


__all__ = ["IdentityMigrationPlan", "build_identity_migration_plan"]
