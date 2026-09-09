"""Route-level schema evolution evidence aggregation."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import (
    RouteKey,
    RouteProfile,
    RouteSchemaApplyDecision,
    RouteSchemaEvolutionReport,
)


class RouteSchemaEvolutionService:
    """Evaluate schema evolution evidence for one source -> sink route."""

    def __init__(self, *, catalog: RouteProfileCatalog | None = None) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        schema_evolution_json: str | Path,
    ) -> RouteSchemaEvolutionReport:
        directory = Path(output_dir)
        key = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(key)
        payload = _read_json(schema_evolution_json)
        blockers = _blockers(key=key, profile=profile, payload=payload)
        warnings = tuple(str(item) for item in _sequence(payload.get("warnings")))
        change = _mapping_or_empty(payload.get("change"))
        plan = _mapping_or_empty(payload.get("plan"))
        apply_decision = _apply_decision(blockers=blockers, change=change, plan=plan)
        report = RouteSchemaEvolutionReport(
            route=key,
            profile=profile,
            passed=not blockers,
            level=_level(profile=profile, blockers=blockers),
            blockers=blockers,
            warnings=warnings,
            next_actions=_next_actions(blockers, apply_decision),
            change=change,
            plan=plan,
            apply_decision=apply_decision,
            upstream_artifacts={"schema_evolution_json": str(schema_evolution_json)},
            output_dir=str(directory),
            json_path=str(directory / "route_schema_evolution.json"),
            markdown_path=str(directory / "route_schema_evolution.md"),
        )
        report.write()
        return report


def _blockers(
    *,
    key: RouteKey,
    profile: RouteProfile | None,
    payload: Mapping[str, Any],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if profile is None:
        blockers.append(f"route.unsupported:{key.colon_id}")
    blockers.extend(str(item) for item in _sequence(payload.get("blockers")))
    if not bool(payload.get("passed", False)):
        blockers.append("route_schema_evolution.upstream_not_passed")
    if _route_mismatch(key, payload):
        blockers.append("route_schema_evolution.route_mismatch")
    return tuple(dict.fromkeys(blockers))


def _apply_decision(
    *,
    blockers: tuple[str, ...],
    change: Mapping[str, object],
    plan: Mapping[str, object],
) -> RouteSchemaApplyDecision:
    ddl_preview = str(plan.get("target_ddl_preview", ""))
    requires_approval = bool(change.get("breaking")) or bool(plan.get("backfill_required"))
    if blockers:
        return RouteSchemaApplyDecision(
            mode="blocked",
            safe_to_apply=False,
            requires_approval=requires_approval,
            ddl_preview=ddl_preview,
            reason="schema evolution has blockers",
        )
    safe_to_apply = (
        str(plan.get("compatibility_level", "")).lower() in {"compatible", "backward_compatible"}
        and bool(plan.get("ddl_dry_run_passed"))
        and bool(plan.get("type_widening_safe", True))
        and bool(plan.get("offset_schema_ordering_safe", True))
        and bool(ddl_preview)
        and not requires_approval
    )
    return RouteSchemaApplyDecision(
        mode="auto_apply" if safe_to_apply else "manual_approval",
        safe_to_apply=safe_to_apply,
        requires_approval=not safe_to_apply,
        ddl_preview=ddl_preview,
        reason="safe additive schema change" if safe_to_apply else "manual approval is required",
    )


def _route_mismatch(key: RouteKey, payload: Mapping[str, Any]) -> bool:
    route = payload.get("route")
    if not isinstance(route, Mapping):
        return False
    artifact = RouteKey.of(
        source=str(route.get("source", "")),
        sink=str(route.get("sink", "")),
        strategy=str(route.get("strategy", key.strategy)),
    )
    return artifact != key


def _level(*, profile: RouteProfile | None, blockers: tuple[str, ...]) -> str:
    if profile is None:
        return "unknown"
    return "blocked" if blockers else "ready"


def _next_actions(
    blockers: tuple[str, ...],
    apply_decision: RouteSchemaApplyDecision,
) -> tuple[str, ...]:
    if not blockers and apply_decision.mode == "auto_apply":
        return tuple()
    actions: list[str] = []
    if apply_decision.mode == "manual_approval":
        actions.append("Review and approve the route schema change before advancing route state.")
    for blocker in blockers:
        if blocker.startswith("route.unsupported"):
            actions.append("Add the route to the integration matrix before evaluating schema evolution.")
        elif blocker == "route_schema_evolution.route_mismatch":
            actions.append("Regenerate schema evolution evidence for the same source, sink, and strategy.")
        else:
            actions.append(f"Open the upstream schema evolution artifact and resolve `{blocker}`.")
    return tuple(dict.fromkeys(actions))


def _read_json(path: str | Path) -> Mapping[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("Route schema evolution artifact must be a JSON object")
    return payload


def _mapping_or_empty(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {}


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(value)
    return (value,)


__all__ = ["RouteSchemaEvolutionService"]
