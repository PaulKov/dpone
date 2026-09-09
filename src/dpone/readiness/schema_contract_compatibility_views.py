"""Provider-neutral schema contract compatibility view planning."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from importlib import import_module
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

COMPATIBILITY_VIEW_PLAN_SCHEMA = "dpone.schema_contract_compatibility_view_plan.v1"
COMPATIBILITY_VIEW_GATE_SCHEMA = "dpone.schema_contract_compatibility_view_gate.v1"


@dataclass(frozen=True, slots=True)
class SchemaContractServingOptions:
    enabled: bool
    mode: str = "gate"
    default_strategy: str = "projection_view"
    unknown_mapping: str = "block"
    expired_view: str = "block"
    view_naming: str = "{schema}.{table}__contract_v{major}"
    versions: tuple[dict[str, Any], ...] = ()

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> SchemaContractServingOptions:
        raw = _schema_contract(manifest).get("serving")
        if not isinstance(raw, Mapping):
            return cls(enabled=False)
        return cls(
            enabled=bool(raw.get("enabled", False)),
            mode=str(raw.get("mode") or "gate"),
            default_strategy=str(raw.get("default_strategy") or "projection_view"),
            unknown_mapping=str(raw.get("unknown_mapping") or "block"),
            expired_view=str(raw.get("expired_view") or "block"),
            view_naming=str(raw.get("view_naming") or "{schema}.{table}__contract_v{major}"),
            versions=tuple(dict(item) for item in raw.get("versions", []) if isinstance(item, Mapping)),
        )


class CompatibilityViewPlanner:
    """Builds versioned compatibility view evidence from contract deltas."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        base_contract: Mapping[str, Any],
        head_contract: Mapping[str, Any],
        consumer_matrix: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = SchemaContractServingOptions.from_manifest(manifest)
        changes = _contract_comparator().diff(base=base_contract, head=head_contract)
        affected = _affected_versions(consumer_matrix)
        views = [_build_view(options, manifest, base_contract, head_contract, version) for version in options.versions]
        blockers = _plan_blockers(options, affected, views, changes)
        warnings = _plan_warnings(options, blockers)
        status = "blocked" if blockers else "disabled" if not options.enabled else "planned"
        payload: dict[str, Any] = {
            "schema_version": COMPATIBILITY_VIEW_PLAN_SCHEMA,
            "status": status,
            "contract_id": head_contract.get("contract_id"),
            "base_version": base_contract.get("version"),
            "head_version": head_contract.get("version"),
            "base_contract_version_id": base_contract.get("contract_version_id"),
            "head_contract_version_id": head_contract.get("contract_version_id"),
            "mode": options.mode,
            "target": dict(head_contract.get("target", {})) if isinstance(head_contract.get("target"), Mapping) else {},
            "required_bump": _required_bump(changes),
            "changes": [dict(item) for item in changes],
            "affected_versions": sorted(affected),
            "views": views,
            "summary": {
                "views_count": len(views),
                "covered_versions": sorted(_covered_versions(views)),
                "blocked_projections": sum(len(view.get("blockers", [])) for view in views),
            },
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["compatibility_view_plan_id"] = stable_fingerprint(payload)
        return payload


class CompatibilityViewGate:
    """Evaluates compatibility view coverage before release/apply."""

    def evaluate(
        self,
        *,
        plan: Mapping[str, Any],
        consumer_gate: Mapping[str, Any] | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        gate_mode = mode or str(plan.get("mode") or "gate")
        blockers = list(str(item) for item in plan.get("blockers", []) if str(item))
        warnings = list(str(item) for item in plan.get("warnings", []) if str(item))
        blockers.extend(_stale_gate_blockers(plan, consumer_gate))
        if gate_mode == "observe":
            warnings.extend(blockers)
            blockers = []
        payload: dict[str, Any] = {
            "schema_version": COMPATIBILITY_VIEW_GATE_SCHEMA,
            "status": "blocked" if blockers else "warning" if warnings else "allowed",
            "contract_id": plan.get("contract_id"),
            "pack_id": _optional(consumer_gate, "pack_id"),
            "compatibility_view_plan_id": plan.get("compatibility_view_plan_id"),
            "consumer_gate_id": _optional(consumer_gate, "consumer_gate_id"),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["compatibility_view_gate_id"] = stable_fingerprint(payload)
        return payload


def _build_view(
    options: SchemaContractServingOptions,
    manifest: Mapping[str, Any],
    base: Mapping[str, Any],
    head: Mapping[str, Any],
    version: Mapping[str, Any],
) -> dict[str, Any]:
    view_name = str(version.get("view") or _default_view_name(options, manifest, version))
    mappings = _mapping(version.get("columns"))
    projections: list[dict[str, Any]] = []
    blockers: list[str] = []
    warnings: list[str] = []
    for column in base.get("columns", []):
        if not isinstance(column, Mapping):
            continue
        projection = _projection(column, head, mappings)
        if projection.get("blocker"):
            signal = str(projection["blocker"])
            (blockers if options.unknown_mapping == "block" else warnings).append(signal)
            continue
        projections.append({key: projection[key] for key in ("column", "expression", "kind")})
    expiry = _expired_blocker(view_name, str(version.get("remove_after") or ""))
    if expiry:
        (blockers if options.expired_view == "block" else warnings).append(expiry)
    ddl = [_render_clickhouse_view(view_name, _table_name(manifest), projections)] if not blockers else []
    return {
        "view": view_name,
        "version_constraint": str(version.get("constraint") or ""),
        "source_contract": version.get("source_contract"),
        "remove_after": version.get("remove_after"),
        "owner": version.get("owner"),
        "strategy": options.default_strategy,
        "status": "blocked" if blockers else "planned",
        "projections": projections,
        "ddl": ddl,
        "validation": [f"SELECT count() FROM {_quote_table(view_name)} LIMIT 1"] if ddl else [],
        "blockers": blockers,
        "warnings": warnings,
    }


def _projection(
    base_column: Mapping[str, Any], head: Mapping[str, Any], configured: Mapping[str, Any]
) -> dict[str, Any]:
    base_name = str(base_column.get("name") or "")
    config = _mapping(configured.get(base_name))
    from_name = str(config.get("from") or "")
    cast = str(config.get("cast") or "")
    head_by_name = _head_columns_by_name(head)
    head_by_identity = _head_columns_by_identity(head)
    target = from_name or base_name
    if target in head_by_name:
        kind = "alias" if from_name and from_name != base_name else "direct"
        return _projection_payload(base_name, target, kind, cast)
    identity = str(base_column.get("identity_id") or "")
    if identity and identity in head_by_identity:
        head_name = str(head_by_identity[identity].get("name"))
        return _projection_payload(base_name, head_name, "alias", cast)
    if config.get("expression"):
        return {"column": base_name, "expression": str(config["expression"]), "kind": "expression"}
    if config.get("default") is not None:
        return {"column": base_name, "expression": repr(config["default"]), "kind": "default"}
    return _projection_payload(base_name, base_name, "direct", cast)


def _projection_payload(base_name: str, expression: str, kind: str, cast: str) -> dict[str, str]:
    if cast:
        return {"column": base_name, "expression": f"CAST({_quote_identifier(expression)}, '{cast}')", "kind": "cast"}
    return {"column": base_name, "expression": expression, "kind": kind}


def _plan_blockers(
    options: SchemaContractServingOptions,
    affected_versions: set[str],
    views: Sequence[Mapping[str, Any]],
    changes: Sequence[Mapping[str, Any]],
) -> list[str]:
    if not options.enabled:
        return (
            []
            if _required_bump(changes) != "major"
            else [f"schema_contract_view.version_uncovered:{item}" for item in sorted(affected_versions)]
        )
    blockers = [str(item) for view in views for item in view.get("blockers", []) if str(item)]
    covered = _covered_versions(views)
    if _required_bump(changes) == "major":
        blockers.extend(f"schema_contract_view.version_uncovered:{item}" for item in affected_versions - covered)
    return blockers


def _plan_warnings(options: SchemaContractServingOptions, blockers: Sequence[str]) -> list[str]:
    if options.mode == "observe":
        return list(blockers)
    return []


def _affected_versions(consumer_matrix: Mapping[str, Any] | None) -> set[str]:
    versions: set[str] = set()
    if not consumer_matrix:
        return versions
    for consumer in consumer_matrix.get("consumers", []):
        if not isinstance(consumer, Mapping):
            continue
        constraint = str(consumer.get("version_constraint") or "")
        if constraint:
            versions.add(constraint)
    return versions


def _covered_versions(views: Sequence[Mapping[str, Any]]) -> set[str]:
    return {str(view.get("version_constraint")) for view in views if view.get("status") != "blocked"}


def _required_bump(changes: Sequence[Mapping[str, Any]]) -> str:
    if any(change.get("bump") == "major" for change in changes):
        return "major"
    if any(change.get("bump") == "minor" for change in changes):
        return "minor"
    return "patch"


def _expired_blocker(view_name: str, remove_after: str) -> str | None:
    if not remove_after:
        return None
    try:
        expired = date.fromisoformat(remove_after) < date.today()
    except ValueError:
        return f"schema_contract_view.remove_after_invalid:{view_name}"
    return f"schema_contract_view.expired:{view_name}" if expired else None


def _render_clickhouse_view(view_name: str, table_name: str, projections: Sequence[Mapping[str, Any]]) -> str:
    select = ", ".join(_projection_sql(item) for item in projections)
    return f"CREATE OR REPLACE VIEW {_quote_table(view_name)} AS SELECT {select} FROM {_quote_table(table_name)}"


def _projection_sql(projection: Mapping[str, Any]) -> str:
    expression = str(projection.get("expression") or "")
    kind = str(projection.get("kind") or "")
    rendered = _quote_identifier(expression) if kind in {"direct", "alias", "configured"} else expression
    return f"{rendered} AS {_quote_identifier(str(projection.get('column')))}"


def _quote_table(name: str) -> str:
    return ".".join(_quote_identifier(part) for part in name.split(".") if part)


def _quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _head_columns_by_name(head: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {str(column.get("name")): column for column in head.get("columns", []) if isinstance(column, Mapping)}


def _head_columns_by_identity(head: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(column.get("identity_id")): column
        for column in head.get("columns", [])
        if isinstance(column, Mapping) and column.get("identity_id")
    }


def _default_view_name(
    options: SchemaContractServingOptions, manifest: Mapping[str, Any], version: Mapping[str, Any]
) -> str:
    table = _table_name(manifest)
    schema, _, name = table.rpartition(".")
    major = str(version.get("constraint") or "0").split(".", maxsplit=1)[0]
    return options.view_naming.format(schema=schema, table=name, major=major)


def _table_name(manifest: Mapping[str, Any]) -> str:
    sink = _mapping(manifest.get("sink"))
    table = sink.get("table")
    if isinstance(table, Mapping):
        return ".".join(str(item) for item in (table.get("schema"), table.get("name")) if item)
    return str(table or sink.get("target_table") or "unknown")


def _schema_contract(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_mapping(_mapping(manifest.get("sink")).get("options")).get("schema_contract"))


def _stale_gate_blockers(plan: Mapping[str, Any], consumer_gate: Mapping[str, Any] | None) -> list[str]:
    blockers: list[str] = []
    if plan.get("schema_version") != COMPATIBILITY_VIEW_PLAN_SCHEMA:
        blockers.append("schema_contract_view.plan_invalid_schema")
    if consumer_gate and consumer_gate.get("consumer_matrix_id"):
        plan_matrix_id = plan.get("consumer_matrix_id")
        if plan_matrix_id and plan_matrix_id != consumer_gate.get("consumer_matrix_id"):
            blockers.append("schema_contract_view.consumer_gate_matrix_mismatch")
    return blockers


def _recommendations(blockers: Sequence[str], warnings: Sequence[str]) -> list[str]:
    if blockers:
        return ["Add projection view coverage or migrate pinned consumers before applying the breaking contract."]
    if warnings:
        return ["Review compatibility view warnings before promotion."]
    return ["Serve compatibility views until consumer matrix proves older pins can be retired."]


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _optional(payload: Mapping[str, Any] | None, key: str) -> Any:
    return payload.get(key) if payload else None


def _contract_comparator() -> Any:
    return import_module("dpone.readiness.schema_contract_registry").SchemaContractComparator()


__all__ = [
    "COMPATIBILITY_VIEW_GATE_SCHEMA",
    "COMPATIBILITY_VIEW_PLAN_SCHEMA",
    "CompatibilityViewGate",
    "CompatibilityViewPlanner",
    "SchemaContractServingOptions",
]
