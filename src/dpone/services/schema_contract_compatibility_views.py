"""File-IO facade for schema contract compatibility view commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class SchemaContractCompatibilityViewFacade:
    """Thin facade; compatibility view business rules live in readiness modules."""

    def plan(self, *, manifest_path: str, against: str, consumer_matrix_path: str | None = None) -> dict[str, Any]:
        manifest = _read_mapping(Path(manifest_path))
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        base = _resolve_against(against=against, head=head)
        if base is None:
            return _blocked_plan(head, f"schema_contract_registry.against_not_found:{against}")
        return (
            _views()
            .CompatibilityViewPlanner()
            .plan(
                manifest=manifest,
                base_contract=base,
                head_contract=head,
                consumer_matrix=_optional_mapping(consumer_matrix_path),
            )
        )

    def gate(self, *, plan_path: str, consumer_gate_path: str | None = None) -> dict[str, Any]:
        plan = _read_mapping(Path(plan_path))
        return (
            _views()
            .CompatibilityViewGate()
            .evaluate(
                plan=plan,
                consumer_gate=_optional_mapping(consumer_gate_path),
                mode=str(plan.get("mode") or "gate"),
            )
        )

    def report(self, *, plan_path: str) -> dict[str, Any]:
        plan = _read_mapping(Path(plan_path))
        markdown = _markdown_report(plan)
        return {
            "schema_version": "dpone.schema_contract_compatibility_view_report.v1",
            "status": plan.get("status"),
            "contract_id": plan.get("contract_id"),
            "compatibility_view_plan_id": plan.get("compatibility_view_plan_id"),
            "markdown": markdown,
            "blockers": plan.get("blockers", []),
            "warnings": plan.get("warnings", []),
        }

    def summary(self, *, manifest_path: str) -> dict[str, Any] | None:
        manifest = _read_mapping(Path(manifest_path))
        options = _views().SchemaContractServingOptions.from_manifest(manifest)
        if not options.enabled:
            return None
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        base = _store_from_version(head).latest(contract_id=str(head["contract_id"]))
        if base is None:
            return {
                "enabled": True,
                "status": "warning",
                "contract_id": head["contract_id"],
                "version": head["version"],
                "views_count": 0,
                "blockers": [],
                "warnings": ["schema_contract_view.base_contract_not_found"],
            }
        matrix = _manual_consumer_matrix(manifest=manifest, base=base, head=head)
        plan = (
            _views()
            .CompatibilityViewPlanner()
            .plan(
                manifest=manifest,
                base_contract=base,
                head_contract=head,
                consumer_matrix=matrix,
            )
        )
        return {
            "enabled": True,
            "mode": plan.get("mode"),
            "status": plan.get("status"),
            "contract_id": plan.get("contract_id"),
            "version": plan.get("head_version"),
            "compatibility_view_plan_id": plan.get("compatibility_view_plan_id"),
            "views_count": plan.get("summary", {}).get("views_count", 0)
            if isinstance(plan.get("summary"), Mapping)
            else 0,
            "covered_versions": plan.get("summary", {}).get("covered_versions", [])
            if isinstance(plan.get("summary"), Mapping)
            else [],
            "blockers": plan.get("blockers", []),
            "warnings": plan.get("warnings", []),
            "phases": _phases(plan),
        }


def _manual_consumer_matrix(
    *, manifest: Mapping[str, Any], base: Mapping[str, Any], head: Mapping[str, Any]
) -> dict[str, Any]:
    consumers = _mapping(_schema_contract(manifest).get("consumers")).get("manual", [])
    inventory = {
        "schema_version": "dpone.schema_contract_consumer_inventory.v1",
        "contract_id": head.get("contract_id"),
        "status": "discovered",
        "blockers": [],
        "warnings": [],
        "consumers": [dict(item) for item in consumers if isinstance(item, Mapping)],
    }
    return (
        _matrix()
        .SchemaConsumerMatrixBuilder()
        .build(
            base=base,
            head=head,
            inventory=inventory,
            unknown_consumer=str(_versioning(manifest).get("unknown_consumer") or "warn"),
        )
    )


def _phases(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    if plan.get("status") != "planned":
        return []
    operations: list[dict[str, str]] = []
    validations: list[dict[str, str]] = []
    for view in plan.get("views", []):
        if not isinstance(view, Mapping):
            continue
        operations.extend(_operations("compatibility_views_expand", view.get("ddl", []), "sql"))
        validations.extend(_operations("compatibility_views_validate", view.get("validation", []), "validation"))
    return [
        {
            "name": "compatibility_views_expand",
            "operations": operations,
            "validations": [],
            "preconditions": [],
        },
        {
            "name": "compatibility_views_validate",
            "operations": [],
            "validations": validations,
            "preconditions": [],
        },
    ]


def _operations(phase: str, sql: object, operation_type: str) -> list[dict[str, str]]:
    if not isinstance(sql, list):
        return []
    return [
        {"name": f"{phase}_{index}", "operation_type": operation_type, "sql": str(item)}
        for index, item in enumerate(sql, start=1)
        if str(item)
    ]


def _markdown_report(plan: Mapping[str, Any]) -> str:
    lines = [
        "# Schema Contract Compatibility Views",
        "",
        f"- status: {plan.get('status')}",
        f"- contract_id: {plan.get('contract_id')}",
        f"- base_version: {plan.get('base_version')}",
        f"- head_version: {plan.get('head_version')}",
        "",
        "## Views",
    ]
    for view in plan.get("views", []):
        if isinstance(view, Mapping):
            lines.append(f"- {view.get('view')} ({view.get('version_constraint')}): {view.get('status')}")
    for key in ("blockers", "warnings"):
        values = [str(item) for item in plan.get(key, []) if str(item)]
        if values:
            lines.extend(["", f"## {key.title()}"])
            lines.extend(f"- {item}" for item in values)
    return "\n".join(lines) + "\n"


def _resolve_against(*, against: str, head: Mapping[str, Any]) -> dict[str, Any] | None:
    path = Path(against)
    if path.exists():
        return _read_mapping(path)
    contract_id, _, version = against.partition("@")
    if not version:
        return None
    return _store_from_version(head).get(contract_id=contract_id, version=version)


def _store_from_version(version: Mapping[str, Any]) -> Any:
    registry = _mapping(version.get("registry"))
    backend = str(registry.get("store_backend") or "local_json")
    uri = str(registry.get("store_uri") or ".dpone/schema-contracts/registry.json")
    if backend == "sqlite":
        return import_module("dpone.readiness.schema_contract_registry_sqlite").SqliteSchemaContractRegistryStore(uri)
    return import_module("dpone.readiness.schema_contract_registry_store").LocalJsonSchemaContractRegistryStore(uri)


def _blocked_plan(head: Mapping[str, Any], blocker: str) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_contract_compatibility_view_plan.v1",
        "status": "blocked",
        "contract_id": head.get("contract_id"),
        "head_version": head.get("version"),
        "head_contract_version_id": head.get("contract_version_id"),
        "views": [],
        "summary": {"views_count": 0, "covered_versions": [], "blocked_projections": 0},
        "blockers": [blocker],
        "warnings": [],
        "compatibility_view_plan_id": _fingerprint({"blocked": blocker, "head": head.get("contract_version_id")}),
    }


def _optional_mapping(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(Path(path)) if path else None


def _read_mapping(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    raw = json.loads(text) if path.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _schema_contract(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_mapping(_mapping(manifest.get("sink")).get("options")).get("schema_contract"))


def _versioning(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_schema_contract(manifest).get("versioning"))


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _fingerprint(payload: object) -> str:
    return import_module("dpone.readiness.migration_control").stable_fingerprint(payload)


def _contracts() -> Any:
    return import_module("dpone.readiness.schema_contract_registry")


def _views() -> Any:
    return import_module("dpone.readiness.schema_contract_compatibility_views")


def _matrix() -> Any:
    return import_module("dpone.readiness.schema_contract_consumer_matrix")


__all__ = ["SchemaContractCompatibilityViewFacade"]
