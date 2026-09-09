"""Public facade for schema impact and dependency contracts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class SchemaImpactFacade:
    def __init__(self, extractor: Any | None = None) -> None:
        self._extractor = extractor

    def plan(
        self,
        *,
        pack: Any,
        manifest_path: str | None = None,
        options: Any | None = None,
    ) -> dict[str, Any]:
        migration_control = import_module("dpone.readiness.migration_control")
        analysis = import_module("dpone.readiness.schema_impact_analysis")
        models = import_module("dpone.readiness.schema_impact_models")
        providers = import_module("dpone.readiness.schema_impact_providers")

        manifest = _load_manifest(manifest_path)
        resolved = options or models.SchemaImpactOptions.from_config(_sink_options(manifest).get("schema_impact", {}))
        graph = providers.load_dependency_graph(
            providers.providers_from_options(
                resolved.to_dict(),
                base_path=Path(manifest_path).parent if manifest_path else Path("."),
                default_owner=_default_owner(resolved),
                manifest=manifest,
                manifest_path=Path(manifest_path) if manifest_path else None,
            )
        )
        blockers = list(graph.blockers)
        warnings = list(graph.warnings)
        if resolved.unknown_dependency == "block":
            missing = [item for item in warnings if ".missing:" in item]
            blockers.extend(missing)
            warnings = [item for item in warnings if item not in missing]
        if not graph.nodes and resolved.empty_graph == "warn":
            warnings.append("schema_impact.dependency_graph_empty")
        if not graph.nodes and resolved.empty_graph == "block":
            blockers.append("schema_impact.dependency_graph_empty")
        extractor = self._extractor or analysis.MigrationPackChangeExtractor()
        subjects = extractor.extract(pack)
        payload = analysis.SchemaImpactAnalyzer(required_for=resolved.required_for).analyze(
            pack=pack,
            subjects=subjects,
            graph=graph,
        )
        payload["blockers"] = list(payload.get("blockers", [])) + blockers
        payload["warnings"] = list(payload.get("warnings", [])) + warnings
        payload.update(
            {
                "schema_version": models.SCHEMA_IMPACT_PLAN_SCHEMA,
                "command": "impact_plan",
                "status": "blocked" if payload["blockers"] else "planned",
                "mode": resolved.mode,
                "options": resolved.to_dict(),
            }
        )
        payload["impact_plan_id"] = migration_control.stable_fingerprint(_fingerprint_payload(payload))
        return payload

    def plan_from_paths(self, *, pack_path: str, manifest_path: str | None = None) -> dict[str, Any]:
        migration_control = import_module("dpone.readiness.migration_control")

        return self.plan(
            pack=migration_control.MigrationPack.from_mapping(migration_control.read_json_object(pack_path)),
            manifest_path=manifest_path,
        )

    def gate_from_paths(
        self,
        *,
        pack_path: str,
        impact_path: str,
        approval_path: str | None = None,
    ) -> dict[str, Any]:
        migration_control = import_module("dpone.readiness.migration_control")
        gate = import_module("dpone.readiness.schema_impact_gate")

        return gate.SchemaImpactGate().evaluate(
            pack=migration_control.MigrationPack.from_mapping(migration_control.read_json_object(pack_path)),
            impact_plan=migration_control.read_json_object(impact_path),
            approval=_load_approval(approval_path),
        )


def __getattr__(name: str) -> Any:
    if name in {"MigrationPackChangeExtractor", "SchemaImpactAnalyzer"}:
        schema_impact_analysis = import_module("dpone.readiness.schema_impact_analysis")
        return getattr(schema_impact_analysis, name)
    if name == "SchemaImpactGate":
        return getattr(import_module("dpone.readiness.schema_impact_gate"), name)
    if name in {"SCHEMA_IMPACT_PLAN_SCHEMA", "SchemaImpactOptions"}:
        schema_impact_models = import_module("dpone.readiness.schema_impact_models")
        return getattr(schema_impact_models, name)
    raise AttributeError(name)


def _load_manifest(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise ValueError("manifest must be a mapping")
    return dict(raw)


def _load_approval(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if Path(path).suffix.lower() == ".json" else yaml.safe_load(text)
    return dict(raw) if isinstance(raw, Mapping) else {}


def _sink_options(manifest: Mapping[str, Any]) -> dict[str, Any]:
    sink = manifest.get("sink", {})
    if not isinstance(sink, Mapping):
        return {}
    options = sink.get("options", {})
    return dict(options) if isinstance(options, Mapping) else {}


def _default_owner(options: Any) -> str | None:
    owners = options.owners or {}
    return str(owners.get("default")) if owners.get("default") else None


def _fingerprint_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key not in {"impact_plan_id", "created_at", "status", "command", "schema_version"}
    }


_analysis_exports = import_module("dpone.readiness.schema_impact_analysis")
_gate_exports = import_module("dpone.readiness.schema_impact_gate")
_model_exports = import_module("dpone.readiness.schema_impact_models")

MigrationPackChangeExtractor = _analysis_exports.MigrationPackChangeExtractor
SCHEMA_IMPACT_PLAN_SCHEMA = _model_exports.SCHEMA_IMPACT_PLAN_SCHEMA
SchemaImpactGate = _gate_exports.SchemaImpactGate
SchemaImpactOptions = _model_exports.SchemaImpactOptions

__all__ = [
    "MigrationPackChangeExtractor",
    "SCHEMA_IMPACT_PLAN_SCHEMA",
    "SchemaImpactFacade",
    "SchemaImpactGate",
    "SchemaImpactOptions",
]
