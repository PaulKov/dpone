"""Schema-impact integration helpers for migration control."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.migration_control import MigrationPack
from dpone.readiness.schema_impact import SchemaImpactFacade, SchemaImpactGate, SchemaImpactOptions


def attach_impact_summary(
    *,
    payload: dict[str, Any],
    manifest_path: str,
) -> dict[str, Any]:
    options = impact_options_from_manifest(manifest_path)
    if not options.enabled:
        return payload
    pack = MigrationPack.from_mapping(payload)
    return {
        **payload,
        "impact_summary": SchemaImpactFacade().plan(pack=pack, manifest_path=manifest_path, options=options),
    }


def embedded_impact_blockers(
    *,
    pack: MigrationPack,
    plan_payload: Mapping[str, Any],
    approval_path: str | None,
) -> tuple[str, ...]:
    impact = plan_payload.get("impact_summary")
    if not isinstance(impact, Mapping):
        return ()
    result = SchemaImpactGate().evaluate(pack=pack, impact_plan=impact, approval=_load_approval(approval_path))
    return tuple(str(item) for item in result.get("blockers", []) if item)


def impact_options_from_manifest(manifest_path: str) -> SchemaImpactOptions:
    raw = yaml.safe_load(Path(manifest_path).read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise ValueError("manifest must be a mapping")
    sink = raw.get("sink", {})
    options = sink.get("options", {}) if isinstance(sink, Mapping) else {}
    schema_impact = options.get("schema_impact", {}) if isinstance(options, Mapping) else {}
    return SchemaImpactOptions.from_config(schema_impact if isinstance(schema_impact, Mapping) else {})


def _load_approval(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    raw_text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(raw_text) if Path(path).suffix.lower() == ".json" else yaml.safe_load(raw_text)
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = ["attach_impact_summary", "embedded_impact_blockers", "impact_options_from_manifest"]
