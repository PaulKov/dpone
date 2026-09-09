from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.conventions import ConventionLayer
from dpone.manifest.registry import RegistryResolution


@dataclass(frozen=True, slots=True)
class ExplainChange:
    path: str
    kind: str  # add | remove | change | append
    before: Any
    after: Any


@dataclass(frozen=True, slots=True)
class ExplainLayer:
    id: str
    title: str


@dataclass(frozen=True, slots=True)
class ExplainResult:
    manifest_path: Path
    kind: str
    selector: str | None
    process_name: str
    task_group: str | None
    source: str | None
    sink: str | None
    conventions: tuple[ConventionLayer, ...]
    registry: RegistryResolution | None
    vars: dict[str, Any]
    vars_origin: dict[str, str]
    naming: dict[str, Any]
    naming_origin: dict[str, str]
    derived_fields: dict[str, str]  # cfg dot-path -> naming key
    config_layers: tuple[ExplainLayer, ...]
    config_changes: dict[str, list[ExplainChange]]  # layer_id -> changes
    config_patches: dict[str, Any]  # layer_id -> deep-merge patch (diff-friendly)
    config_patch_removes: dict[str, list[str]]  # layer_id -> removed paths (cannot be expressed by deep-merge)
    config_origin: dict[str, str]  # leaf dot-path -> origin label
    final_config: dict[str, Any]
    matches_compiler: bool
    config_snapshots: dict[str, dict[str, Any]] | None = None
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "manifest_path": str(self.manifest_path),
            "kind": self.kind,
            "selector": self.selector,
            "process_name": self.process_name,
            "task_group": self.task_group,
            "source": self.source,
            "sink": self.sink,
            "conventions": [{"name": c.name, "source": c.source} for c in self.conventions],
            "registry": (
                {
                    "registry_paths": [str(p) for p in self.registry.registry_paths],
                    "matched_key": list(self.registry.matched_key),
                    "entry_source": str(self.registry.entry_source),
                    "vars": dict(self.registry.vars),
                }
                if self.registry
                else None
            ),
            "vars": self.vars,
            "vars_origin": self.vars_origin,
            "naming": self.naming,
            "naming_origin": self.naming_origin,
            "derived_fields": self.derived_fields,
            "config_layers": [{"id": layer.id, "title": layer.title} for layer in self.config_layers],
            "config_changes": {
                lid: [
                    {
                        "path": ch.path,
                        "kind": ch.kind,
                        "before": ch.before,
                        "after": ch.after,
                    }
                    for ch in changes
                ]
                for lid, changes in self.config_changes.items()
            },
            "config_patches": self.config_patches,
            "config_patch_removes": self.config_patch_removes,
            "config_origin": self.config_origin,
            "final_config": self.final_config,
            "matches_compiler": self.matches_compiler,
            "warnings": list(self.warnings),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_json_dict(), ensure_ascii=False, indent=2)


@dataclass(frozen=True, slots=True)
class WhyEvent:
    layer_id: str
    layer_title: str
    change: ExplainChange


@dataclass(frozen=True, slots=True)
class WhyExplanation:
    query: str
    canonical_path: str
    exists: bool
    final_value: Any
    final_origin: str
    events: tuple[WhyEvent, ...]
    naming: dict[str, Any] | None = None
    list_items: tuple[dict[str, Any], ...] | None = None
    map_items: tuple[dict[str, Any], ...] | None = None
    parent: dict[str, Any] | None = None
    timeline: tuple[dict[str, Any], ...] | None = None
    suggested_patches: tuple[dict[str, Any], ...] | None = None
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "canonical_path": self.canonical_path,
            "exists": self.exists,
            "final_value": self.final_value,
            "final_origin": self.final_origin,
            "events": [
                {
                    "layer_id": e.layer_id,
                    "layer_title": e.layer_title,
                    "kind": e.change.kind,
                    "path": e.change.path,
                    "before": e.change.before,
                    "after": e.change.after,
                }
                for e in self.events
            ],
            "naming": self.naming,
            "list_items": list(self.list_items) if self.list_items else None,
            "map_items": list(self.map_items) if self.map_items else None,
            "parent": self.parent,
            "timeline": list(self.timeline) if self.timeline else None,
            "suggested_patches": list(self.suggested_patches) if self.suggested_patches else None,
            "warnings": list(self.warnings),
        }


__all__ = [
    "ExplainChange",
    "ExplainLayer",
    "ExplainResult",
    "WhyEvent",
    "WhyExplanation",
]
