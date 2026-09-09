"""Public models for end-to-end dependency explanations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class OriginInfo:
    path: str
    origin: str
    origin_title: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "origin": self.origin,
            "origin_title": self.origin_title,
        }


@dataclass(frozen=True, slots=True)
class ProducedEdge:
    upstream: str
    upstream_ref: str | None
    reason_kinds: tuple[str, ...]
    reasons: tuple[dict[str, Any], ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "upstream": self.upstream,
            "upstream_ref": self.upstream_ref,
            "reason_kinds": list(self.reason_kinds),
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class DirectDependencyExplanation:
    index: int
    compiled_item: Any
    compiled_origins: tuple[OriginInfo, ...] = ()
    parsed_dependency: dict[str, Any] | None = None
    parse_records: tuple[dict[str, Any], ...] = ()
    upstream_tasks: tuple[str, ...] = ()
    edges: tuple[ProducedEdge, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "compiled_item": self.compiled_item,
            "compiled_origins": [o.to_json_dict() for o in self.compiled_origins],
            "parsed_dependency": self.parsed_dependency,
            "parse_records": list(self.parse_records),
            "upstream_tasks": list(self.upstream_tasks),
            "edges": [e.to_json_dict() for e in self.edges],
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class InheritedGroupDependencyExplanation:
    upstream_group: str
    dependent_group: str
    upstream_tasks: tuple[str, ...] = ()
    trigger_count: int = 0
    triggers: tuple[dict[str, Any], ...] = ()
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "upstream_group": self.upstream_group,
            "dependent_group": self.dependent_group,
            "upstream_tasks": list(self.upstream_tasks),
            "trigger_count": self.trigger_count,
            "triggers": list(self.triggers),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class EndToEndDependenciesResult:
    downstream: dict[str, Any]
    direct: tuple[DirectDependencyExplanation, ...] = ()
    inherited_group_deps: tuple[InheritedGroupDependencyExplanation, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "downstream": dict(self.downstream),
            "direct": [d.to_json_dict() for d in self.direct],
            "inherited_group_deps": [g.to_json_dict() for g in self.inherited_group_deps],
            "warnings": list(self.warnings),
        }


__all__ = [
    "DirectDependencyExplanation",
    "EndToEndDependenciesResult",
    "InheritedGroupDependencyExplanation",
    "OriginInfo",
    "ProducedEdge",
]
