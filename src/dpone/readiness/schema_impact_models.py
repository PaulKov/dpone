"""Source-agnostic schema impact models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal

SCHEMA_IMPACT_PLAN_SCHEMA = "dpone.schema_impact_plan.v1"
SCHEMA_IMPACT_GATE_SCHEMA = "dpone.schema_impact_gate.v1"
DEFAULT_APPROVAL_RISKS = ("compatibility_breaking", "data_destructive", "direct_rename", "shadow_cutover")
SEVERITY_ORDER = {"none": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}


@dataclass(frozen=True, slots=True)
class SchemaImpactOptions:
    enabled: bool = False
    mode: Literal["observe", "gate"] = "gate"
    unknown_dependency: Literal["warn", "block"] = "warn"
    empty_graph: Literal["allow", "warn", "block"] = "warn"
    required_for: tuple[str, ...] = DEFAULT_APPROVAL_RISKS
    sources: Mapping[str, Any] | None = None
    owners: Mapping[str, Any] | None = None

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> SchemaImpactOptions:
        if not raw:
            return cls()
        if not isinstance(raw, Mapping):
            raise ValueError("sink.options.schema_impact must be an object")
        approval = raw.get("approval", {})
        approval_map = approval if isinstance(approval, Mapping) else {}
        required = approval_map.get("required_for", DEFAULT_APPROVAL_RISKS)
        sources = raw.get("sources", {})
        owners = raw.get("owners", {})
        return cls(
            enabled=bool(raw.get("enabled", False)),
            mode=_literal(raw.get("mode", "gate"), {"observe", "gate"}, "mode"),  # type: ignore[arg-type]
            unknown_dependency=_literal(raw.get("unknown_dependency", "warn"), {"warn", "block"}, "unknown_dependency"),  # type: ignore[arg-type]
            empty_graph=_literal(raw.get("empty_graph", "warn"), {"allow", "warn", "block"}, "empty_graph"),  # type: ignore[arg-type]
            required_for=tuple(str(item) for item in required) if isinstance(required, list | tuple) else (),
            sources=dict(sources) if isinstance(sources, Mapping) else {},
            owners=dict(owners) if isinstance(owners, Mapping) else {},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "unknown_dependency": self.unknown_dependency,
            "empty_graph": self.empty_graph,
            "approval": {"required_for": list(self.required_for)},
            "sources": dict(self.sources or {}),
            "owners": dict(self.owners or {}),
        }


@dataclass(frozen=True, slots=True)
class DatasetRef:
    namespace: str
    schema: str
    table: str

    @classmethod
    def from_string(cls, value: str, *, default_namespace: str = "unknown") -> DatasetRef:
        parts = [part for part in str(value).split(".") if part]
        if len(parts) >= 3:
            return cls(parts[-3].lower(), parts[-2].lower(), parts[-1].lower())
        if len(parts) == 2:
            return cls(default_namespace.lower(), parts[0].lower(), parts[1].lower())
        return cls(default_namespace.lower(), "", str(value).lower())

    def key(self) -> str:
        return ".".join(part for part in (self.namespace, self.schema, self.table) if part)

    def to_dict(self) -> dict[str, str]:
        return asdict(self) | {"dataset": self.key()}


@dataclass(frozen=True, slots=True)
class SchemaChangeSubject:
    path: str
    change_type: str
    dataset: DatasetRef
    column: str | None
    severity: str
    risk_tags: tuple[str, ...]
    recommendation: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["dataset"] = self.dataset.to_dict()
        return {key: value for key, value in payload.items() if value is not None}


@dataclass(frozen=True, slots=True)
class DependencyNode:
    node_id: str
    node_type: str
    owner: str | None = None
    dataset: DatasetRef | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {"id": self.node_id, **asdict(self)}
        payload["dataset"] = self.dataset.to_dict() if self.dataset else None
        payload.pop("node_id", None)
        return {key: value for key, value in payload.items() if value is not None}


@dataclass(frozen=True, slots=True)
class DependencyEdge:
    source_id: str
    target_id: str
    relationship: str = "reads"
    columns: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SchemaImpactGraph:
    nodes: tuple[DependencyNode, ...] = ()
    edges: tuple[DependencyEdge, ...] = ()
    warnings: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [edge.to_dict() for edge in self.edges],
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
        }

    @property
    def by_id(self) -> dict[str, DependencyNode]:
        return {node.node_id: node for node in self.nodes}


def max_severity(values: tuple[str, ...]) -> str:
    return max(values or ("none",), key=lambda item: SEVERITY_ORDER.get(item, 0))


def merge_graphs(graphs: tuple[SchemaImpactGraph, ...]) -> SchemaImpactGraph:
    nodes: dict[str, DependencyNode] = {}
    edges: dict[tuple[str, str, str, tuple[str, ...]], DependencyEdge] = {}
    warnings: list[str] = []
    blockers: list[str] = []
    for graph in graphs:
        nodes.update({node.node_id: node for node in graph.nodes})
        edges.update({(edge.source_id, edge.target_id, edge.relationship, edge.columns): edge for edge in graph.edges})
        warnings.extend(graph.warnings)
        blockers.extend(graph.blockers)
    return SchemaImpactGraph(
        tuple(sorted(nodes.values(), key=lambda item: item.node_id)),
        tuple(edges.values()),
        tuple(warnings),
        tuple(blockers),
    )


def _literal(value: Any, allowed: set[str], field_name: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in allowed:
        raise ValueError(f"schema_impact.{field_name} must be one of: {', '.join(sorted(allowed))}")
    return normalized


__all__ = [
    "DEFAULT_APPROVAL_RISKS",
    "SCHEMA_IMPACT_GATE_SCHEMA",
    "SCHEMA_IMPACT_PLAN_SCHEMA",
    "DatasetRef",
    "DependencyEdge",
    "DependencyNode",
    "SchemaChangeSubject",
    "SchemaImpactGraph",
    "SchemaImpactOptions",
    "max_severity",
    "merge_graphs",
]
