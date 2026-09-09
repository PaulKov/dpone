"""Schema impact extraction and classification services."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.readiness.migration_control import MigrationPack
from dpone.readiness.schema_impact_models import (
    DEFAULT_APPROVAL_RISKS,
    DatasetRef,
    DependencyNode,
    SchemaChangeSubject,
    SchemaImpactGraph,
    max_severity,
)

_RISK_ORDER = DEFAULT_APPROVAL_RISKS


class MigrationPackChangeExtractor:
    """Convert migration-pack changes into target-agnostic impact subjects."""

    def extract(self, pack: MigrationPack) -> tuple[SchemaChangeSubject, ...]:
        dataset = DatasetRef.from_string(pack.target.table, default_namespace=pack.target.sink_type)
        return tuple(self._subject(change, dataset) for change in pack.changes) or (
            SchemaChangeSubject("target_table", "create_or_verify", dataset, None, "low", ()),
        )

    def _subject(self, change: Mapping[str, Any], dataset: DatasetRef) -> SchemaChangeSubject:
        change_type = str(change.get("change_type", "change"))
        path = str(change.get("path", "target_table"))
        risk = str(change.get("risk", ""))
        tags, severity = _risk_tags(change_type, risk)
        return SchemaChangeSubject(
            path=path,
            change_type=change_type,
            dataset=dataset,
            column=_column_from_path(path),
            severity=severity,
            risk_tags=tags,
            recommendation=str(change.get("recommendation")) if change.get("recommendation") else None,
        )


@dataclass(frozen=True, slots=True)
class SchemaImpactAnalyzer:
    required_for: tuple[str, ...] = _RISK_ORDER

    def analyze(
        self,
        *,
        pack: MigrationPack,
        subjects: tuple[SchemaChangeSubject, ...],
        graph: SchemaImpactGraph,
    ) -> dict[str, Any]:
        consumers = _impacted_consumers(subjects, graph)
        required = _required_approvals(subjects, self.required_for)
        severities = tuple(subject.severity for subject in subjects)
        warnings = list(graph.warnings)
        blockers = list(graph.blockers)
        return {
            "pack_id": pack.pack_id,
            "target": pack.target.to_dict(),
            "subjects": [subject.to_dict() for subject in subjects],
            "dependency_graph": graph.to_dict(),
            "impacted_consumers": [consumer.to_dict() for consumer in consumers],
            "required_approvals": list(required),
            "summary": {
                "max_severity": max_severity(severities),
                "changed_subjects": len(subjects),
                "impacted_consumers": len(consumers),
                "required_approvals": len(required),
            },
            "warnings": warnings,
            "blockers": blockers,
            "recommendations": _recommendations(subjects, consumers, required),
        }


def _impacted_consumers(
    subjects: tuple[SchemaChangeSubject, ...],
    graph: SchemaImpactGraph,
) -> tuple[DependencyNode, ...]:
    nodes = graph.by_id
    impacted: dict[str, DependencyNode] = {}
    for subject in subjects:
        for edge in graph.edges:
            source = nodes.get(edge.source_id)
            target = nodes.get(edge.target_id)
            if not source or not target or not source.dataset:
                continue
            if source.dataset.key() != subject.dataset.key():
                continue
            if subject.column and edge.columns and subject.column.lower() not in edge.columns:
                continue
            if target.node_type != "dataset":
                impacted[target.node_id] = target
    return tuple(sorted(impacted.values(), key=lambda node: node.node_id))


def _required_approvals(subjects: tuple[SchemaChangeSubject, ...], required_for: tuple[str, ...]) -> tuple[str, ...]:
    seen = {tag for subject in subjects for tag in subject.risk_tags if tag in required_for}
    return tuple(tag for tag in required_for if tag in seen)


def _recommendations(
    subjects: tuple[SchemaChangeSubject, ...],
    consumers: tuple[DependencyNode, ...],
    required: tuple[str, ...],
) -> list[str]:
    recommendations: list[str] = []
    if consumers:
        recommendations.append("notify impacted owners before migration apply")
    if "direct_rename" in required:
        recommendations.append("prefer schema_identity expand_contract over direct_rename")
    if "shadow_cutover" in required:
        recommendations.append("run shadow validate before approving cutover")
    if any(subject.column and "data_destructive" in subject.risk_tags for subject in subjects):
        recommendations.append("capture rollback or backup evidence before destructive contract")
    return recommendations


def _risk_tags(change_type: str, risk: str) -> tuple[tuple[str, ...], str]:
    normalized = f"{change_type}:{risk}".lower()
    if any(token in normalized for token in ("drop", "truncate", "destructive")):
        return ("compatibility_breaking", "data_destructive"), "critical"
    if "unsafe_direct_rename" in normalized or "direct_rename" in normalized:
        return ("compatibility_breaking", "direct_rename"), "high"
    if "shadow_cutover" in normalized:
        return ("compatibility_breaking", "shadow_cutover"), "critical"
    if any(token in normalized for token in ("type_narrowing", "incompatible", "not nullable", "shadow_required")):
        return ("compatibility_breaking",), "high"
    if "widen" in normalized:
        return (), "medium"
    return (), "low"


def _column_from_path(path: str) -> str | None:
    parts = str(path).split(".")
    if len(parts) >= 2 and parts[0] == "columns":
        return parts[1]
    return None


__all__ = ["MigrationPackChangeExtractor", "SchemaImpactAnalyzer"]
