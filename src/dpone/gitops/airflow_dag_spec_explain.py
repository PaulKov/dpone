"""Explain dag-spec dependency edges with three-layer provenance.

The GitOps dag-spec builder merges inferred, declared, and curated dependency
layers. This module exposes that merge result to ``dpone dag explain-edge
--dag-spec`` so users can see why an edge exists, which layer won a duplicate,
and (optionally) a shortest transitive path when no direct edge is present.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from dpone.dag.edge_explain import DagEdgeExplanation, EdgeReason
from dpone.gitops.airflow_dag_spec import DagSpecEdge, DagSpecNode
from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder, ResolvedDagSpecContext
from dpone.gitops.airflow_dag_spec_deps import (
    EdgeMergeResult,
    default_edge_providers,
)
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue

_LAYER_NAMES = ("inferred", "declared", "curated")
_REASON_PRIORITY = {"curated": 3, "declared": 2, "inferred": 1}


@dataclass(frozen=True, slots=True)
class DagSpecLayerContribution:
    """Whether one dependency layer contributed a direct edge."""

    layer: str
    present: bool
    reason: str | None = None
    origin: str | None = None

    def to_jsonable(self) -> dict[str, object]:
        return {
            "layer": self.layer,
            "present": self.present,
            "reason": self.reason,
            "origin": self.origin,
        }


@dataclass(frozen=True, slots=True)
class DagSpecDirectEdgeExplanation:
    """Direct-edge explanation for a dag-spec dependency merge."""

    dag_id: str
    upstream: DagSpecNode
    downstream: DagSpecNode
    direct_edge: bool
    winning_edge: DagSpecEdge | None
    layer_contributions: tuple[DagSpecLayerContribution, ...]
    overridden_edges: tuple[DagSpecEdge, ...]
    warnings: tuple[str, ...]
    merge_blockers: tuple[GitOpsWorkloadCatalogIssue, ...]
    path: tuple[str, ...] | None = None

    def to_jsonable(self) -> dict[str, object]:
        return {
            "kind": "dag.explain_edge_dag_spec",
            "dag_id": self.dag_id,
            "upstream": self.upstream.to_jsonable(),
            "downstream": self.downstream.to_jsonable(),
            "direct_edge": self.direct_edge,
            "winning_edge": self.winning_edge.to_jsonable() if self.winning_edge else None,
            "layer_contributions": [item.to_jsonable() for item in self.layer_contributions],
            "overridden_edges": [edge.to_jsonable() for edge in self.overridden_edges],
            "warnings": list(self.warnings),
            "merge_blockers": [blocker.to_jsonable() for blocker in self.merge_blockers],
            "path": list(self.path) if self.path else None,
        }


def resolve_dag_spec_context(
    *,
    repo_root: Path,
    workload_set: str,
    dag_id: str,
    env: str = "dev",
) -> tuple[ResolvedDagSpecContext | None, tuple[GitOpsWorkloadCatalogIssue, ...]]:
    """Resolve one dag-spec build context for explain/report tooling."""

    return AirflowDagSpecBuilder(repo_root=repo_root).resolve_context(
        workload_set=workload_set,
        dag_id=dag_id,
        env=env,
    )


def explain_dag_spec_direct_edge(
    *,
    context: ResolvedDagSpecContext,
    upstream_id: str,
    downstream_id: str,
    include_path: bool = False,
) -> DagSpecDirectEdgeExplanation:
    """Explain a direct or transitive dag-spec edge with layer provenance."""

    upstream = _node_by_id(context.membership.nodes, upstream_id)
    downstream = _node_by_id(context.membership.nodes, downstream_id)
    winning = _winning_edge(context.merge, upstream_id, downstream_id)
    layer_contributions, overridden = _layer_provenance(
        membership=context.membership,
        repo_root=context.repo_root,
        upstream_id=upstream_id,
        downstream_id=downstream_id,
        winning=winning,
    )
    warnings = tuple(issue.message for issue in context.merge.warnings)
    path = None
    if include_path and winning is None:
        path = _shortest_path(context.merge, upstream_id, downstream_id)
    return DagSpecDirectEdgeExplanation(
        dag_id=context.dag_id,
        upstream=upstream,
        downstream=downstream,
        direct_edge=winning is not None,
        winning_edge=winning,
        layer_contributions=layer_contributions,
        overridden_edges=overridden,
        warnings=warnings,
        merge_blockers=context.merge.blockers,
        path=path,
    )


def to_dag_edge_explanation(exp: DagSpecDirectEdgeExplanation) -> DagEdgeExplanation:
    """Adapt dag-spec explain output to the legacy edge renderer contract."""

    reasons: list[EdgeReason] = []
    if exp.winning_edge is not None:
        reasons.append(
            EdgeReason(
                kind=exp.winning_edge.reason,
                description=exp.winning_edge.origin,
                evidence={
                    "source": "dag-spec",
                    "layer": exp.winning_edge.reason,
                    "priority": "winning",
                },
            )
        )
    for contribution in exp.layer_contributions:
        if not contribution.present:
            continue
        if exp.winning_edge is not None and contribution.reason == exp.winning_edge.reason:
            continue
        reasons.append(
            EdgeReason(
                kind=f"{contribution.layer}_contribution",
                description=contribution.origin or contribution.layer,
                evidence={
                    "source": "dag-spec",
                    "layer": contribution.layer,
                    "reason": contribution.reason,
                },
            )
        )
    for edge in exp.overridden_edges:
        reasons.append(
            EdgeReason(
                kind=f"{edge.reason}_overridden",
                description=(
                    f"Lower-priority {edge.reason} edge from {edge.origin} was superseded by a higher-priority layer"
                ),
                evidence={"source": "dag-spec", "origin": edge.origin, "reason": edge.reason},
            )
        )
    for blocker in exp.merge_blockers:
        reasons.append(
            EdgeReason(
                kind="merge_blocker",
                description=blocker.message,
                evidence={"source": "dag-spec", "code": blocker.code},
            )
        )
    return DagEdgeExplanation(
        upstream=_node_summary(exp.upstream),
        downstream=_node_summary(exp.downstream),
        direct_edge=exp.direct_edge,
        reasons=reasons,
        warnings=[*exp.warnings],
        path=list(exp.path) if exp.path else None,
    )


def _node_by_id(nodes: Sequence[DagSpecNode], node_id: str) -> DagSpecNode:
    for node in nodes:
        if node.node_id == node_id:
            return node
    known = ", ".join(sorted(node.node_id for node in nodes))
    raise ValueError(f"Unknown dag-spec node {node_id!r}; known nodes: {known}")


def _node_summary(node: DagSpecNode) -> dict[str, object]:
    return {
        "name": node.node_id,
        "workload_id": node.workload_id,
        "selector": node.selector,
        "task_group": node.task_group,
    }


def _winning_edge(merge: EdgeMergeResult, upstream_id: str, downstream_id: str) -> DagSpecEdge | None:
    for edge in merge.edges:
        if edge.upstream == upstream_id and edge.downstream == downstream_id:
            return edge
    return None


def _layer_provenance(
    *,
    membership,
    repo_root: Path,
    upstream_id: str,
    downstream_id: str,
    winning: DagSpecEdge | None,
) -> tuple[tuple[DagSpecLayerContribution, ...], tuple[DagSpecEdge, ...]]:
    providers = default_edge_providers(repo_root=repo_root)
    layer_edges: dict[str, DagSpecEdge | None] = {}
    for layer_name, provider in zip(_LAYER_NAMES, providers, strict=True):
        edges, _warnings = provider.provide(membership)
        layer_edges[layer_name] = next(
            (edge for edge in edges if edge.upstream == upstream_id and edge.downstream == downstream_id),
            None,
        )
    contributions = tuple(
        DagSpecLayerContribution(
            layer=layer_name,
            present=layer_edges[layer_name] is not None,
            reason=None if layer_edges[layer_name] is None else layer_edges[layer_name].reason,
            origin=None if layer_edges[layer_name] is None else layer_edges[layer_name].origin,
        )
        for layer_name in _LAYER_NAMES
    )
    overridden: list[DagSpecEdge] = []
    if winning is not None:
        winning_priority = _REASON_PRIORITY[winning.reason]
        for layer_name in _LAYER_NAMES:
            edge = layer_edges[layer_name]
            if edge is None:
                continue
            if _REASON_PRIORITY[edge.reason] < winning_priority:
                overridden.append(edge)
    return contributions, tuple(overridden)


def _shortest_path(merge: EdgeMergeResult, upstream_id: str, downstream_id: str) -> tuple[str, ...] | None:
    adjacency: dict[str, set[str]] = {}
    for edge in merge.edges:
        adjacency.setdefault(edge.upstream, set()).add(edge.downstream)
    if upstream_id not in adjacency:
        return None
    queue: deque[tuple[str, tuple[str, ...]]] = deque([(upstream_id, (upstream_id,))])
    visited = {upstream_id}
    while queue:
        current, path = queue.popleft()
        for neighbor in sorted(adjacency.get(current, ())):
            if neighbor in visited:
                continue
            next_path = (*path, neighbor)
            if neighbor == downstream_id:
                return next_path
            visited.add(neighbor)
            queue.append((neighbor, next_path))
    return None


__all__ = [
    "DagSpecDirectEdgeExplanation",
    "DagSpecLayerContribution",
    "explain_dag_spec_direct_edge",
    "resolve_dag_spec_context",
    "to_dag_edge_explanation",
]
