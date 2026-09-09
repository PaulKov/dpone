"""Three-layer dependency merge for dag-spec builds.

One dependency system, three layers (see the declarative Airflow design spec):

1. ``inferred`` — lineage inference from the build-time asset graph (sink URI of
   producer matches source/inlet URI of consumer).
2. ``declared`` — ``depends_on``/``task_group`` read from workload manifests
   through the existing manifest machinery (``ManifestLoaderRouter`` +
   ``DependencyParser`` semantics), including internal ``#selector`` edges of
   ``dpone.batch.v1`` manifests.
3. ``curated`` — the ``wiring`` block of the ``dags:`` entry (waves expanded
   to explicit edges, or a literal dependency mapping).

Merging dedupes identical edges keeping the highest-priority reason
(curated > declared > inferred) and validates the result with the shared
graph algorithms (topological order + cycle detection).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast

from dpone.dag.graph_algorithms import GraphAlgorithms
from dpone.gitops.airflow_asset_graph import AssetGraphReport, edges_for_membership
from dpone.gitops.airflow_dag_spec import (
    EDGE_REASON_CURATED,
    EDGE_REASON_DECLARED,
    EDGE_REASON_INFERRED,
    DagSpecDeclaration,
    DagSpecEdge,
    DagSpecNode,
)
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, issue

_REASON_PRIORITY = {EDGE_REASON_CURATED: 3, EDGE_REASON_DECLARED: 2, EDGE_REASON_INFERRED: 1}
_DEPS_SOURCE = "dpone gitops airflow dag-spec"


@dataclass(frozen=True, slots=True)
class DagMembership:
    """Resolved DAG membership shared by all edge providers.

    ``dependencies`` maps node id to the ``DependencyConfig`` records parsed
    from its manifest (the builder loads every manifest exactly once);
    ``manifest_index`` maps repo-relative manifest paths to member nodes.
    """

    declaration: DagSpecDeclaration
    nodes: tuple[DagSpecNode, ...]
    dependencies: Mapping[str, tuple[Any, ...]]
    manifest_index: Mapping[str, tuple[DagSpecNode, ...]]
    group_index: Mapping[str, tuple[str, ...]]

    def node_ids(self) -> tuple[str, ...]:
        return tuple(node.node_id for node in self.nodes)


@dataclass(frozen=True, slots=True)
class EdgeMergeResult:
    edges: tuple[DagSpecEdge, ...]
    topological_order: tuple[str, ...]
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...]
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...]


class EdgeProvider(Protocol):
    """One dependency layer contributing edges for a DAG."""

    def provide(
        self, membership: DagMembership
    ) -> tuple[tuple[DagSpecEdge, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]: ...


class InferredEdgeProvider:
    """Lineage-driven edges from the build-time asset graph (layer 1)."""

    def __init__(self, asset_graph: AssetGraphReport | None = None) -> None:
        self._asset_graph = asset_graph or AssetGraphReport.empty()

    def provide(
        self, membership: DagMembership
    ) -> tuple[tuple[DagSpecEdge, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
        if membership.declaration.wiring.mode != "assets":
            return (), ()
        member_ids = frozenset(membership.node_ids())
        graph_warnings = tuple(warning for warning in self._asset_graph.warnings if warning.path in member_ids)
        edges = tuple(
            DagSpecEdge(
                upstream=edge.producer_node_id,
                downstream=edge.consumer_node_id,
                reason=EDGE_REASON_INFERRED,
                origin=f"asset:{edge.uri}",
            )
            for edge in edges_for_membership(self._asset_graph, node_ids=member_ids)
        )
        if membership.declaration.wiring.mode == "assets" and not edges and not graph_warnings:
            warning = issue(
                code="dag_spec_inferred_edges_empty",
                message=(
                    "wiring.mode=assets found no lineage-inferred edges for this DAG; "
                    "declare depends_on, wiring.dependencies, or matching source/sink URIs"
                ),
                path=membership.declaration.dag_id,
                source=_DEPS_SOURCE,
            )
            return (), (warning,)
        return edges, graph_warnings


class DeclaredEdgeProvider:
    """Edges from manifest ``depends_on`` declarations (layer 2)."""

    def __init__(self, *, repo_root: Path) -> None:
        self._repo_root = repo_root

    def provide(
        self, membership: DagMembership
    ) -> tuple[tuple[DagSpecEdge, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
        edges: list[DagSpecEdge] = []
        warnings: list[GitOpsWorkloadCatalogIssue] = []
        selector_index = {
            (node.workload_id, node.selector): node for node in membership.nodes if node.selector is not None
        }
        for node in membership.nodes:
            for dependency in membership.dependencies.get(node.node_id, ()):
                resolved = self._resolve_dependency(
                    node=node, dependency=dependency, membership=membership, selector_index=selector_index
                )
                if isinstance(resolved, GitOpsWorkloadCatalogIssue):
                    warnings.append(resolved)
                    continue
                edges.extend(resolved)
        return tuple(edges), tuple(warnings)

    def _resolve_dependency(
        self,
        *,
        node: DagSpecNode,
        dependency: Any,
        membership: DagMembership,
        selector_index: Mapping[tuple[str, str | None], DagSpecNode],
    ) -> tuple[DagSpecEdge, ...] | GitOpsWorkloadCatalogIssue:
        if dependency.group:
            return self._resolve_group_dependency(node=node, group=str(dependency.group), membership=membership)
        raw_path = str(dependency.path or "")
        if raw_path.startswith("#"):
            selector = raw_path[1:]
            upstream = selector_index.get((node.workload_id, selector))
            if upstream is None:
                return self._outside_warning(node, f"#{selector}")
            return (_declared_edge(upstream.node_id, node.node_id, origin=f"depends_on #{selector}"),)
        file_part, _, selector = raw_path.partition("#")
        relative_path = self._repo_relative(file_part)
        origin_ref = f"{relative_path}#{selector}" if selector else relative_path
        upstream_nodes = membership.manifest_index.get(relative_path)
        if not upstream_nodes:
            return self._outside_warning(node, raw_path)
        return tuple(
            _declared_edge(upstream.node_id, node.node_id, origin=f"depends_on {origin_ref}")
            for upstream in upstream_nodes
            if upstream.node_id != node.node_id
        )

    def _resolve_group_dependency(
        self, *, node: DagSpecNode, group: str, membership: DagMembership
    ) -> tuple[DagSpecEdge, ...] | GitOpsWorkloadCatalogIssue:
        member_ids = set(membership.group_index.get(group, ()))
        upstream_nodes = [
            candidate
            for candidate in membership.nodes
            if candidate.workload_id in member_ids and candidate.node_id != node.node_id
        ]
        if not upstream_nodes:
            return self._outside_warning(node, f"group:{group}")
        return tuple(
            _declared_edge(upstream.node_id, node.node_id, origin=f"depends_on group:{group}")
            for upstream in upstream_nodes
        )

    def _outside_warning(self, node: DagSpecNode, reference: str) -> GitOpsWorkloadCatalogIssue:
        return issue(
            code="dag_spec_dependency_outside_dag",
            message=(
                f"Workload {node.workload_id} declares depends_on {reference!r} that resolves to no member "
                "of this DAG; the edge is skipped"
            ),
            path=node.node_id,
            source=_DEPS_SOURCE,
        )

    def _repo_relative(self, raw_path: str) -> str:
        path = Path(raw_path)
        if not path.is_absolute():
            path = (self._repo_root / raw_path).resolve(strict=False)
        try:
            return path.resolve(strict=False).relative_to(self._repo_root.resolve(strict=False)).as_posix()
        except ValueError:
            return path.as_posix()


class CuratedEdgeProvider:
    """Edges from the ``dags:`` wiring block (layer 3)."""

    def provide(
        self, membership: DagMembership
    ) -> tuple[tuple[DagSpecEdge, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
        wiring = membership.declaration.wiring
        if wiring.mode == "explicit":
            return self._explicit_edges(membership)
        if wiring.mode == "waves":
            return self._wave_edges(membership), ()
        return (), ()

    def _explicit_edges(
        self, membership: DagMembership
    ) -> tuple[tuple[DagSpecEdge, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
        node_ids = set(membership.node_ids())
        edges: list[DagSpecEdge] = []
        warnings: list[GitOpsWorkloadCatalogIssue] = []
        for downstream, upstreams in membership.declaration.wiring.dependencies.items():
            for upstream in upstreams:
                if upstream not in node_ids or downstream not in node_ids:
                    warnings.append(
                        issue(
                            code="dag_spec_wiring_reference_unknown",
                            message=f"wiring.dependencies edge {upstream} -> {downstream} references a non-member id",
                            path=membership.declaration.dag_id,
                            source=_DEPS_SOURCE,
                        )
                    )
                    continue
                edges.append(
                    DagSpecEdge(
                        upstream=upstream,
                        downstream=downstream,
                        reason=EDGE_REASON_CURATED,
                        origin="wiring.dependencies",
                    )
                )
        return tuple(edges), tuple(warnings)

    def _wave_edges(self, membership: DagMembership) -> tuple[DagSpecEdge, ...]:
        width = membership.declaration.wiring.max_parallel_workloads
        node_ids = membership.node_ids()
        edges: list[DagSpecEdge] = []
        for offset in range(width, len(node_ids), width):
            previous_wave = node_ids[offset - width : offset]
            current_wave = node_ids[offset : offset + width]
            wave_number = offset // width
            for upstream in previous_wave:
                for downstream in current_wave:
                    edges.append(
                        DagSpecEdge(
                            upstream=upstream,
                            downstream=downstream,
                            reason=EDGE_REASON_CURATED,
                            origin=f"wiring.waves[{wave_number - 1}->{wave_number}]",
                        )
                    )
        return tuple(edges)


def _declared_edge(upstream: str, downstream: str, *, origin: str) -> DagSpecEdge:
    return DagSpecEdge(upstream=upstream, downstream=downstream, reason=EDGE_REASON_DECLARED, origin=origin)


def default_edge_providers(*, repo_root: Path, asset_graph: AssetGraphReport | None = None) -> tuple[EdgeProvider, ...]:
    """Layer order is lowest priority first; merge resolves duplicates."""

    return (
        InferredEdgeProvider(asset_graph),
        DeclaredEdgeProvider(repo_root=repo_root),
        CuratedEdgeProvider(),
    )


def merge_dag_edges(membership: DagMembership, providers: Iterable[EdgeProvider]) -> EdgeMergeResult:
    """Merge all layers, dedupe by priority, and validate the graph."""

    warnings: list[GitOpsWorkloadCatalogIssue] = []
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    merged: dict[tuple[str, str], DagSpecEdge] = {}
    for provider in providers:
        edges, provider_warnings = provider.provide(membership)
        for provider_issue in provider_warnings:
            if provider_issue.code == "dag_spec_wiring_reference_unknown":
                blockers.append(provider_issue)
            else:
                warnings.append(provider_issue)
        for edge in edges:
            key = (edge.upstream, edge.downstream)
            current = merged.get(key)
            if current is None or _REASON_PRIORITY[edge.reason] > _REASON_PRIORITY[current.reason]:
                merged[key] = edge
    ordered_edges = tuple(sorted(merged.values(), key=lambda edge: (edge.upstream, edge.downstream)))
    topo, graph_blockers = _validated_order(membership, ordered_edges)
    return EdgeMergeResult(
        edges=ordered_edges,
        topological_order=topo,
        warnings=tuple(warnings),
        blockers=tuple((*blockers, *graph_blockers)),
    )


@dataclass
class _MergeNode:
    """Duck-typed graph node satisfying the GraphAlgorithms contract."""

    name: str
    dependents: list[_MergeNode] = field(default_factory=list)


def _validated_order(
    membership: DagMembership, edges: tuple[DagSpecEdge, ...]
) -> tuple[tuple[str, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
    nodes = {node_id: _MergeNode(name=node_id) for node_id in membership.node_ids()}
    for edge in edges:
        nodes[edge.upstream].dependents.append(nodes[edge.downstream])
    graph_nodes = cast("Mapping[str, Any]", nodes)
    cycles = GraphAlgorithms.detect_cycles(graph_nodes)
    if cycles:
        rendered = "; ".join(" -> ".join(cycle) for cycle in cycles)
        blocker = issue(
            code="dag_spec_cycle",
            message=(
                f"Dependency merge produced a cycle: {rendered}. Reconcile depends_on declarations with the "
                "curated wiring block (run `dpone dag explain-edge --dag-spec` for edge provenance)"
            ),
            path=membership.declaration.dag_id,
            source=_DEPS_SOURCE,
        )
        return (), (blocker,)
    ordered = GraphAlgorithms.topological_sort(graph_nodes)
    return tuple(node.name for node in ordered), ()


__all__ = [
    "CuratedEdgeProvider",
    "DagMembership",
    "DeclaredEdgeProvider",
    "EdgeMergeResult",
    "EdgeProvider",
    "InferredEdgeProvider",
    "default_edge_providers",
    "merge_dag_edges",
]
