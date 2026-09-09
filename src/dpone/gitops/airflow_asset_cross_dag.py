"""Cross-DAG asset schedule recommendations from lineage edges."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.airflow_asset_graph import AssetGraphEdge, AssetGraphReport


from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from dpone.dag.graph_algorithms import GraphAlgorithms
from dpone.gitops.airflow_asset_dag_index import node_dag_index
from dpone.gitops.airflow_asset_graph import INFERRED_OUTLET_PROVENANCE
from dpone.gitops.airflow_asset_partition import AssetPartitionSpec
from dpone.gitops.airflow_asset_partition_plan import attach_asset_partition_plans
from dpone.gitops.airflow_dag_spec import DagAssetRef, DagScheduleAssets, GitOpsAirflowDagSpec
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, GitOpsWorkloadCatalogReport, issue

_CROSS_DAG_SOURCE = "dpone gitops airflow asset-graph"
_OUTLET_PROVENANCE_DECLARED = "declared"


@dataclass(frozen=True, slots=True)
class CrossDagRecommendation:
    uri: str
    producer_node_id: str
    consumer_node_id: str
    producer_dag_id: str
    consumer_dag_id: str
    missing_outlet: bool
    missing_schedule_asset: bool
    producer_outlet_provenance: str = _OUTLET_PROVENANCE_DECLARED
    partition: AssetPartitionSpec | None = None
    partition_provenance: str | None = None

    def to_jsonable(self) -> dict[str, object]:
        return {
            "uri": self.uri,
            "producer_node_id": self.producer_node_id,
            "consumer_node_id": self.consumer_node_id,
            "producer_dag_id": self.producer_dag_id,
            "consumer_dag_id": self.consumer_dag_id,
            "missing_outlet": self.missing_outlet,
            "missing_schedule_asset": self.missing_schedule_asset,
            "producer_outlet_provenance": self.producer_outlet_provenance,
            "partition": self.partition.to_jsonable() if self.partition is not None else None,
            "partition_provenance": self.partition_provenance,
        }


def cross_dag_build_warnings(
    *,
    asset_graph: AssetGraphReport,
    specs: tuple[GitOpsAirflowDagSpec, ...],
    catalog: GitOpsWorkloadCatalogReport,
) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
    recommendations = cross_dag_recommendations(asset_graph=asset_graph, specs=specs, catalog=catalog)
    return cross_dag_warnings(recommendations)


def apply_cross_dag_schedule_assets(
    specs: tuple[GitOpsAirflowDagSpec, ...],
    recommendations: tuple[CrossDagRecommendation, ...],
) -> tuple[GitOpsAirflowDagSpec, ...]:
    """Enrich consumer dag-spec schedules for cross-DAG lineage edges at build time."""

    assets_by_dag: dict[str, dict[str, AssetPartitionSpec | None]] = defaultdict(dict)
    for item in recommendations:
        if item.missing_schedule_asset:
            assets_by_dag[item.consumer_dag_id][item.uri] = item.partition
    if not assets_by_dag:
        return specs

    enriched: list[GitOpsAirflowDagSpec] = []
    for spec in specs:
        assets = assets_by_dag.get(spec.dag_id)
        if not assets:
            enriched.append(spec)
            continue
        schedule = spec.declaration.schedule
        if isinstance(schedule, str):
            enriched.append(spec)
            continue
        existing_assets = schedule.assets if isinstance(schedule, DagScheduleAssets) else ()
        merged_by_uri = {asset.uri: asset for asset in existing_assets}
        changed_assets: list[DagAssetRef] = []
        for uri, partition in sorted(assets.items()):
            current = merged_by_uri.get(uri)
            if current is not None and current.partition == partition:
                continue
            if current is not None and current.partition is not None and partition is not None:
                continue
            replacement = DagAssetRef(
                uri=uri,
                external=current.external if current is not None else False,
                partition=partition,
            )
            merged_by_uri[uri] = replacement
            changed_assets.append(replacement)
        if not changed_assets:
            enriched.append(spec)
            continue
        merged_schedule = DagScheduleAssets(assets=tuple(merged_by_uri[uri] for uri in sorted(merged_by_uri)))
        info_warnings = tuple(
            issue(
                code="asset_graph_cross_dag_schedule_inferred",
                message=(
                    f"Build plane added schedule.assets entry {asset.uri!r} for cross-DAG lineage consumer "
                    f"{spec.dag_id}"
                ),
                path=spec.dag_id,
                source=_CROSS_DAG_SOURCE,
            )
            for asset in changed_assets
        )
        enriched.append(
            replace(
                spec,
                declaration=replace(spec.declaration, schedule=merged_schedule),
                warnings=(*spec.warnings, *info_warnings),
            )
        )
    return tuple(enriched)


def enrich_cross_dag_specs(
    *,
    asset_graph: AssetGraphReport,
    specs: tuple[GitOpsAirflowDagSpec, ...],
    catalog: GitOpsWorkloadCatalogReport,
) -> tuple[GitOpsAirflowDagSpec, ...]:
    recommendations = cross_dag_recommendations(asset_graph=asset_graph, specs=specs, catalog=catalog)
    scheduled = apply_cross_dag_schedule_assets(specs, recommendations)
    return attach_asset_partition_plans(asset_graph=asset_graph, specs=scheduled)


def cross_dag_cycle_blockers(
    *,
    asset_graph: AssetGraphReport,
    specs: tuple[GitOpsAirflowDagSpec, ...],
) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
    """Return blockers for every DAG participating in a cross-DAG inferred cycle."""

    node_dags = node_dag_index(specs)
    dag_edges = _cross_dag_edges(asset_graph=asset_graph, node_dags=node_dags)
    if not dag_edges:
        return ()

    dag_node_ids = sorted({dag for edge in dag_edges for dag in edge})
    dag_nodes: dict[str, _CrossDagNode] = {_dag_id: _CrossDagNode(name=_dag_id) for _dag_id in dag_node_ids}
    for producer, consumer in _sorted_edges(dag_edges):
        dag_nodes[producer].dependents.append(dag_nodes[consumer])

    cycles = GraphAlgorithms.detect_cycles(dag_nodes.values())  # type: ignore[arg-type]
    if not cycles:
        return ()

    blockers: list[GitOpsWorkloadCatalogIssue] = []
    blocked_dags: set[str] = set()
    rendered_cycles: list[str] = []
    for cycle in sorted((tuple(cycle) for cycle in cycles), key=_sort_cycle_key):
        normalized = _normalize_cycle(cycle)
        if not normalized:
            continue
        blocked_dags.update(normalized)
        rendered_cycles.append(_render_cycle(normalized))

    if not rendered_cycles:
        return ()
    blocker_msg = "Cross-DAG inferred dependency cycle detected: " + "; ".join(rendered_cycles)
    for dag_id in sorted(blocked_dags):
        blockers.append(
            issue(
                code="dag_spec_cross_dag_cycle",
                message=blocker_msg,
                path=dag_id,
                source=_CROSS_DAG_SOURCE,
            )
        )
    return tuple(blockers)


def cross_dag_recommendations(
    *,
    asset_graph: AssetGraphReport,
    specs: tuple[GitOpsAirflowDagSpec, ...],
    catalog: GitOpsWorkloadCatalogReport,
) -> tuple[CrossDagRecommendation, ...]:
    node_dags = node_dag_index(specs)
    spec_by_id = {spec.dag_id: spec for spec in specs}
    outlet_index = _declared_outlets_by_node(catalog, specs)
    recommendations: list[CrossDagRecommendation] = []
    for edge in asset_graph.edges:
        if _shares_dag(edge, node_dags):
            continue
        producer_dags = node_dags.get(edge.producer_node_id, ())
        consumer_dags = node_dags.get(edge.consumer_node_id, ())
        for producer_dag in producer_dags:
            for consumer_dag in consumer_dags:
                if producer_dag == consumer_dag:
                    continue
                consumer_spec = spec_by_id.get(consumer_dag)
                missing_schedule = not _schedule_includes_asset(consumer_spec, edge.uri, edge.partition)
                producer_outlet_provenance, missing_outlet = _producer_outlet_provenance(
                    edge_uri=edge.uri,
                    edge_reason=edge.reason,
                    producer_node_id=edge.producer_node_id,
                    outlet_index=outlet_index,
                )
                if not missing_schedule and not missing_outlet:
                    continue
                recommendations.append(
                    CrossDagRecommendation(
                        uri=edge.uri,
                        producer_node_id=edge.producer_node_id,
                        consumer_node_id=edge.consumer_node_id,
                        producer_dag_id=producer_dag,
                        consumer_dag_id=consumer_dag,
                        missing_outlet=missing_outlet,
                        missing_schedule_asset=missing_schedule,
                        producer_outlet_provenance=producer_outlet_provenance,
                        partition=edge.partition,
                        partition_provenance=edge.partition_provenance,
                    )
                )
    return tuple(recommendations)


def cross_dag_warnings(
    recommendations: tuple[CrossDagRecommendation, ...],
) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
    warnings: list[GitOpsWorkloadCatalogIssue] = []
    for item in recommendations:
        hints: list[str] = []
        if item.missing_outlet:
            hints.append(
                f"add {item.uri!r} to airflow.execution.outlets for workload {item.producer_node_id} "
                f"(inferred provenance={item.producer_outlet_provenance!r})"
            )
        if item.missing_schedule_asset:
            hints.append(
                f"add schedule.assets entry {item.uri!r} to dags.{item.consumer_dag_id} "
                "or mark external when upstream is outside GitOps"
            )
        warnings.append(
            issue(
                code="asset_graph_cross_dag_wiring",
                message=(
                    f"Cross-DAG lineage edge {item.producer_node_id} -> {item.consumer_node_id} "
                    f"via {item.uri!r} requires: {'; '.join(hints)}"
                ),
                path=item.consumer_dag_id,
                source=_CROSS_DAG_SOURCE,
            )
        )
    return tuple(warnings)


def _cross_dag_edges(
    *,
    asset_graph: AssetGraphReport,
    node_dags: Mapping[str, tuple[str, ...]],
) -> tuple[tuple[str, str], ...]:
    edges: list[tuple[str, str]] = []
    for edge in asset_graph.edges:
        if _shares_dag(edge, node_dags):
            continue
        for producer_dag in node_dags.get(edge.producer_node_id, ()):
            for consumer_dag in node_dags.get(edge.consumer_node_id, ()):
                if producer_dag == consumer_dag:
                    continue
                edges.append((producer_dag, consumer_dag))
    return _sorted_edges(edges)


def _declared_outlets_by_node(
    catalog: GitOpsWorkloadCatalogReport,
    specs: tuple[GitOpsAirflowDagSpec, ...],
) -> dict[str, dict[str, str]]:
    workload_outlets: dict[str, dict[str, str]] = {}
    for workload in catalog.workloads:
        execution = _execution_block(workload.effective_config)
        workload_outlets.setdefault(workload.workload_id, {})
        for uri, provenance in _declared_outlets_with_provenance(execution):
            workload_outlets[workload.workload_id].setdefault(uri, provenance)
    node_outlets: dict[str, dict[str, str]] = {}
    for spec in specs:
        for node in spec.nodes:
            node_outlets[node.node_id] = dict(workload_outlets.get(node.workload_id, {}))
    return node_outlets


def _declared_outlets_with_provenance(execution: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
    raw = execution.get("outlets")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    declared: list[tuple[str, str]] = []
    for item in raw:
        uri = _outlet_uri(item)
        if not uri:
            continue
        declared.append((uri, _outlet_provenance(item)))
    return tuple(declared)


def _outlet_uri(raw_item: Any) -> str | None:
    if isinstance(raw_item, str):
        text = raw_item.strip()
        return text or None
    if isinstance(raw_item, Mapping):
        text = str(raw_item.get("uri") or "").strip()
        return text or None
    return None


def _outlet_provenance(raw_item: object) -> str:
    if not isinstance(raw_item, Mapping):
        return _OUTLET_PROVENANCE_DECLARED
    value = raw_item.get("provenance")
    if isinstance(value, str):
        text = value.strip()
        return text or _OUTLET_PROVENANCE_DECLARED
    return _OUTLET_PROVENANCE_DECLARED


def _producer_outlet_provenance(
    *,
    edge_uri: str,
    edge_reason: str,
    producer_node_id: str,
    outlet_index: Mapping[str, Mapping[str, str]],
) -> tuple[str, bool]:
    provenance = outlet_index.get(producer_node_id, {}).get(edge_uri)
    if provenance is not None:
        return provenance, False
    # An inferred edge can only originate from the producer's inferred sink
    # URI; compact-pack compilation materializes that same outlet. Other edge
    # sources must declare an outlet until their own materializer is defined.
    if edge_reason == "inferred":
        return INFERRED_OUTLET_PROVENANCE, False
    return edge_reason or _OUTLET_PROVENANCE_DECLARED, True


def _execution_block(effective_config: Mapping[str, object]) -> Mapping[str, object]:
    airflow = effective_config.get("airflow")
    if not isinstance(airflow, Mapping):
        return {}
    execution = airflow.get("execution")
    return execution if isinstance(execution, Mapping) else {}


def _schedule_includes_asset(
    spec: GitOpsAirflowDagSpec | None,
    uri: str,
    partition: AssetPartitionSpec | None,
) -> bool:
    if spec is None:
        return False
    schedule = spec.declaration.schedule
    if not isinstance(schedule, DagScheduleAssets):
        return False
    for asset in schedule.assets:
        if asset.uri != uri:
            continue
        if partition is None:
            return True
        return asset.partition is not None and asset.partition.identity == partition.identity
    return False


def _shares_dag(edge: AssetGraphEdge, node_dags: Mapping[str, tuple[str, ...]]) -> bool:
    producer_dags = set(node_dags.get(edge.producer_node_id, ()))
    consumer_dags = set(node_dags.get(edge.consumer_node_id, ()))
    return bool(producer_dags.intersection(consumer_dags))


def _normalize_cycle(cycle: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    if cycle and cycle[0] == cycle[-1]:
        return tuple(cycle[:-1])
    return tuple(cycle)


def _render_cycle(cycle: tuple[str, ...]) -> str:
    if not cycle:
        return "<empty>"
    return " -> ".join(cycle) + f" -> {cycle[0]}"


def _sort_cycle_key(cycle: tuple[str, ...]) -> tuple[str, ...]:
    return cycle


def _sorted_edges(edges: Sequence[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(set(edges)))


@dataclass(frozen=True, slots=True)
class _CrossDagNode:
    name: str
    dependents: list[_CrossDagNode] = field(default_factory=list)


__all__ = [
    "CrossDagRecommendation",
    "apply_cross_dag_schedule_assets",
    "cross_dag_build_warnings",
    "cross_dag_cycle_blockers",
    "cross_dag_recommendations",
    "cross_dag_warnings",
    "enrich_cross_dag_specs",
]
