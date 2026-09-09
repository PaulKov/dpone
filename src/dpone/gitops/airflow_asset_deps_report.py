"""Markdown rendering for reviewable Airflow asset dependency graphs."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.gitops.airflow_asset_cross_dag import CrossDagRecommendation
from dpone.gitops.airflow_asset_graph import AssetGraphEdge, AssetGraphNode, AssetGraphReport
from dpone.gitops.airflow_dag_spec import GitOpsAirflowDagSpec
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue


def render_asset_deps_markdown(
    *,
    workload_set: str,
    env: str,
    asset_graph: AssetGraphReport,
    specs: tuple[GitOpsAirflowDagSpec, ...],
    node_dags: Mapping[str, tuple[str, ...]],
    cross_dag: tuple[CrossDagRecommendation, ...] = (),
) -> str:
    lines = [
        "# dpone GitOps Airflow asset dependency graph",
        "",
        f"- workload-set: `{workload_set}`",
        f"- environment: `{env}`",
        f"- workloads: {len(asset_graph.nodes)}",
        f"- inferred edges: {len(asset_graph.edges)}",
        "",
    ]
    if asset_graph.warnings:
        lines.extend(_warning_section(asset_graph.warnings))
    lines.extend(_node_section(asset_graph.nodes))
    lines.extend(_edge_section("Same-DAG inferred edges", _same_dag_edges(asset_graph.edges, node_dags)))
    lines.extend(_edge_section("Cross-DAG lineage edges", _cross_dag_edges(asset_graph.edges, node_dags)))
    lines.extend(_cross_dag_recommendations_section(cross_dag))
    lines.extend(_dag_spec_section(specs))
    return "\n".join(lines).rstrip() + "\n"


def _warning_section(warnings: tuple[GitOpsWorkloadCatalogIssue, ...]) -> list[str]:
    lines = ["## Warnings", ""]
    for warning in warnings:
        lines.append(f"- `{warning.code}` @ `{warning.path}`: {warning.message}")
    lines.append("")
    return lines


def _node_section(nodes: tuple[AssetGraphNode, ...]) -> list[str]:
    lines = ["## Workload nodes", "", "| node | domain | sink URIs | consumer URIs |", "| --- | --- | --- | --- |"]
    for node in nodes:
        sinks = ", ".join(sorted(node.sink_uris)) or "—"
        consumers = ", ".join(sorted(node.consumer_uris)) or "—"
        lines.append(f"| `{node.node_id}` | {node.domain or '—'} | {sinks} | {consumers} |")
    lines.append("")
    return lines


def _edge_section(title: str, edges: tuple[AssetGraphEdge, ...]) -> list[str]:
    if not edges:
        return [f"## {title}", "", "_None._", ""]
    lines = [f"## {title}", "", "| URI | producer | consumer |", "| --- | --- | --- |"]
    for edge in edges:
        lines.append(f"| `{edge.uri}` | `{edge.producer_node_id}` | `{edge.consumer_node_id}` |")
    lines.append("")
    return lines


def _same_dag_edges(
    edges: tuple[AssetGraphEdge, ...], node_dags: Mapping[str, tuple[str, ...]]
) -> tuple[AssetGraphEdge, ...]:
    return tuple(edge for edge in edges if _shares_dag(edge, node_dags))


def _cross_dag_edges(
    edges: tuple[AssetGraphEdge, ...], node_dags: Mapping[str, tuple[str, ...]]
) -> tuple[AssetGraphEdge, ...]:
    return tuple(edge for edge in edges if not _shares_dag(edge, node_dags))


def _shares_dag(edge: AssetGraphEdge, node_dags: Mapping[str, tuple[str, ...]]) -> bool:
    producer_dags = set(node_dags.get(edge.producer_node_id, ()))
    consumer_dags = set(node_dags.get(edge.consumer_node_id, ()))
    return bool(producer_dags.intersection(consumer_dags))


def _cross_dag_recommendations_section(recommendations: tuple[CrossDagRecommendation, ...]) -> list[str]:
    if not recommendations:
        return ["## Cross-DAG wiring recommendations", "", "_None._", ""]
    lines = [
        "## Cross-DAG wiring recommendations",
        "",
        "| URI | producer DAG | consumer DAG | outlet | schedule.assets |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in recommendations:
        lines.append(
            f"| `{item.uri}` | `{item.producer_dag_id}` | `{item.consumer_dag_id}` | "
            f"{'missing' if item.missing_outlet else 'ok'} | "
            f"{'missing' if item.missing_schedule_asset else 'ok'} |"
        )
    lines.append("")
    return lines


def _dag_spec_section(specs: tuple[GitOpsAirflowDagSpec, ...]) -> list[str]:
    if not specs:
        return ["## Declared DAG specs", "", "_No dag-spec artifacts were built._", ""]
    lines = ["## Declared DAG specs", ""]
    for spec in specs:
        inferred = [edge for edge in spec.edges if edge.reason == "inferred"]
        lines.append(f"### `{spec.dag_id}`")
        lines.append(f"- nodes: {len(spec.nodes)}")
        lines.append(f"- edges: {len(spec.edges)} ({len(inferred)} inferred)")
        if inferred:
            lines.append("- inferred:")
            for edge in inferred:
                lines.append(f"  - `{edge.upstream}` → `{edge.downstream}` ({edge.origin})")
        lines.append("")
    return lines


__all__ = ["render_asset_deps_markdown"]
