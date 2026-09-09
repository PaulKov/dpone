"""Edge inference helpers for the Airflow asset graph."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from dpone.gitops.airflow_asset_partition import AssetPartitionSpec
from dpone.gitops.workload_catalog_models import issue

if TYPE_CHECKING:
    from dpone.gitops.airflow_asset_graph import AssetGraphEdge, AssetGraphNode, AssetPartitionBinding
    from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue

_GRAPH_SOURCE = "dpone gitops airflow asset-graph"


def infer_edges(
    nodes: tuple[AssetGraphNode, ...],
) -> tuple[tuple[AssetGraphEdge, ...], list[GitOpsWorkloadCatalogIssue], list[GitOpsWorkloadCatalogIssue]]:
    from dpone.gitops.airflow_asset_graph import AssetGraphEdge

    producers: dict[str, list[tuple[str, AssetPartitionSpec | None]]] = defaultdict(list)
    for node in nodes:
        for uri in node.sink_uris:
            producers[uri].append((node.node_id, partition_for(node.sink_partitions, uri)))
    edges: list[AssetGraphEdge] = []
    warnings: list[GitOpsWorkloadCatalogIssue] = []
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    for node in nodes:
        for uri in node.consumer_uris:
            candidates = [producer for producer in producers.get(uri, ()) if producer[0] != node.node_id]
            if not candidates:
                continue
            if len(candidates) > 1:
                ordered = sorted(producer for producer, _ in candidates)
                blockers.append(
                    issue(
                        code="asset_graph_uri_ambiguous",
                        message=(
                            f"Asset URI {uri!r} is produced by multiple workloads: "
                            f"{', '.join(ordered)}; declare an explicit curator decision"
                        ),
                        path=node.node_id,
                        source=_GRAPH_SOURCE,
                    )
                )
                continue
            producer_node_id, producer_partition = candidates[0]
            consumer_partition = partition_for(node.consumer_partitions, uri)
            partition, partition_provenance = resolve_edge_partition(
                uri=uri,
                producer=producer_partition,
                consumer=consumer_partition,
                consumer_node_id=node.node_id,
                blockers=blockers,
            )
            if producer_partition is not None or consumer_partition is not None:
                if partition is None:
                    continue
            edges.append(
                AssetGraphEdge(
                    uri=uri,
                    producer_node_id=producer_node_id,
                    consumer_node_id=node.node_id,
                    reason="inferred",
                    partition=partition,
                    partition_provenance=partition_provenance,
                )
            )
    return (
        tuple(sorted(edges, key=lambda edge: (edge.uri, edge.producer_node_id, edge.consumer_node_id))),
        warnings,
        blockers,
    )


def resolve_edge_partition(
    *,
    uri: str,
    producer: AssetPartitionSpec | None,
    consumer: AssetPartitionSpec | None,
    consumer_node_id: str,
    blockers: list[GitOpsWorkloadCatalogIssue],
) -> tuple[AssetPartitionSpec | None, str | None]:
    if producer is None and consumer is None:
        return None, None
    if producer is None or (consumer is not None and producer.identity != consumer.identity):
        blockers.append(
            issue(
                code="asset_partition_contract_mismatch",
                message=(
                    f"Asset URI {uri!r} has incompatible producer and consumer partition contracts; "
                    "declare one matching partition on the producer outlet"
                ),
                path=consumer_node_id,
                source=_GRAPH_SOURCE,
            )
        )
        return None, None
    if consumer is None:
        return producer, "inherited:producer_outlet"
    return producer, "declared:matched"


def partition_for(bindings: tuple[AssetPartitionBinding, ...], uri: str) -> AssetPartitionSpec | None:
    return next((binding.partition for binding in bindings if binding.uri == uri), None)


__all__ = ["infer_edges", "partition_for", "resolve_edge_partition"]
