"""Build-time asset graph from workload lineage and declared inlets/outlets."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, GitOpsWorkloadDefinition
    from dpone.manifest.loader import ProcessSpec
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.gitops.airflow_asset_graph_edges import infer_edges
from dpone.gitops.airflow_asset_graph_lineage import (
    canonicalize_declared_uri,
    declared_uris,
    lineage_uri,
    requires_canonical_assets,
)
from dpone.gitops.airflow_asset_partition import (
    AssetPartitionContractError,
    AssetPartitionSpec,
    partition_from_asset_item,
)
from dpone.gitops.airflow_asset_uri import (
    MSSQL_ASSET_URI_INVALID,
    AssetUriIssue,
    AssetUriResolution,
    MssqlAssetAuthority,
    ResolvedMssqlAssetRegistry,
    resolve_mssql_asset_registry,
)
from dpone.gitops.airflow_process_identity import resolve_airflow_process_identities
from dpone.gitops.workload_catalog_models import issue
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.loader import ManifestLoaderRouter

_GRAPH_SOURCE = "dpone gitops airflow asset-graph"
INFERRED_OUTLET_PROVENANCE = "inferred:asset_graph.v1"


@dataclass(frozen=True, slots=True)
class AssetGraphNode:
    workload_id: str
    node_id: str
    domain: str | None
    sink_uris: frozenset[str]
    consumer_uris: frozenset[str]
    sink_partitions: tuple[AssetPartitionBinding, ...] = ()
    consumer_partitions: tuple[AssetPartitionBinding, ...] = ()


@dataclass(frozen=True, slots=True)
class AssetPartitionBinding:
    uri: str
    partition: AssetPartitionSpec


@dataclass(frozen=True, slots=True)
class AssetGraphEdge:
    uri: str
    producer_node_id: str
    consumer_node_id: str
    reason: str = "inferred"
    partition: AssetPartitionSpec | None = None
    partition_provenance: str | None = None


@dataclass(frozen=True, slots=True)
class AssetGraphReport:
    nodes: tuple[AssetGraphNode, ...]
    edges: tuple[AssetGraphEdge, ...]
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()

    @classmethod
    def empty(cls) -> AssetGraphReport:
        return cls(nodes=(), edges=(), warnings=(), blockers=())


def build_asset_graph(
    workloads: tuple[GitOpsWorkloadDefinition, ...],
    *,
    repo_root: Path,
    manifest_loader: ManifestLoaderRouter | None = None,
    env: str = "dev",
    mssql_registry: ResolvedMssqlAssetRegistry | None = None,
) -> AssetGraphReport:
    loader = manifest_loader or ManifestLoaderRouter()
    registry = mssql_registry or resolve_mssql_asset_registry(repo_root, env=env)
    resolved_authorities = dict(registry.authorities)
    database_index = dict(registry.databases)
    nodes: list[AssetGraphNode] = []
    warnings: list[GitOpsWorkloadCatalogIssue] = []
    blockers: list[GitOpsWorkloadCatalogIssue] = [
        issue(code=item.code, message=item.message, path=item.path, source=_GRAPH_SOURCE) for item in registry.issues
    ]
    if blockers:
        return AssetGraphReport(nodes=(), edges=(), warnings=(), blockers=tuple(blockers))
    for workload in workloads:
        try:
            manifest = loader.load((repo_root / workload.manifest).resolve(strict=False), metadata_only=True)
        except (ManifestConfigurationError, OSError, UnicodeError) as exc:
            warnings.append(
                issue(
                    code="asset_graph_manifest_invalid",
                    message=f"Manifest {workload.manifest} failed to load for asset graph: {exc}",
                    path=workload.workload_id,
                    source=_GRAPH_SOURCE,
                )
            )
            continue
        execution = _execution_block(workload.effective_config)
        try:
            process_assets = tuple(
                _process_assets(
                    process,
                    execution,
                    authority_index=resolved_authorities,
                    database_index=database_index,
                    mssql_registry=registry,
                    workload_id=workload.workload_id,
                    blockers=blockers,
                )
                for process in manifest.processes
            )
        except AssetPartitionContractError as exc:
            blockers.append(
                issue(
                    code=exc.code,
                    message=str(exc),
                    path=workload.workload_id,
                    source=_GRAPH_SOURCE,
                )
            )
            continue
        identities = resolve_airflow_process_identities(manifest, workload_id=workload.workload_id)
        if len(identities) > 1:
            for identity, assets in zip(identities, process_assets, strict=True):
                sink_uris, consumer_uris, sink_partitions, consumer_partitions = assets
                nodes.append(
                    AssetGraphNode(
                        workload_id=workload.workload_id,
                        node_id=identity.node_id,
                        domain=workload.domain,
                        sink_uris=sink_uris,
                        consumer_uris=consumer_uris,
                        sink_partitions=sink_partitions,
                        consumer_partitions=consumer_partitions,
                    )
                )
        elif identities:
            sink_uris, consumer_uris, sink_partitions, consumer_partitions = process_assets[0]
            nodes.append(
                AssetGraphNode(
                    workload_id=workload.workload_id,
                    node_id=identities[0].node_id,
                    domain=workload.domain,
                    sink_uris=sink_uris,
                    consumer_uris=consumer_uris,
                    sink_partitions=sink_partitions,
                    consumer_partitions=consumer_partitions,
                )
            )
    edges, edge_warnings, edge_blockers = infer_edges(tuple(nodes))
    warnings.extend(edge_warnings)
    return AssetGraphReport(
        nodes=tuple(nodes),
        edges=tuple(edges),
        warnings=tuple(warnings),
        blockers=tuple((*blockers, *edge_blockers)),
    )


def edges_for_membership(
    report: AssetGraphReport,
    *,
    node_ids: frozenset[str],
) -> tuple[AssetGraphEdge, ...]:
    return tuple(
        edge for edge in report.edges if edge.producer_node_id in node_ids and edge.consumer_node_id in node_ids
    )


def _process_assets(
    process: ProcessSpec,
    execution: Mapping[str, Any],
    *,
    authority_index: Mapping[str, MssqlAssetAuthority],
    database_index: Mapping[str, str],
    mssql_registry: ResolvedMssqlAssetRegistry,
    workload_id: str,
    blockers: list[GitOpsWorkloadCatalogIssue],
) -> tuple[
    frozenset[str],
    frozenset[str],
    tuple[AssetPartitionBinding, ...],
    tuple[AssetPartitionBinding, ...],
]:
    raw = process.raw_config if isinstance(process.raw_config, Mapping) else {}
    source_block = _mapping(raw.get("source"))
    sink_block = _mapping(raw.get("sink"))
    needs_assets = requires_canonical_assets(execution)
    sink_uris: set[str] = set()
    consumer_uris: set[str] = set()
    for uri in declared_uris(execution.get("outlets")):
        _absorb_resolution(
            canonicalize_declared_uri(uri, path=workload_id, mssql_registry=mssql_registry),
            uris=sink_uris,
            blockers=blockers,
            workload_id=workload_id,
            requires_assets=needs_assets,
        )
    for uri in declared_uris(execution.get("inlets")):
        _absorb_resolution(
            canonicalize_declared_uri(uri, path=workload_id, mssql_registry=mssql_registry),
            uris=consumer_uris,
            blockers=blockers,
            workload_id=workload_id,
            requires_assets=needs_assets,
        )
    sink_partitions = _canonicalize_partitions(
        _declared_partitions(execution.get("outlets")),
        blockers=blockers,
        workload_id=workload_id,
        requires_assets=needs_assets,
        mssql_registry=mssql_registry,
    )
    consumer_partitions = _canonicalize_partitions(
        _declared_partitions(execution.get("inlets")),
        blockers=blockers,
        workload_id=workload_id,
        requires_assets=needs_assets,
        mssql_registry=mssql_registry,
    )
    source_resolution = lineage_uri(
        source_block,
        process,
        role="source",
        authority_index=authority_index,
        database_index=database_index,
        path=workload_id,
    )
    sink_resolution = lineage_uri(
        sink_block,
        process,
        role="sink",
        authority_index=authority_index,
        database_index=database_index,
        path=workload_id,
    )
    if source_resolution is not None:
        _absorb_resolution(
            source_resolution,
            uris=consumer_uris,
            blockers=blockers,
            workload_id=workload_id,
            requires_assets=needs_assets,
        )
    if sink_resolution is not None:
        _absorb_resolution(
            sink_resolution,
            uris=sink_uris,
            blockers=blockers,
            workload_id=workload_id,
            requires_assets=needs_assets,
        )
    return frozenset(sink_uris), frozenset(consumer_uris), sink_partitions, consumer_partitions


def _canonicalize_partitions(
    bindings: tuple[AssetPartitionBinding, ...],
    *,
    blockers: list[GitOpsWorkloadCatalogIssue],
    workload_id: str,
    requires_assets: bool,
    mssql_registry: ResolvedMssqlAssetRegistry,
) -> tuple[AssetPartitionBinding, ...]:
    normalized: list[AssetPartitionBinding] = []
    for binding in bindings:
        resolved = canonicalize_declared_uri(
            binding.uri,
            path=workload_id,
            mssql_registry=mssql_registry,
        )
        _absorb_resolution(
            resolved,
            uris=set(),
            blockers=blockers,
            workload_id=workload_id,
            requires_assets=requires_assets,
        )
        if resolved.uri is not None and not resolved.issues:
            normalized.append(AssetPartitionBinding(uri=resolved.uri, partition=binding.partition))
    return tuple(sorted(normalized, key=lambda item: item.uri))


def _absorb_resolution(
    resolution: AssetUriResolution,
    *,
    uris: set[str],
    blockers: list[GitOpsWorkloadCatalogIssue],
    workload_id: str,
    requires_assets: bool,
) -> None:
    for item in resolution.issues:
        blockers.append(_blocker_from_issue(item, workload_id=workload_id))
    if resolution.uri is not None and not resolution.issues:
        uris.add(resolution.uri)
        return
    if requires_assets and resolution.uri is None and not resolution.issues:
        blockers.append(
            issue(
                code=MSSQL_ASSET_URI_INVALID,
                message="Asset scheduling/outlets require a canonical URI",
                path=workload_id,
                source=_GRAPH_SOURCE,
            )
        )


def _blocker_from_issue(item: AssetUriIssue, *, workload_id: str) -> GitOpsWorkloadCatalogIssue:
    return issue(
        code=item.code,
        message=item.message,
        path=item.path or workload_id,
        source=_GRAPH_SOURCE,
    )


def _execution_block(effective_config: Mapping[str, Any]) -> Mapping[str, Any]:
    airflow = _mapping(effective_config.get("airflow"))
    return _mapping(airflow.get("execution"))


def _declared_partitions(value: object) -> tuple[AssetPartitionBinding, ...]:
    if not isinstance(value, list):
        return ()
    bindings: list[AssetPartitionBinding] = []
    for item in value:
        partition = partition_from_asset_item(item)
        if partition is None:
            continue
        uri = str(item.get("uri") or "").strip() if isinstance(item, Mapping) else ""
        if not uri:
            raise AssetPartitionContractError(
                "asset_partition_invalid",
                "partitioned asset entries require a non-empty uri",
            )
        bindings.append(AssetPartitionBinding(uri=uri, partition=partition))
    return tuple(sorted(bindings, key=lambda binding: binding.uri))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "AssetGraphEdge",
    "AssetGraphNode",
    "AssetGraphReport",
    "AssetPartitionBinding",
    "INFERRED_OUTLET_PROVENANCE",
    "build_asset_graph",
    "edges_for_membership",
]
