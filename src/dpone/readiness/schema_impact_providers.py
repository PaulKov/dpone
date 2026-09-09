"""Dependency graph providers for schema impact planning."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.readiness.schema_impact_models import (
    DatasetRef,
    DependencyEdge,
    DependencyNode,
    SchemaImpactGraph,
    merge_graphs,
)


class DependencyProvider(Protocol):
    def load(self) -> SchemaImpactGraph: ...


class ManualDependencyProvider:
    def __init__(self, config: Mapping[str, Any], *, default_owner: str | None = None) -> None:
        self._config = config
        self._default_owner = default_owner

    def load(self) -> SchemaImpactGraph:
        nodes: list[DependencyNode] = []
        edges: list[DependencyEdge] = []
        for consumer in self._config.get("consumers", []):
            if not isinstance(consumer, Mapping):
                continue
            consumer_id = str(consumer.get("id", "")).strip()
            if not consumer_id:
                continue
            nodes.append(
                DependencyNode(
                    node_id=consumer_id,
                    node_type=str(consumer.get("type", "consumer")),
                    owner=str(consumer.get("owner") or self._default_owner or ""),
                )
            )
            for read in consumer.get("reads", []):
                if not isinstance(read, Mapping):
                    continue
                dataset = DatasetRef.from_string(str(read.get("dataset", "")))
                dataset_id = f"dataset:{dataset.key()}"
                nodes.append(DependencyNode(node_id=dataset_id, node_type="dataset", dataset=dataset))
                columns = tuple(str(item).lower() for item in read.get("columns", []) if item)
                edges.append(DependencyEdge(source_id=dataset_id, target_id=consumer_id, columns=columns))
        return SchemaImpactGraph(nodes=tuple(nodes), edges=tuple(edges))


class ManifestDependencyProvider:
    def __init__(self, manifest: Mapping[str, Any], *, manifest_path: Path | None = None) -> None:
        self._manifest = manifest
        self._manifest_path = manifest_path

    def load(self) -> SchemaImpactGraph:
        sink = self._manifest.get("sink", {})
        if not isinstance(sink, Mapping):
            return SchemaImpactGraph(warnings=("schema_impact.manifest_sink_missing",))
        sink_type = str(sink.get("type", "unknown"))
        dataset = _manifest_dataset(sink, default_namespace=sink_type)
        dataset_id = f"dataset:{dataset.key()}"
        route_id = f"manifest:{self._manifest_path or dataset.key()}"
        return SchemaImpactGraph(
            nodes=(
                DependencyNode(node_id=dataset_id, node_type="dataset", dataset=dataset),
                DependencyNode(node_id=route_id, node_type="manifest_route"),
            ),
            edges=(DependencyEdge(source_id=dataset_id, target_id=route_id, relationship="owns"),),
        )


class DbtManifestDependencyProvider:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> SchemaImpactGraph:
        payload, issue = _load_json(self._path, "dbt_manifest")
        if issue:
            return issue
        nodes: dict[str, DependencyNode] = {}
        edges: list[DependencyEdge] = []
        sources = payload.get("sources", {}) if isinstance(payload.get("sources"), Mapping) else {}
        models = payload.get("nodes", {}) if isinstance(payload.get("nodes"), Mapping) else {}
        for unique_id, raw in {**sources, **models}.items():
            if not isinstance(raw, Mapping):
                continue
            dataset = _dbt_dataset(raw)
            node_type = str(raw.get("resource_type", "dbt"))
            nodes[str(unique_id)] = DependencyNode(str(unique_id), node_type, dataset=dataset)
        for unique_id, raw in models.items():
            if not isinstance(raw, Mapping):
                continue
            deps = raw.get("depends_on", {})
            dep_nodes = deps.get("nodes", []) if isinstance(deps, Mapping) else []
            for dep in dep_nodes if isinstance(dep_nodes, list) else []:
                source_id = str(dep)
                if source_id in nodes and str(unique_id) in nodes:
                    edges.append(
                        DependencyEdge(source_id=source_id, target_id=str(unique_id), relationship="depends_on")
                    )
        return SchemaImpactGraph(nodes=tuple(nodes.values()), edges=tuple(edges))


class OpenLineageDependencyProvider:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> SchemaImpactGraph:
        payload, issue = _load_json(self._path, "openlineage")
        if issue:
            return issue
        events = payload.get("events", [payload])
        nodes: list[DependencyNode] = []
        edges: list[DependencyEdge] = []
        for event in events if isinstance(events, list) else []:
            if not isinstance(event, Mapping):
                continue
            job = event.get("job", {})
            job_id = _job_id(job)
            nodes.append(DependencyNode(node_id=job_id, node_type="job"))
            for dataset_raw in event.get("inputs", []):
                dataset = _openlineage_dataset(dataset_raw)
                dataset_id = f"dataset:{dataset.key()}"
                nodes.append(DependencyNode(node_id=dataset_id, node_type="dataset", dataset=dataset))
                edges.append(DependencyEdge(source_id=dataset_id, target_id=job_id))
        return SchemaImpactGraph(nodes=tuple(nodes), edges=tuple(edges))


def providers_from_options(
    options: Mapping[str, Any],
    *,
    base_path: Path,
    default_owner: str | None,
    manifest: Mapping[str, Any] | None = None,
    manifest_path: Path | None = None,
) -> tuple[DependencyProvider, ...]:
    sources = options.get("sources", {})
    if not isinstance(sources, Mapping):
        return ()
    providers: list[DependencyProvider] = []
    if sources.get("manifests") is True and manifest is not None:
        providers.append(ManifestDependencyProvider(manifest, manifest_path=manifest_path))
    manual = sources.get("manual")
    if isinstance(manual, Mapping):
        providers.append(ManualDependencyProvider(manual, default_owner=default_owner))
    dbt = sources.get("dbt_manifest")
    if dbt:
        providers.append(DbtManifestDependencyProvider(_resolve(base_path, str(dbt))))
    openlineage = sources.get("openlineage")
    if openlineage:
        providers.append(OpenLineageDependencyProvider(_resolve(base_path, str(openlineage))))
    return tuple(providers)


def load_dependency_graph(providers: tuple[DependencyProvider, ...]) -> SchemaImpactGraph:
    return merge_graphs(tuple(provider.load() for provider in providers))


def _resolve(base_path: Path, raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else base_path / path


def _load_json(path: Path, source: str) -> tuple[dict[str, Any], SchemaImpactGraph | None]:
    if not path.exists():
        return {}, SchemaImpactGraph(warnings=(f"schema_impact.{source}.missing:{path}",))
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}, SchemaImpactGraph(blockers=(f"schema_impact.{source}.invalid_json:{path}",))
    return (
        (dict(payload), None)
        if isinstance(payload, Mapping)
        else ({}, SchemaImpactGraph(blockers=(f"schema_impact.{source}.not_object:{path}",)))
    )


def _dbt_dataset(raw: Mapping[str, Any]) -> DatasetRef:
    database = str(raw.get("database", "dbt"))
    schema = str(raw.get("schema", ""))
    table = str(raw.get("alias") or raw.get("identifier") or raw.get("name", ""))
    return DatasetRef.from_string(f"{database}.{schema}.{table}", default_namespace=database)


def _manifest_dataset(sink: Mapping[str, Any], *, default_namespace: str) -> DatasetRef:
    table = sink.get("table", {})
    if isinstance(table, Mapping):
        return DatasetRef.from_string(
            f"{default_namespace}.{table.get('schema', '')}.{table.get('name', '')}",
            default_namespace=default_namespace,
        )
    return DatasetRef.from_string(str(table or ""), default_namespace=default_namespace)


def _job_id(job: Any) -> str:
    if not isinstance(job, Mapping):
        return "openlineage.unknown_job"
    return ".".join(part for part in (job.get("namespace"), job.get("name")) if part) or "openlineage.unknown_job"


def _openlineage_dataset(raw: Any) -> DatasetRef:
    if not isinstance(raw, Mapping):
        return DatasetRef.from_string("unknown")
    return DatasetRef.from_string(f"{raw.get('namespace', 'unknown')}.{raw.get('name', '')}")


__all__ = [
    "DependencyProvider",
    "DbtManifestDependencyProvider",
    "ManifestDependencyProvider",
    "ManualDependencyProvider",
    "OpenLineageDependencyProvider",
    "load_dependency_graph",
    "providers_from_options",
]
