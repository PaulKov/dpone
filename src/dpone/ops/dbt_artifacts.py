"""dbt artifact graph models and parser."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

FAILING_DBT_STATUSES = {"error", "fail", "failed", "runtime error"}
MODEL_RESOURCE_TYPES = {"model", "seed", "snapshot"}


@dataclass(frozen=True, slots=True)
class DbtLineageNode:
    unique_id: str
    name: str
    resource_type: str
    package_name: str
    relation_name: str
    status: str
    depends_on: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["depends_on"] = list(self.depends_on)
        return data


@dataclass(frozen=True, slots=True)
class DbtLineageEdge:
    upstream: str
    downstream: str

    def to_dict(self) -> dict[str, str]:
        return {"upstream": self.upstream, "downstream": self.downstream}


@dataclass(frozen=True, slots=True)
class DbtArtifactGraph:
    nodes: Mapping[str, DbtLineageNode]
    edges: tuple[DbtLineageEdge, ...]
    invocation_id: str
    results_passed: bool


class DbtArtifactParser:
    """Parses dbt ``manifest.json`` and ``run_results.json`` payloads."""

    def parse(self, manifest: Mapping[str, Any], run_results: Mapping[str, Any]) -> DbtArtifactGraph:
        nodes = self._nodes(manifest, run_results)
        return DbtArtifactGraph(
            nodes=nodes,
            edges=self._edges(nodes),
            invocation_id=self.invocation_id(run_results),
            results_passed=self.results_passed(run_results),
        )

    def _nodes(self, manifest: Mapping[str, Any], run_results: Mapping[str, Any]) -> dict[str, DbtLineageNode]:
        statuses = self.statuses(run_results)
        nodes: dict[str, DbtLineageNode] = {}
        for collection_name in ("sources", "nodes"):
            raw_collection = manifest.get(collection_name, {})
            if not isinstance(raw_collection, Mapping):
                continue
            for unique_id, payload in raw_collection.items():
                if isinstance(payload, Mapping):
                    node = self._node(str(unique_id), payload, statuses)
                    nodes[node.unique_id] = node
        return nodes

    def _node(
        self,
        unique_id: str,
        payload: Mapping[str, Any],
        statuses: Mapping[str, str],
    ) -> DbtLineageNode:
        depends_on = payload.get("depends_on", {})
        dependencies = depends_on.get("nodes", []) if isinstance(depends_on, Mapping) else []
        source_name = str(payload.get("source_name", ""))
        name = str(payload.get("name", unique_id))
        return DbtLineageNode(
            unique_id=unique_id,
            name=name,
            resource_type=str(payload.get("resource_type", "unknown")),
            package_name=str(payload.get("package_name", "")),
            relation_name=self._relation_name(payload, source_name, name),
            status=statuses.get(unique_id, "unknown"),
            depends_on=tuple(str(item) for item in dependencies if isinstance(item, str)),
        )

    @staticmethod
    def _relation_name(payload: Mapping[str, Any], source_name: str, name: str) -> str:
        relation_name = payload.get("relation_name")
        if isinstance(relation_name, str) and relation_name:
            return relation_name
        if source_name:
            return f"{source_name}.{name}"
        return name

    @staticmethod
    def _edges(nodes: Mapping[str, DbtLineageNode]) -> tuple[DbtLineageEdge, ...]:
        edges: list[DbtLineageEdge] = []
        for node in nodes.values():
            for upstream in node.depends_on:
                if upstream in nodes:
                    edges.append(DbtLineageEdge(upstream=upstream, downstream=node.unique_id))
        return tuple(edges)

    @staticmethod
    def statuses(run_results: Mapping[str, Any]) -> dict[str, str]:
        results = run_results.get("results", [])
        if not isinstance(results, list):
            return {}
        statuses: dict[str, str] = {}
        for item in results:
            if isinstance(item, Mapping):
                unique_id = item.get("unique_id")
                status = item.get("status")
                if isinstance(unique_id, str) and isinstance(status, str):
                    statuses[unique_id] = status
        return statuses

    @classmethod
    def results_passed(cls, run_results: Mapping[str, Any]) -> bool:
        statuses = cls.statuses(run_results)
        return not any(status.lower() in FAILING_DBT_STATUSES for status in statuses.values())

    @staticmethod
    def invocation_id(run_results: Mapping[str, Any]) -> str:
        metadata = run_results.get("metadata", {})
        if isinstance(metadata, Mapping):
            invocation_id = metadata.get("invocation_id")
            if isinstance(invocation_id, str) and invocation_id:
                return invocation_id
        return "unknown_dbt_invocation"
