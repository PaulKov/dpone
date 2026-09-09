"""Dependency graph facade.

Important for Variant C (batch manifests): one YAML may contain many processes.
The graph exposes compatibility properties while delegating responsibilities to
smaller collaborators:

- NodeRegistry           -> indexing / uniqueness
- RelationshipBuilder    -> dependency edges
- GraphAlgorithms        -> topo order / cycle detection
"""

from __future__ import annotations

from pathlib import Path

from dpone.dag.graph_algorithms import GraphAlgorithms
from dpone.dag.graph_relationships import RelationshipBuilder
from dpone.dag.node_registry import NodeRegistry
from dpone.dag.resolver import DependencyResolver
from dpone.dag.yaml_types import ProcessNode


class DependencyGraph:
    """Facade over node indexing, relationship building and graph algorithms."""

    def __init__(
        self,
        *,
        resolver: DependencyResolver | None = None,
        registry: NodeRegistry | None = None,
        relationships: RelationshipBuilder | None = None,
    ) -> None:
        self.registry = registry or NodeRegistry()
        self.relationships = relationships or RelationshipBuilder(
            self.registry,
            resolver=resolver,
        )

    @property
    def nodes(self) -> dict[str, ProcessNode]:
        return self.registry.nodes

    @property
    def nodes_by_path(self):
        return self.registry.nodes_by_path

    @property
    def nodes_by_ref(self):
        return self.registry.nodes_by_ref

    @property
    def nodes_by_group(self):
        return self.registry.nodes_by_group

    def add_node(self, node: ProcessNode) -> None:
        self.registry.add(node)

    def get_node(self, name: str) -> ProcessNode | None:
        return self.registry.get(name)

    def get_nodes_by_path(self, path: Path) -> list[ProcessNode]:
        return self.registry.get_by_path(path)

    def get_node_by_ref(self, path: Path, selector: str) -> ProcessNode | None:
        return self.registry.get_by_ref(path, selector)

    def _find_node_by_selector(self, path: Path, selector: str) -> ProcessNode | None:
        return self.registry.find_by_selector(path, selector)

    def add_dependency(self, dependent_name: str, dependency_name: str) -> None:
        self.relationships.add_dependency(dependent_name, dependency_name)

    def build_relationships(self) -> None:
        self.relationships.build()

    def get_execution_order(self) -> list[ProcessNode]:
        return GraphAlgorithms.topological_sort(self.nodes)

    def detect_cycles(self) -> list[list[str]]:
        return GraphAlgorithms.detect_cycles(self.nodes)


__all__ = ["DependencyGraph"]
