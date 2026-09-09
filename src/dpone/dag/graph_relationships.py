"""Relationship building for dependency graphs.

The actual dependency semantics are centralized in :mod:`dpone.dag.edge_resolver`
so that execution-order graph building, Airflow DAG building and explain/report
flows all agree on the same edge set.
"""

from __future__ import annotations

import logging

from dpone.dag.edge_resolver import build_dependency_indexes, collect_dependency_resolutions
from dpone.dag.node_registry import NodeRegistry
from dpone.dag.resolver import DependencyResolver

logger = logging.getLogger(__name__)


class RelationshipBuilder:
    """Build upstream/downstream links between already indexed nodes."""

    def __init__(self, registry: NodeRegistry, *, resolver: DependencyResolver | None = None) -> None:
        self.registry = registry
        self.resolver = resolver or DependencyResolver()

    def add_dependency(self, dependent_name: str, dependency_name: str) -> None:
        """Add dependency edge: dependency -> dependent."""
        dependent = self.registry.get(dependent_name)
        dependency = self.registry.get(dependency_name)

        if dependent and dependency:
            dependency.add_dependent(dependent)
            if dependent.upstream is None:
                dependent.upstream = dependency

    def build(self) -> None:
        """Build graph relationships from node.dependencies using unified edge semantics."""
        nodes = list(self.registry.iter_nodes())
        indexes = build_dependency_indexes(nodes)
        resolutions = collect_dependency_resolutions(nodes, indexes=indexes)

        for resolution in resolutions:
            if resolution.kind == "unresolved":
                raw = resolution.raw_path or resolution.group or ""
                logger.warning(
                    "Не удалось разрешить зависимость '%s' в задаче '%s'",
                    raw,
                    resolution.trigger_task_name,
                )
                continue

            for upstream_name in resolution.upstream_names:
                for downstream_name in resolution.downstream_names:
                    if upstream_name == downstream_name:
                        continue
                    self.add_dependency(downstream_name, upstream_name)


__all__ = ["RelationshipBuilder"]
