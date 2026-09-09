"""Recursive manifest loading for dependency graphs."""

from __future__ import annotations

import logging
from pathlib import Path

from dpone.dag.errors import DagConfigurationError
from dpone.dag.graph import DependencyGraph
from dpone.dag.loader import ConfigLoader
from dpone.dag.node_factory import ProcessNodeFactory
from dpone.dag.resolver import DependencyResolver
from dpone.dag.task_group_index import TaskGroupFileIndex
from dpone.dag.yaml_types import ProcessNode

logger = logging.getLogger(__name__)


class ManifestChainLoader:
    """Recursively load manifests and all upstream manifests they depend on."""

    def __init__(
        self,
        *,
        loader: ConfigLoader,
        resolver: DependencyResolver,
        graph: DependencyGraph,
        task_group_index: TaskGroupFileIndex,
        node_factory: ProcessNodeFactory | None = None,
    ) -> None:
        self.loader = loader
        self.resolver = resolver
        self.graph = graph
        self.task_group_index = task_group_index
        self.node_factory = node_factory or ProcessNodeFactory()
        self._processed_paths: set[Path] = set()

    @property
    def processed_paths(self) -> set[Path]:
        return set(self._processed_paths)

    def load_process_recursive(self, yaml_path: Path) -> ProcessNode:
        nodes = self.load_manifest_recursive(yaml_path)
        if not nodes:
            raise DagConfigurationError(f"Не удалось загрузить конфигурацию {yaml_path}")
        return nodes[0]

    def load_manifest_recursive(self, yaml_path: Path) -> list[ProcessNode]:
        yaml_path = Path(yaml_path)

        if yaml_path in self._processed_paths:
            return self.graph.get_nodes_by_path(yaml_path)

        self._processed_paths.add(yaml_path)
        manifest = self.loader.get_manifest(yaml_path, metadata_only=True)
        if not manifest or not manifest.processes:
            raise DagConfigurationError(f"Не удалось загрузить конфигурацию {yaml_path}")

        for spec in manifest.processes:
            node = self.node_factory.from_spec(spec, yaml_path)
            try:
                self.graph.add_node(node)
            except ValueError as exc:
                raise DagConfigurationError(str(exc)) from exc

        for spec in manifest.processes:
            for dep in spec.config.dependencies:
                group = getattr(dep, "group", None)
                if group:
                    self.load_task_group_files(group)
                    continue

                raw = getattr(dep, "path", "") or ""
                if not raw:
                    continue

                dep_file_path, _ = self.resolver.resolve_dependency_ref(raw, yaml_path)
                if not self.resolver.validate_dependency_path(dep_file_path):
                    logger.warning("Путь зависимости не найден: %s (resolved file: %s)", raw, dep_file_path)
                    continue

                self.load_manifest_recursive(dep_file_path)

        return self.graph.get_nodes_by_path(yaml_path)

    def load_task_group_files(self, task_group: str) -> list[ProcessNode]:
        group_files = self.task_group_index.get_files(task_group)
        if not group_files:
            logger.warning("Task group '%s' not found in cache", task_group)
            return []

        for yaml_file in sorted(group_files):
            try:
                self.load_manifest_recursive(yaml_file)
            except Exception as exc:  # pragma: no cover - defensive logging path
                logger.warning("Failed to load %s for task_group='%s': %s", yaml_file, task_group, exc)

        return list(self.graph.nodes_by_group.get(task_group, []))


__all__ = ["ManifestChainLoader"]
