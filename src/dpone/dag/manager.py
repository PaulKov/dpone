"""Dependency manager for ETL manifests.

Historically dpone worked in the model: 1 YAML = 1 ETL process.

For Variant C (batch manifests) one YAML may contain multiple processes.
DependencyManager now orchestrates graph building on the manifest level:
- a YAML file is loaded through ConfigLoader.get_manifest()
- every ProcessSpec becomes an individual ProcessNode
- dependency to file without selector means "wait for all" (all processes from that file)
- dependency "file.yaml#selector" means dependency to a specific process
- dependency "#selector" means dependency to a process inside current file
"""

from __future__ import annotations

import logging
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.dag.errors import DagConfigurationError

logger = logging.getLogger(__name__)


def _symbol(path: str) -> Any:
    module_name, attr = path.split(":", 1)
    return getattr(import_module(module_name), attr)


def _default_manifest_dir() -> Path:
    return Path(getattr(import_module("dpone.config.env"), "MANIFEST_DIR"))


class DependencyManager:
    """Loads manifests, resolves dependencies and exposes execution plans."""

    def __init__(
        self,
        base_path: Path | None = None,
        *,
        loader: Any | None = None,
        resolver: Any | None = None,
        graph: Any | None = None,
        task_group_index: Any | None = None,
        chain_loader: Any | None = None,
        finder: Any | None = None,
    ):
        self.base_path = base_path if base_path is not None else _default_manifest_dir()
        self.resolver = resolver or _symbol("dpone.dag.resolver:DependencyResolver")(self.base_path)
        self.graph = graph or _symbol("dpone.dag.graph:DependencyGraph")(resolver=self.resolver)
        self.loader = loader or _symbol("dpone.dag.loader:ConfigLoader")(self.base_path)
        self.task_group_index = task_group_index or _symbol("dpone.dag.task_group_index:TaskGroupFileIndex")(
            self.loader
        )
        self.chain_loader = chain_loader or _symbol("dpone.dag.manifest_chain_loader:ManifestChainLoader")(
            loader=self.loader,
            resolver=self.resolver,
            graph=self.graph,
            task_group_index=self.task_group_index,
        )
        self.finder = finder or _symbol("dpone.dag.finder:DependencyFinder")(
            self.graph,
            self.loader,
            self.resolver,
            processed_paths_getter=lambda: self.chain_loader.processed_paths,
            process_loader=self.chain_loader.load_process_recursive,
        )

    @property
    def processed_paths(self) -> set[Path]:
        """Compatibility accessor for already loaded manifest files."""
        return self.chain_loader.processed_paths

    def load_process_chain(self, root_yaml: Path) -> Any:
        """Load root manifest with all direct and reverse dependencies."""
        root_yaml = Path(root_yaml)

        if not root_yaml.exists():
            raise DagConfigurationError(f"Файл конфигурации не найден: {root_yaml}")

        root_manifest = self.loader.get_manifest(root_yaml, metadata_only=True)
        if not root_manifest or not root_manifest.processes:
            raise DagConfigurationError(f"Не удалось загрузить конфигурацию {root_yaml}")

        root_groups = {p.config.task_group for p in root_manifest.processes if p.config.task_group}
        for group in sorted(root_groups):
            self.chain_loader.load_task_group_files(group)

        root_nodes = self.chain_loader.load_manifest_recursive(root_yaml)
        if not root_nodes:
            raise DagConfigurationError(
                f"Не удалось загрузить корневую конфигурацию {root_yaml}. Проверьте ошибки выше."
            )

        self.finder.find_reverse_dependencies(root_yaml)
        self.finder.find_all_reverse_dependencies()

        self.graph.build_relationships()

        cycles = self.graph.detect_cycles()
        if cycles:
            raise DagConfigurationError(f"Обнаружены циклические зависимости: {cycles}")

        return root_nodes[0]

    def get_execution_plan(self) -> list[Any]:
        return self.graph.get_execution_order()

    def validate_dependencies(self) -> list[str]:
        """Best-effort validation of dependency file targets."""
        errors: list[str] = []

        for node in self.graph.nodes.values():
            for dep in node.dependencies:
                if getattr(dep, "group", None):
                    continue

                raw = getattr(dep, "path", "") or ""
                if not raw:
                    continue

                file_path, _ = self.resolver.resolve_dependency_ref(raw, node.config_path)
                if not self.resolver.validate_dependency_path(file_path):
                    errors.append(f"Зависимость не найдена: {raw} (resolved file: {file_path}) в задаче '{node.name}'")

        return errors

    # Legacy-compatible helpers used by some debug flows.
    def _load_process_recursive(self, yaml_path: Path) -> Any:
        return self.chain_loader.load_process_recursive(yaml_path)

    def _load_manifest_recursive(self, yaml_path: Path) -> list[Any]:
        return self.chain_loader.load_manifest_recursive(yaml_path)

    def _load_task_group_files(self, task_group: str) -> list[Any]:
        return self.chain_loader.load_task_group_files(task_group)


__all__ = ["DependencyManager"]
