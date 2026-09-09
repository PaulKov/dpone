"""Reverse dependency scanning for manifests.

Dpone builds DAGs not only from direct depends_on declarations but also by
pulling in "reverse" dependencies: manifests that refer to the selected root
manifest or its task groups.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

from dpone.dag.graph import DependencyGraph
from dpone.dag.loader import ConfigLoader
from dpone.dag.resolver import DependencyResolver
from dpone.dag.yaml_types import ProcessNode

logger = logging.getLogger(__name__)


class DependencyFinder:
    """Find reverse dependencies between YAML manifests."""

    def __init__(
        self,
        graph: DependencyGraph,
        loader: ConfigLoader,
        resolver: DependencyResolver,
        *,
        processed_paths_getter: Callable[[], set[Path]],
        process_loader: Callable[[Path], ProcessNode],
    ) -> None:
        self.graph = graph
        self.loader = loader
        self.resolver = resolver
        self._processed_paths_getter = processed_paths_getter
        self._process_loader = process_loader

    def find_reverse_dependencies(self, target_yaml: Path) -> None:
        """Recursively find manifests that reference target_yaml or its groups."""
        target_yaml = Path(target_yaml)

        to_process = [target_yaml]
        processed_targets: set[Path] = set()

        while to_process:
            current_target = to_process.pop(0)
            if current_target in processed_targets:
                continue
            processed_targets.add(current_target)

            target_manifest = self.loader.get_manifest(current_target, metadata_only=True)
            current_groups = (
                {p.config.task_group for p in target_manifest.processes if p.config.task_group}
                if target_manifest
                else set()
            )

            for yaml_file in self.loader.get_yaml_files():
                if yaml_file in self._processed_paths_getter():
                    continue

                manifest = self.loader.get_manifest(yaml_file, metadata_only=True)
                if manifest is None:
                    continue

                if self._manifest_depends_on_target(
                    manifest=manifest,
                    manifest_path=yaml_file,
                    target_file=current_target,
                    target_groups=current_groups,
                ):
                    self._process_loader(yaml_file)
                    to_process.append(yaml_file)

    def find_all_reverse_dependencies(self) -> None:
        """Find reverse dependencies for all currently loaded manifests."""
        loaded_files = list(self._processed_paths_getter())
        if not loaded_files:
            return

        loaded_files_set = set(loaded_files)
        loaded_task_groups: set[str] = set()
        for loaded_file in loaded_files:
            manifest = self.loader.get_manifest(loaded_file, metadata_only=True)
            if not manifest:
                continue
            for proc in manifest.processes:
                if proc.config.task_group:
                    loaded_task_groups.add(proc.config.task_group)

        for yaml_file in self.loader.get_yaml_files():
            if yaml_file in self._processed_paths_getter():
                continue

            manifest = self.loader.get_manifest(yaml_file, metadata_only=True)
            if manifest is None:
                continue

            has_dependency = False
            for proc in manifest.processes:
                for dep in proc.config.dependencies:
                    if dep.group and dep.group in loaded_task_groups:
                        logger.info(
                            "Найдена обратная зависимость по группе: %s зависит от группы '%s'",
                            yaml_file,
                            dep.group,
                        )
                        has_dependency = True
                        break

                    if dep.path:
                        dep_file_path, _ = self.resolver.resolve_dependency_ref(dep.path, yaml_file)
                        if dep_file_path in loaded_files_set:
                            logger.info(
                                "Найдена обратная зависимость по файлу: %s ссылается на %s",
                                yaml_file,
                                dep_file_path,
                            )
                            has_dependency = True
                            break

                if has_dependency:
                    break

            if has_dependency:
                self._process_loader(yaml_file)
                loaded_files_set.add(yaml_file)
                for proc in manifest.processes:
                    if proc.config.task_group:
                        loaded_task_groups.add(proc.config.task_group)

    def _manifest_depends_on_target(
        self,
        *,
        manifest,
        manifest_path: Path,
        target_file: Path,
        target_groups: set[str],
    ) -> bool:
        for proc in manifest.processes:
            for dep in proc.config.dependencies:
                if dep.group and dep.group in target_groups:
                    logger.info(
                        "Найдена обратная зависимость по группе: %s зависит от группы '%s'",
                        manifest_path,
                        dep.group,
                    )
                    return True

                if dep.path:
                    dep_file_path, _ = self.resolver.resolve_dependency_ref(dep.path, manifest_path)
                    if dep_file_path == target_file:
                        logger.info(
                            "Найдена обратная зависимость по файлу: %s ссылается на %s",
                            manifest_path,
                            target_file,
                        )
                        return True
        return False


__all__ = ["DependencyFinder"]
