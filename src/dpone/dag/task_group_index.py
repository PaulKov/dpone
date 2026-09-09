"""Lazy task-group -> manifest file index."""

from __future__ import annotations

import logging
from pathlib import Path

from dpone.dag.loader import ConfigLoader
from dpone.manifest.models import LoadedManifest

logger = logging.getLogger(__name__)


class TaskGroupFileIndex:
    """Builds and caches mapping task_group -> YAML files."""

    def __init__(self, loader: ConfigLoader) -> None:
        self.loader = loader
        self._groups: dict[str, set[Path]] = {}
        self._initialized = False

    @property
    def initialized(self) -> bool:
        return self._initialized

    def initialize(self) -> None:
        if self._initialized:
            return

        for yaml_file in self.loader.get_yaml_files():
            manifest = self.loader.get_manifest(yaml_file, metadata_only=True)
            if not manifest:
                continue
            for group in self._groups_in_manifest(manifest):
                self._groups.setdefault(group, set()).add(yaml_file)

        self._initialized = True

    def get_files(self, task_group: str) -> set[Path]:
        if not self._initialized:
            self.initialize()
        return set(self._groups.get(task_group, set()))

    @staticmethod
    def _groups_in_manifest(manifest: LoadedManifest) -> set[str]:
        return {p.config.task_group for p in manifest.processes if p.config.task_group}


__all__ = ["TaskGroupFileIndex"]
