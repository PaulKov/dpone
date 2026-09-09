"""Manifest loaders.

Step 1 introduced the manifest layer (LoadedManifest / ProcessSpec) and the
legacy loader (1 yaml == 1 process).

Step 3 adds a batch loader (Variant C): 1 yaml == many processes.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

import yaml

from dpone.config.env import SOURCES_REGISTRY_PATHS
from dpone.manifest.authoring import AuthoringCompiler, default_authoring_compiler
from dpone.manifest.errors import LegacySingleManifestConfigurationError, ManifestConfigurationError
from dpone.manifest.models import LoadedManifest, ProcessSpec

logger = logging.getLogger(__name__)


def _symbol(path: str) -> Any:
    module_name, attr = path.split(":", 1)
    return getattr(import_module(module_name), attr)


def _parse_etl_config(data: dict[str, Any], *, base_path: Path, metadata_only: bool) -> Any:
    try:
        return _symbol("dpone.dag.config:ETLProcessConfig").from_dict(
            data,
            base_path=base_path,
            metadata_only=metadata_only,
        )
    except _symbol("dpone.dag.errors:DagConfigurationError") as exc:
        raise LegacySingleManifestConfigurationError(f"Некорректная конфигурация YAML: {exc}") from exc


def _default_batch_loader(registry_paths: Sequence[Path]) -> BatchManifestLoader:
    return _symbol("dpone.manifest.batch_loader:BatchYamlManifestLoader")(registry_paths=registry_paths)


class ManifestLoader(Protocol):
    """Loads a YAML manifest into one or more ProcessSpec."""

    def load(self, path: Path, *, metadata_only: bool = True) -> LoadedManifest:  # pragma: no cover
        ...


class BatchManifestLoader(ManifestLoader, Protocol):
    """Loads canonical batch mappings without reparsing authoring YAML."""

    def load_mapping(
        self,
        data: dict[str, Any],
        *,
        path: Path,
        metadata_only: bool = True,
    ) -> LoadedManifest:  # pragma: no cover
        ...


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ManifestConfigurationError(f"YAML конфигурация не найдена: {path}")

    try:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ManifestConfigurationError(f"Ошибка парсинга YAML {path}: {exc}") from exc


class SingleYamlManifestLoader:
    """Legacy loader: 1 YAML file == 1 ETL process."""

    kind = "dpone.single.v1"

    def load(self, path: Path, *, metadata_only: bool = True) -> LoadedManifest:
        data = _read_yaml(path)

        # NOTE: we intentionally reuse ETLProcessConfig parsing logic.
        # In metadata_only mode it will not resolve credentials.
        config = _parse_etl_config(data, base_path=path.parent, metadata_only=metadata_only)

        spec = ProcessSpec(
            name=config.name,
            config_path=path,
            config=config,
            raw_config=data,
            selector=None,
        )

        return LoadedManifest(
            path=path,
            kind=self.kind,
            raw=data,
            processes=(spec,),
        )


class ManifestLoaderRouter:
    """Routes to a concrete loader by inspecting the YAML header.

    This allows gradual rollout:
    - existing manifests stay as-is (legacy)
    - new batch manifests will be detected by `kind: dpone.batch.v1`
    """

    def __init__(
        self,
        *,
        single_loader: ManifestLoader | None = None,
        batch_loader: BatchManifestLoader | None = None,
        authoring_compiler: AuthoringCompiler | None = None,
        registry_paths: Sequence[Path] | None = None,
    ) -> None:
        self._single = single_loader or SingleYamlManifestLoader()
        # Registry paths are treated as optional defaults.
        # Precedence order is implemented in apply_registries():
        # extra_registry_paths (env/cli) -> manifest registry/registries.
        effective_registry_paths = tuple(SOURCES_REGISTRY_PATHS) + tuple(registry_paths or ())
        self._batch = batch_loader or _default_batch_loader(effective_registry_paths)
        self._authoring_compiler = authoring_compiler or default_authoring_compiler()

    def load(self, path: Path, *, metadata_only: bool = True) -> LoadedManifest:
        data = _read_yaml(path)
        kind = str(data.get("kind") or data.get("schema") or "")

        normalized_kind = kind.strip()
        if normalized_kind in {"dpone.flow.v1", "dpone.pipeline.v1"} or (
            normalized_kind == "dpone.batch.v1" and "processes" in data and "schemas" not in data
        ):
            compilation = self._authoring_compiler.compile(data, source_path=path)
            loaded = self._batch.load_mapping(
                dict(compilation.canonical_manifest),
                path=path,
                metadata_only=metadata_only,
            )
            return LoadedManifest(
                path=path,
                kind=compilation.canonical_kind,
                raw=data,
                processes=loaded.processes,
                source_kind=compilation.source_kind,
                deprecated_aliases=compilation.deprecated_aliases,
            )

        if normalized_kind == "dpone.batch.v1":
            return self._batch.load(path, metadata_only=metadata_only)

        # Default: legacy manifest
        return self._single.load(path, metadata_only=metadata_only)
