"""Batch YAML manifest loader (Variant C).

Reads a dpone.batch.v1 manifest and compiles it into multiple ProcessSpec.

At this step we only *compile* the processes. Dependency graph support for
many-nodes-per-file will be implemented in later steps.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.batch_compiler_impl import BatchManifestCompiler
from dpone.manifest.conventions import apply_conventions
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.models import LoadedManifest, ProcessSpec
from dpone.manifest.registry import apply_registries

logger = logging.getLogger(__name__)


def _symbol(path: str) -> Any:
    module_name, attr = path.split(":", 1)
    return getattr(import_module(module_name), attr)


def _parse_etl_config(data: dict[str, Any], *, base_path: Path, metadata_only: bool) -> Any:
    return _symbol("dpone.dag.config:ETLProcessConfig").from_dict(
        data,
        base_path=base_path,
        metadata_only=metadata_only,
    )


class BatchYamlManifestLoader:
    """Loads a batch manifest (kind: dpone.batch.v1)."""

    kind = "dpone.batch.v1"

    def __init__(
        self,
        *,
        compiler: BatchManifestCompiler | None = None,
        registry_paths: Sequence[Path] = (),
    ) -> None:
        self._compiler = compiler or BatchManifestCompiler()
        self._registry_paths = tuple(Path(p) for p in registry_paths if str(p))

    def load(self, path: Path, *, metadata_only: bool = True) -> LoadedManifest:
        data = self._read_yaml(path)
        return self.load_mapping(data, path=path, metadata_only=metadata_only)

    def load_mapping(
        self,
        data: dict[str, Any],
        *,
        path: Path,
        metadata_only: bool = True,
    ) -> LoadedManifest:
        """Load an already parsed canonical batch mapping."""

        kind = str(data.get("kind") or "").strip()
        if kind != self.kind:
            raise ManifestConfigurationError(
                f"Неверный kind для BatchYamlManifestLoader: '{kind}'. Ожидалось '{self.kind}' ({path})"
            )

        # Step 8: apply optional conventions/presets (naming + metadata bootstrapping)
        effective = apply_conventions(data, manifest_path=path)

        # Step 9: apply optional source registries (host/type/db -> vars defaults)
        effective = apply_registries(
            effective,
            manifest_path=path,
            extra_registry_paths=self._registry_paths,
        )

        compiled = self._compiler.compile(effective, manifest_path=path)
        specs: list[ProcessSpec] = []

        for proc in compiled:
            try:
                cfg = _parse_etl_config(proc.raw_config, base_path=path.parent, metadata_only=metadata_only)
            except Exception as exc:
                raise ManifestConfigurationError(
                    f"Ошибка парсинга скомпилированного процесса '{proc.selector}' из {path}: {exc}"
                ) from exc

            specs.append(
                ProcessSpec(
                    name=cfg.name,
                    config_path=path,
                    config=cfg,
                    raw_config=proc.raw_config,
                    selector=proc.selector,
                )
            )

        return LoadedManifest(
            path=path,
            kind=self.kind,
            raw=effective,
            processes=tuple(specs),
        )

    @staticmethod
    def _read_yaml(path: Path) -> dict[str, Any]:
        if not path.exists():
            raise ManifestConfigurationError(f"YAML конфигурация не найдена: {path}")

        try:
            with path.open("r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except yaml.YAMLError as exc:
            raise ManifestConfigurationError(f"Ошибка парсинга YAML {path}: {exc}") from exc
