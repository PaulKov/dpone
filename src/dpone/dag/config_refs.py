"""Helpers for resolving manifest references into process configs.

This module keeps path/selector parsing and manifest-loader integration out of
:mod:`dpone.dag.config_models`, so the public ETLProcessConfig model stays
small and focused.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.dag.errors import DagConfigurationError

if TYPE_CHECKING:  # pragma: no cover
    from dpone.dag.config_models import ETLProcessConfig


ManifestRef = tuple[Path, str | None]


def split_manifest_ref(yaml_path: str | Path) -> ManifestRef:
    """Parse ``<path>.yaml#<selector>`` into ``(Path, selector)``."""
    s = str(yaml_path)
    if "#" in s:
        file_part, selector = s.split("#", 1)
        return Path(file_part), selector.strip() or None
    return Path(s), None


def load_via_manifest_loader(
    path: Path,
    selector: str | None,
    *,
    metadata_only: bool,
) -> ETLProcessConfig:
    """Load config through ``ManifestLoaderRouter`` (supports batch manifests)."""
    from dpone.manifest.loader import ManifestLoaderRouter

    loader = ManifestLoaderRouter()
    manifest = loader.load(path, metadata_only=metadata_only)

    if selector:
        for spec in manifest.processes:
            if spec.selector == selector or spec.name == selector:
                return spec.config
        raise DagConfigurationError(
            f"Процесс '{selector}' не найден в manifest {path}. "
            f"Доступные selector'ы: {[p.selector for p in manifest.processes]}"
        )

    if len(manifest.processes) != 1:
        raise DagConfigurationError(
            f"Manifest {path} содержит {len(manifest.processes)} процессов. Укажите selector: '<path>.yaml#<selector>'."
        )

    return manifest.processes[0].config


__all__ = [
    "ManifestRef",
    "split_manifest_ref",
    "load_via_manifest_loader",
]
