"""Core dataclasses for dpone manifests."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ProcessSpec:
    """A single ETL process produced from a manifest.

    Notes:
    - For *legacy* manifests: one YAML file maps to one ProcessSpec.
    - For *batch* manifests (Variant C): one YAML file maps to many ProcessSpec.
    """

    name: str
    config_path: Path
    config: Any
    raw_config: Mapping[str, Any]
    selector: str | None = None

    @property
    def task_group(self) -> str | None:
        return self.config.task_group


@dataclass(frozen=True, slots=True)
class LoadedManifest:
    """Loaded source with canonical process kind and optional authoring origin."""

    path: Path
    kind: str
    raw: Mapping[str, Any]
    processes: tuple[ProcessSpec, ...]
    source_kind: str | None = None
    deprecated_aliases: tuple[str, ...] = ()


__all__ = ["LoadedManifest", "ProcessSpec"]
