from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.models import LoadedManifest, ProcessSpec


import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.loader import ManifestLoaderRouter


@dataclass(frozen=True, slots=True)
class ManifestCommandContext:
    """Shared context for manifest CLI commands."""

    registry_paths: tuple[Path, ...]
    loader: ManifestLoaderRouter


class ManifestSelectionError(ManifestConfigurationError):
    """Raised when a process selection is ambiguous or invalid."""


def normalize_registry_paths(raw: Sequence[str | os.PathLike[str] | None]) -> tuple[Path, ...]:
    """Normalize repeated --registry arguments.

    Behaviour intentionally mirrors DAG helpers:
    - ignore empty values
    - dedupe by resolved path
    - keep relative paths as provided by the caller
    """

    out: list[Path] = []
    seen: set[str] = set()
    for item in raw or ():
        if item is None:
            continue
        s = str(item).strip()
        if not s:
            continue
        path = Path(s)
        key = str(path.resolve(strict=False))
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return tuple(out)


def build_manifest_context(args: object, *, ctx: object, registry_attr: str = "registry") -> ManifestCommandContext:
    registry_paths = normalize_registry_paths(getattr(args, registry_attr, None) or [])
    return ManifestCommandContext(
        registry_paths=registry_paths,
        loader=ManifestLoaderRouter(registry_paths=registry_paths),
    )


def iter_yaml_paths(path: Path, *, recursive: bool) -> Iterable[Path]:
    """Yield YAML files from a file or directory, sorted and deterministic."""

    if path.is_file():
        yield path
        return

    if not path.exists():
        raise ManifestConfigurationError(f"Path not found: {path}")

    if not path.is_dir():
        raise ManifestConfigurationError(f"Unsupported path: {path}")

    pattern = "**/*.y*ml" if recursive else "*.y*ml"
    for item in sorted(path.glob(pattern)):
        if item.name.startswith("."):
            continue
        yield item


def load_manifest(path: Path, *, manifest_ctx: ManifestCommandContext, metadata_only: bool = True) -> LoadedManifest:
    return manifest_ctx.loader.load(path, metadata_only=metadata_only)


def resolve_single_process(manifest: LoadedManifest, *, selector: str | None) -> ProcessSpec:
    """Return a single process from a loaded manifest.

    Rules:
    - if selector is given: match by selector or by process name
    - else: manifest must contain exactly one process
    """

    if selector:
        for spec in manifest.processes:
            if spec.selector == selector or spec.name == selector:
                return spec
        available = [p.selector or p.name for p in manifest.processes]
        raise ManifestSelectionError(f"Selector '{selector}' not found in {manifest.path}. Available: {available}")

    if len(manifest.processes) != 1:
        raise ManifestSelectionError(
            f"Manifest {manifest.path} contains {len(manifest.processes)} processes. "
            "Use --selector <process-name-or-selector>."
        )

    return manifest.processes[0]


__all__ = [
    "ManifestCommandContext",
    "ManifestSelectionError",
    "build_manifest_context",
    "iter_yaml_paths",
    "load_manifest",
    "normalize_registry_paths",
    "resolve_single_process",
]
