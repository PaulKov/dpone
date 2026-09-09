"""Credential-free process projection for advisory commands."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.authoring import AuthoringCompiler, default_authoring_compiler
from dpone.manifest.errors import ManifestConfigurationError

_AUTHORING_KINDS = frozenset({"dpone.batch.v1", "dpone.flow.v1", "dpone.pipeline.v1"})


class AdvisoryManifestProcessReader:
    """Read one process through the canonical authoring compiler when required."""

    def __init__(self, *, compiler: AuthoringCompiler | None = None) -> None:
        self._compiler = compiler or default_authoring_compiler()

    def read(self, path: str | Path) -> dict[str, Any]:
        source_path = Path(path)
        payload = _read_mapping(source_path)
        kind = str(payload.get("kind") or payload.get("schema") or "").strip()
        if kind not in _AUTHORING_KINDS:
            return payload

        compilation = self._compiler.compile(payload, source_path=source_path)
        if not compilation.processes:
            raise ManifestConfigurationError("Advisory command requires at least one compiled process.")
        return dict(compilation.processes[0])


def _read_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ManifestConfigurationError(f"YAML configuration was not found: {path}")
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ManifestConfigurationError(f"YAML configuration could not be read: {path}") from exc
    if not isinstance(payload, Mapping):
        raise ManifestConfigurationError(f"YAML configuration must contain an object: {path}")
    return dict(payload)


__all__ = ["AdvisoryManifestProcessReader"]
