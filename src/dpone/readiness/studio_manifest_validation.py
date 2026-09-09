"""Bounded canonical manifest validation shared by Studio query services."""

from __future__ import annotations

import tempfile
from pathlib import Path

from dpone.manifest.loader import ManifestLoaderRouter
from dpone.manifest.models import LoadedManifest
from dpone.readiness.studio_errors import StudioError

MAX_STUDIO_MANIFEST_BYTES = 1024 * 1024


def validate_manifest_size(manifest_yaml: str) -> None:
    if len(manifest_yaml.encode("utf-8")) > MAX_STUDIO_MANIFEST_BYTES:
        raise StudioError(
            "DPONE_STUDIO_MANIFEST_TOO_LARGE",
            "Inline manifest exceeds the 1 MiB limit.",
        )


def validate_manifest_yaml(manifest_yaml: str) -> None:
    load_manifest_yaml(manifest_yaml)


def load_manifest_yaml(manifest_yaml: str) -> LoadedManifest:
    """Compile one bounded inline source and return its resolved process snapshot."""

    validate_manifest_size(manifest_yaml)
    with tempfile.NamedTemporaryFile(
        "w",
        suffix=".batch.yaml",
        encoding="utf-8",
        delete=False,
    ) as handle:
        handle.write(manifest_yaml)
        path = Path(handle.name)
    try:
        return ManifestLoaderRouter().load(path, metadata_only=True)
    except Exception as exc:
        raise StudioError(
            "DPONE_MANIFEST_DRAFT_INVALID",
            "Manifest draft failed canonical validation.",
        ) from exc
    finally:
        path.unlink(missing_ok=True)


def load_manifest_path(path: Path) -> LoadedManifest:
    """Compile one existing project source through the canonical manifest loader."""

    try:
        return ManifestLoaderRouter().load(path, metadata_only=True)
    except Exception as exc:
        raise StudioError(
            "DPONE_MANIFEST_DRAFT_INVALID",
            "Manifest source failed canonical validation.",
        ) from exc


__all__ = [
    "MAX_STUDIO_MANIFEST_BYTES",
    "load_manifest_path",
    "load_manifest_yaml",
    "validate_manifest_size",
    "validate_manifest_yaml",
]
