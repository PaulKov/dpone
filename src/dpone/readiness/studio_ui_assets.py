"""Side-effect-free compatibility probe for optional Studio UI assets."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version
from importlib.util import find_spec
from pathlib import Path

_DISTRIBUTION = "dpone-studio-assets"
_PACKAGE = "dpone_studio_assets"
_MANIFEST = "dpone-studio-assets.json"
_MAX_MANIFEST_BYTES = 64 * 1024
_SUPPORTED_VERSION = re.compile(r"^0\.1\.[0-9]+$")


@dataclass(frozen=True, slots=True)
class StudioUiAssetsStatus:
    status: str
    version: str | None
    reason_code: str

    def to_dict(self) -> dict[str, str | None]:
        return {
            "ui_status": self.status,
            "ui_version": self.version,
            "ui_reason_code": self.reason_code,
            "usability_status": "UNVERIFIED",
            "release_verdict": "NO-GO",
            "release_reason_code": "studio_usability_evidence_missing",
        }


def probe_studio_ui_assets() -> StudioUiAssetsStatus:
    """Inspect package metadata and static files without importing UI code."""

    try:
        spec = find_spec(_PACKAGE)
    except (ImportError, ModuleNotFoundError, ValueError):
        return _status("incompatible", None, "ui_package_discovery_failed")
    if spec is None:
        return _status("not_installed", None, "ui_package_not_installed")
    locations = tuple(spec.submodule_search_locations or ())
    if len(locations) != 1:
        return _status("incompatible", None, "ui_package_layout_invalid")
    try:
        version = distribution_version(_DISTRIBUTION)
    except PackageNotFoundError:
        return _status("incompatible", None, "ui_distribution_metadata_missing")
    return validate_studio_ui_assets(Path(locations[0]), version=version)


def validate_studio_ui_assets(
    package_root: Path,
    *,
    version: str,
) -> StudioUiAssetsStatus:
    """Validate one discovered assets package against the v1 protocol."""

    if _SUPPORTED_VERSION.fullmatch(version) is None:
        return _status("incompatible", version, "ui_version_incompatible")
    root = package_root.resolve(strict=False)
    manifest_path = root / _MANIFEST
    content = _read_confined_file(root, manifest_path, max_bytes=_MAX_MANIFEST_BYTES)
    if content is None:
        return _status("incompatible", version, "ui_manifest_missing_or_unsafe")
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _status("incompatible", version, "ui_manifest_invalid")
    if not isinstance(payload, dict) or set(payload) != {
        "schema",
        "api_version",
        "package_version",
        "entrypoint",
    }:
        return _status("incompatible", version, "ui_manifest_invalid")
    if (
        payload.get("schema") != "dpone.studio-assets.v1"
        or payload.get("api_version") != "v1"
        or payload.get("package_version") != version
    ):
        return _status("incompatible", version, "ui_protocol_incompatible")
    entrypoint = payload.get("entrypoint")
    if not isinstance(entrypoint, str) or not entrypoint or "\\" in entrypoint:
        return _status("incompatible", version, "ui_entrypoint_unsafe")
    entrypoint_path = root / entrypoint
    if _read_confined_file(root, entrypoint_path, max_bytes=8 * 1024 * 1024) is None:
        return _status("incompatible", version, "ui_entrypoint_missing_or_unsafe")
    return _status("installed", version, "ui_assets_compatible")


def _read_confined_file(root: Path, path: Path, *, max_bytes: int) -> bytes | None:
    try:
        if path.is_symlink():
            return None
        resolved = path.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
        if not resolved.is_file() or resolved.stat().st_size > max_bytes:
            return None
        return resolved.read_bytes()
    except (OSError, RuntimeError, ValueError):
        return None


def _status(
    status: str,
    version: str | None,
    reason_code: str,
) -> StudioUiAssetsStatus:
    return StudioUiAssetsStatus(
        status=status,
        version=version,
        reason_code=reason_code,
    )


__all__ = [
    "StudioUiAssetsStatus",
    "probe_studio_ui_assets",
    "validate_studio_ui_assets",
]
