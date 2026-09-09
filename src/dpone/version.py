"""Lightweight installed dpone version lookup."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


def installed_version() -> str:
    """Return package metadata version with a source-tree fallback."""

    try:
        return version("dpone")
    except PackageNotFoundError:  # pragma: no cover - editable installs provide metadata
        return "0.0.0"


__all__ = ["installed_version"]
