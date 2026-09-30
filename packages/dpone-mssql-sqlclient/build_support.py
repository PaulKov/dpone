"""Build-only normalization for the version-matched SqlClient companion."""

from __future__ import annotations

from pathlib import Path


def _remove_satellite_resource_assemblies(publish_root: Path) -> None:
    """Remove optional localization satellites from a framework-dependent publish tree.

    The companion exposes stable machine-readable diagnostics and does not consume
    localized dependency messages.  Keeping the neutral assemblies while dropping
    ``*.resources.dll`` files makes the wheel deterministic across NuGet locale
    additions and avoids shipping unused localized strings.
    """

    for resource in sorted(publish_root.rglob("*.resources.dll")):
        resource.unlink()

    directories = sorted(
        (path for path in publish_root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for directory in directories:
        if not any(directory.iterdir()):
            directory.rmdir()
