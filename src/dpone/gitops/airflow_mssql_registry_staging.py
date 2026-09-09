"""Stage deployment-owned connection registries into isolated pack build roots."""

from __future__ import annotations

from pathlib import Path


def stage_connection_registries(project_root: Path, build_root: Path) -> tuple[Path, ...]:
    """Copy env-bound connection registries into a temporary pack build root.

    Symlinked registry directories and escapes outside ``project_root`` are rejected.
    """

    root = project_root.resolve(strict=False)
    staged: list[Path] = []
    for relative in (
        Path(".dpone") / "registry" / "connection-registries",
        Path("platform") / "connection-registries",
    ):
        source_dir = project_root / relative
        if source_dir.is_symlink() or not source_dir.is_dir():
            continue
        destination_dir = build_root / relative
        destination_dir.mkdir(parents=True, exist_ok=True)
        for source in sorted(source_dir.glob("*.yaml")):
            if not source.is_file() or source.is_symlink():
                continue
            try:
                resolved = source.resolve(strict=True)
            except OSError:
                continue
            if not resolved.is_relative_to(root):
                continue
            destination = destination_dir / source.name
            destination.write_bytes(resolved.read_bytes())
            staged.append(destination)
    return tuple(staged)


__all__ = ["stage_connection_registries"]
