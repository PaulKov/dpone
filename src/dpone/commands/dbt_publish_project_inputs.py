"""Confined dbt project input discovery for publishing commands."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from itertools import chain
from pathlib import Path, PurePosixPath

import yaml

_BUNDLE_ROOT_FILES = (
    "dbt_project.yml",
    "dependencies.yml",
    "package-lock.yml",
    "packages.yml",
    "selectors.yml",
)
_BUNDLE_SOURCE_ROOTS = {
    "analysis-paths": ("analyses",),
    "asset-paths": (),
    "docs-paths": (),
    "macro-paths": ("macros",),
    "model-paths": ("models",),
    "seed-paths": ("seeds",),
    "snapshot-paths": ("snapshots",),
    "test-paths": ("tests",),
}
_BUNDLE_EXCLUDED_DIRECTORIES = frozenset({".dpone-ci", ".git", ".pytest_cache", "__pycache__", "logs", "target"})


class DbtProjectRootError(ValueError):
    """Safe public dbt project configuration error."""

    code = "DPONE_DBT_PROJECT_INVALID"


def bundle_inputs(root: Path) -> Iterator[Path]:
    """Yield every confined build input for one dbt project."""

    seen: set[Path] = set()
    for name in _BUNDLE_ROOT_FILES:
        path = root / name
        if path.exists():
            seen.add(path.absolute())
            yield path
    for source_root in configured_source_roots(root):
        absolute_root = root / source_root
        if not absolute_root.exists() and not absolute_root.is_symlink():
            continue
        for path in chain((absolute_root,), absolute_root.rglob("*")):
            absolute = path.absolute()
            if absolute in seen:
                continue
            seen.add(absolute)
            relative = path.relative_to(root)
            if path.is_symlink() or not excluded_bundle_path(relative):
                yield path


def configured_source_roots(root: Path) -> tuple[PurePosixPath, ...]:
    """Load the closed configured dbt source-root set."""

    try:
        payload = yaml.safe_load((root / "dbt_project.yml").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise DbtProjectRootError("dbt_project.yml cannot be read safely") from exc
    if not isinstance(payload, Mapping):
        raise DbtProjectRootError("dbt_project.yml must contain a mapping")
    roots: list[PurePosixPath] = []
    for field, defaults in _BUNDLE_SOURCE_ROOTS.items():
        roots.extend(configured_paths(payload.get(field, defaults), field))
    roots.extend(
        configured_paths(
            payload.get("packages-install-path", "dbt_packages"),
            "packages-install-path",
        )
    )
    return tuple(sorted(set(roots), key=PurePosixPath.as_posix))


def configured_paths(value: object, field: str) -> tuple[PurePosixPath, ...]:
    """Parse one dbt relative-directory configuration value."""

    values: Sequence[object]
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        values = value
    else:
        raise DbtProjectRootError(f"{field} must contain relative directory paths")
    paths = []
    for raw in values:
        if not isinstance(raw, str):
            raise DbtProjectRootError(f"{field} must contain relative directory paths")
        path = PurePosixPath(raw)
        if (
            not raw
            or raw != raw.strip()
            or "\\" in raw
            or path.is_absolute()
            or path.as_posix() != raw
            or path == PurePosixPath(".")
            or any(part in {"", ".", ".."} for part in path.parts)
        ):
            raise DbtProjectRootError(f"{field} contains an unsafe directory path")
        paths.append(path)
    return tuple(paths)


def excluded_bundle_path(path: Path) -> bool:
    """Return whether a project path belongs to generated or private state."""

    return any(part.casefold() in _BUNDLE_EXCLUDED_DIRECTORIES for part in path.parts)
