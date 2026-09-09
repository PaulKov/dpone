"""Shared confinement for dbt audit installation and read-only verification."""

import stat
from pathlib import Path, PurePosixPath

from dpone.contracts.dbt_promotion import DbtProdMirrorError


def confined_mirror_destination(root: Path, relative: PurePosixPath) -> Path:
    """Validate every existing output component without following symlinks."""

    candidate = root.joinpath(*relative.parts)
    try:
        candidate.resolve(strict=False).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise DbtProdMirrorError("prod promotion output escapes the repository") from exc
    current = root
    for part in relative.parts:
        current /= part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode) or (current != candidate and not stat.S_ISDIR(metadata.st_mode)):
            raise DbtProdMirrorError("prod promotion destination component is unsafe")
    return candidate


__all__ = ["confined_mirror_destination"]
