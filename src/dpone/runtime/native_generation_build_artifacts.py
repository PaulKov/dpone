"""Fixed build-artifact reads under the bootstrap's captured output identity.

The caller authenticates the command and OUTPUT root before construction and
holds its isolation/lifetime through the build. This reader never turns a path
or a decoded root record into authority. It restricts subsequent reads to the
two fixed build files and the captured byte allowance.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path

from dpone.contracts.native_trusted_dbt_environment import TrustedDbtOwnedRoot
from dpone.manifest.confined_files import read_confined_file


@dataclass(frozen=True, slots=True)
class _CapturedOutputIdentity:
    path: Path
    device: int
    inode: int

    def matches(self, metadata: os.stat_result) -> bool:
        return stat.S_ISDIR(metadata.st_mode) and (metadata.st_dev, metadata.st_ino) == (self.device, self.inode)


class CapturedBuildArtifactReader:
    """ConfinedReleaseFileReader-compatible capability for one admitted target.

    `target` must come from the already authenticated BUILD argv target-path
    slot, never from a later read request. No filesystem mutation occurs here.
    """

    def __init__(self, *, root: Path, target: Path, identity: TrustedDbtOwnedRoot, max_bytes: int) -> None:
        if type(identity) is not TrustedDbtOwnedRoot:
            raise ValueError("build artifacts require an exact authenticated output identity")
        identity.__post_init__()
        if identity.role != "OUTPUT":
            raise ValueError("build artifact root must have OUTPUT role")
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("captured artifact byte allowance must be an exact positive integer")
        if (
            not isinstance(root, Path)
            or not isinstance(target, Path)
            or not root.is_absolute()
            or not target.is_absolute()
            or ".." in root.parts
            or ".." in target.parts
        ):
            raise ValueError("captured build paths must be absolute without traversal")
        relative = target.relative_to(root)
        if relative == Path("."):
            raise ValueError("build target must be below the captured output root")
        self._identity = _CapturedOutputIdentity(root, identity.device, identity.inode)
        self._paths = frozenset((relative / name).as_posix() for name in ("manifest.json", "run_results.json"))
        self._maximum = max_bytes

    def __call__(self, root: Path, relative_path: str, *, max_bytes: int) -> bytes:
        """Read only an admitted file, rejecting root substitution or larger bounds."""
        if root != self._identity.path or type(relative_path) is not str or relative_path not in self._paths:
            raise ValueError("artifact read differs from the captured build paths")
        if type(max_bytes) is not int or not 1 <= max_bytes <= self._maximum:
            raise ValueError("artifact read exceeds its captured byte allowance")
        return read_confined_file(root, relative_path, max_bytes=max_bytes, root_identity=self._identity)
