"""Immutable interpreter and import roots for fixed Python child launchers.

Admission preserves virtual-environment symlinks and resolves explicit dependency
roots. It does not select an executable protocol, construct argv or admit a build.
Launchers retain their writable compatibility fields and revalidate at use.
"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AdmittedPythonInputs:
    """Validated filesystem inputs; operation-specific admission stays upstream."""

    python: Path
    package_root: Path
    dependency_paths: tuple[Path, ...] = ()

    def __post_init__(self) -> None:
        python = self.python.absolute()
        if not python.is_file() or not os.access(python, os.X_OK):
            raise ValueError("mssql_native.tds_interpreter_invalid")
        root = self.package_root.resolve(strict=True)
        dependencies = tuple(path.resolve(strict=True) for path in self.dependency_paths)
        if any(not path.is_dir() for path in dependencies):
            raise ValueError("mssql_native.tds_dependency_path_invalid")
        object.__setattr__(self, "python", python)
        object.__setattr__(self, "package_root", root)
        object.__setattr__(self, "dependency_paths", dependencies)
