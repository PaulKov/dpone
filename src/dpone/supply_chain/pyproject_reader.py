"""Project metadata reader for SBOM generation."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ProjectDependency:
    name: str
    requirement: str
    scope: str


@dataclass(frozen=True, slots=True)
class ProjectMetadata:
    name: str
    version: str
    dependencies: tuple[ProjectDependency, ...]


class PyprojectMetadataReader:
    """Read package metadata from ``pyproject.toml`` without build imports."""

    def read(self, project_root: str | Path) -> ProjectMetadata:
        path = Path(project_root) / "pyproject.toml"
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
        project = payload.get("project", {})
        deps: list[ProjectDependency] = []
        for requirement in project.get("dependencies", []):
            deps.append(self._dependency(str(requirement), "runtime"))
        optional = project.get("optional-dependencies", {})
        if isinstance(optional, dict):
            for extra, requirements in optional.items():
                for requirement in requirements or []:
                    deps.append(self._dependency(str(requirement), f"optional:{extra}"))
        return ProjectMetadata(
            name=str(project.get("name", "unknown")),
            version=str(project.get("version", "0.0.0")),
            dependencies=tuple(deps),
        )

    @staticmethod
    def _dependency(requirement: str, scope: str) -> ProjectDependency:
        name = requirement
        for separator in ("[", "==", ">=", "<=", "~=", "!=", ">", "<", ";"):
            if separator in name:
                name = name.split(separator, 1)[0]
        return ProjectDependency(name=name.strip(), requirement=requirement, scope=scope)
