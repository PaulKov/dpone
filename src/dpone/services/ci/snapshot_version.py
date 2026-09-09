from __future__ import annotations

import re
from pathlib import Path

from dpone._compat import tomllib

_RELEASE_BASE_RE = re.compile(r"^(\d+\.\d+\.\d+)")


def read_project_version(pyproject_path: str | Path = "pyproject.toml") -> str:
    path = Path(pyproject_path)
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return str(data.get("project", {}).get("version", "0.0.0"))


def _release_base(version: str) -> str:
    match = _RELEASE_BASE_RE.match(version.strip())
    if match:
        return match.group(1)
    return "0.0.0"


def build_snapshot_version(base_version: str, pipeline_iid: str | int) -> str:
    iid = str(int(str(pipeline_iid)))
    return f"{_release_base(base_version)}.dev{iid}"


def build_snapshot_version_from_pyproject(
    pyproject_path: str | Path = "pyproject.toml",
    *,
    pipeline_iid: str | int,
) -> str:
    return build_snapshot_version(read_project_version(pyproject_path), pipeline_iid)
