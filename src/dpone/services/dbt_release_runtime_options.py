"""Service-side profile path resolution and compatible pure option imports."""

from __future__ import annotations

from pathlib import Path

from dpone.contracts.dbt_project_artifacts import dbt_warning_policy as dbt_warning_policy
from dpone.contracts.dbt_project_artifacts import positive_int as positive_int


def profiles_dir(value: Path | None, project_root: Path) -> Path | None:
    """Resolve an optional dbt profiles directory against its project."""

    if value is None:
        return None
    path = Path(value)
    return path if path.is_absolute() else (Path(project_root) / path).absolute()
