"""Bind dbt execution to the same Python environment whose packages are inspected."""

from __future__ import annotations

import sys
from pathlib import Path


def current_environment_dbt_executable() -> str:
    """Return the dbt console script adjacent to the active Python executable."""

    return str(Path(sys.executable).with_name("dbt"))


__all__ = ["current_environment_dbt_executable"]
