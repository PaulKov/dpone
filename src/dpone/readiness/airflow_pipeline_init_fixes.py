"""Safe executable fixes for self-service pipeline initialization."""

from __future__ import annotations

import shlex
from pathlib import Path


def pipeline_id_fix_command(
    *,
    suggested_id: str,
    recipe: str | None,
    route: str | None = None,
    airflow: bool | None,
    authoring_mode: str,
    profile: str | None,
    answers: str | Path | None,
) -> str:
    """Preserve every authoring choice while replacing only an invalid ID."""

    command = ["dpone", "init", "pipeline", suggested_id]
    if route is not None:
        command.extend(("--route", route))
    elif recipe is not None:
        command.extend(("--recipe", recipe))
    command.extend(("--authoring", authoring_mode))
    if airflow is True:
        command.append("--airflow")
    elif airflow is False:
        command.append("--no-airflow")
    if profile is not None:
        command.extend(("--profile", profile))
    if answers is not None:
        command.extend(("--answers", str(answers)))
    return shlex.join(command)


__all__ = ["pipeline_id_fix_command"]
