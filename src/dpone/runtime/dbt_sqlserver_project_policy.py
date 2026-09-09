"""Runtime acquisition and enforcement of SQL Server dbt project policy."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from dpone.adapters.dbt_sqlserver_project_policy import (
    PROJECT_POLICY_INVALID_CODE,
    validate_sqlserver_project_policy,
)
from dpone.runtime.dbt_project_bundle_safety import (
    open_directory,
    read_root_bounded_yaml_mapping,
)

_PROJECT_FILE = "dbt_project.yml"


def validate_runtime_sqlserver_project_policy(
    project_dir: Path,
    *,
    expected_flags: Mapping[str, bool],
    maximum: int,
) -> tuple[str, str] | None:
    """Return an exact project-policy issue before credential or subprocess work."""

    root_descriptor = open_directory(
        project_dir,
        code=PROJECT_POLICY_INVALID_CODE,
    )
    try:
        project = read_root_bounded_yaml_mapping(
            root_descriptor,
            _PROJECT_FILE,
            maximum=maximum,
            error_code=PROJECT_POLICY_INVALID_CODE,
        )
    finally:
        os.close(root_descriptor)
    report = validate_sqlserver_project_policy(
        project,
        project_file=_PROJECT_FILE,
    )
    if report.issues:
        issue = report.issues[0]
        return issue.code, issue.message
    if dict(report.required_project_flags or {}) != dict(expected_flags):
        return (
            PROJECT_POLICY_INVALID_CODE,
            "dbt project flags differ from the execution-pack adapter policy",
        )
    return None


__all__ = ["validate_runtime_sqlserver_project_policy"]
