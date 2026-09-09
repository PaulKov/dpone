"""Fail-closed public release-set validation before deployment projection."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.gitops.release_set_validation import release_set_validation_failure
from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)


def validate_release_set_schema(
    release: Mapping[str, Any],
    *,
    path: Path,
) -> None:
    """Validate the exact public schema before compatibility projections."""

    failure = release_set_validation_failure(release)
    if failure is not None:
        raise AirflowDeploymentProjectionError(
            failure.code,
            failure.message,
            path=path.as_posix(),
        )


__all__ = ["validate_release_set_schema"]
