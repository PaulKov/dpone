"""Exact deployment-identity binding rules for promoted dbt evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity


def _deployment_identity_error(value: object) -> str:
    try:
        AirflowDeploymentIdentity.from_mapping(value)
    except ValueError as exc:
        return str(exc)
    return ""


def attempt_deployment_identity(
    payload: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Validate and return the exact identity shared by an attempt envelope and XCom."""

    expected = payload.get("deployment_identity")
    observed = summary.get("deployment_identity")
    if (
        not isinstance(expected, Mapping)
        or not isinstance(observed, Mapping)
        or _deployment_identity_error(expected)
        or _deployment_identity_error(observed)
        or dict(expected) != dict(observed)
    ):
        raise ValueError("deployment identity binding is invalid")
    return expected


def workflow_deployment_identity_is_valid(payload: Mapping[str, Any]) -> bool:
    """Return whether workflow identity matches its legacy or exact contract."""

    identity = payload.get("deployment_identity")
    schema = payload.get("schema")
    if schema in {
        "dpone.dbt-workflow-outcome.v1",
        "dpone.dbt-workflow-evidence-outcome.v1",
    }:
        return identity is None
    return bool(
        schema
        in {
            "dpone.dbt-workflow-outcome.v2",
            "dpone.dbt-workflow-evidence-outcome.v2",
        }
        and identity is not None
        and isinstance(identity, Mapping)
        and not _deployment_identity_error(identity)
        and identity.get("release_id") == payload.get("release_id")
        and identity.get("deployment_id") == payload.get("deployment_id")
    )
