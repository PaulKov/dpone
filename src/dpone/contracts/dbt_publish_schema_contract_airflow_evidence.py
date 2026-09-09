"""Versioned Airflow evidence schemas for dbt self-service publishing."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone.contracts.airflow_run_identity_schema import airflow_run_identity_schema
from dpone.contracts.airflow_xcom_summary_schema import passed_airflow_xcom_summary_schema

ObjectSchemaFactory = Callable[
    [tuple[str, ...], dict[str, Any]],
    dict[str, Any],
]


def airflow_evidence_schema_contracts(
    *,
    digest: Mapping[str, Any],
    token: Mapping[str, Any],
    object_schema: ObjectSchemaFactory,
    deployment_identity_schema: Callable[[], dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Return immutable legacy and exact deployment-bound evidence schemas."""

    return {
        "dpone.dbt-airflow-attempt-evidence.v1": _attempt_evidence(
            schema_name="dpone.dbt-airflow-attempt-evidence.v1",
            exact=False,
            digest=digest,
            token=token,
            object_schema=object_schema,
            deployment_identity_schema=deployment_identity_schema,
        ),
        "dpone.dbt-airflow-attempt-evidence.v2": _attempt_evidence(
            schema_name="dpone.dbt-airflow-attempt-evidence.v2",
            exact=True,
            digest=digest,
            token=token,
            object_schema=object_schema,
            deployment_identity_schema=deployment_identity_schema,
        ),
        "dpone.dbt-workflow-evidence-outcome.v1": _workflow_outcome(
            schema_name="dpone.dbt-workflow-evidence-outcome.v1",
            exact=False,
            digest=digest,
            token=token,
            object_schema=object_schema,
            deployment_identity_schema=deployment_identity_schema,
        ),
        "dpone.dbt-workflow-evidence-outcome.v2": _workflow_outcome(
            schema_name="dpone.dbt-workflow-evidence-outcome.v2",
            exact=True,
            digest=digest,
            token=token,
            object_schema=object_schema,
            deployment_identity_schema=deployment_identity_schema,
        ),
    }


def _attempt_evidence(
    *,
    schema_name: str,
    exact: bool,
    digest: Mapping[str, Any],
    token: Mapping[str, Any],
    object_schema: ObjectSchemaFactory,
    deployment_identity_schema: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    required = [
        "schema",
        "status",
        "evidence_set_id",
        "run_identity",
        "attempt",
        "xcom_summary_sha256",
        "xcom_summary",
    ]
    properties: dict[str, Any] = {
        "schema": {"const": schema_name},
        "status": {"const": "passed"},
        "evidence_set_id": digest,
        "run_identity": airflow_run_identity_schema() if exact else {"type": "object"},
        "attempt": _attempt(token=token, object_schema=object_schema),
        "xcom_summary_sha256": digest,
        "xcom_summary": passed_airflow_xcom_summary_schema(exact_activation=True) if exact else {"type": "object"},
    }
    if exact:
        required.append("deployment_identity")
        properties["deployment_identity"] = deployment_identity_schema()
    schema = object_schema(tuple(required), properties)
    if exact:
        schema["x-dpone-semantic-validator"] = (
            "dpone.services.dbt_dev_airflow_evidence_contracts.validate_dbt_airflow_attempt_evidence"
        )
    return schema


def _attempt(
    *,
    token: Mapping[str, Any],
    object_schema: ObjectSchemaFactory,
) -> dict[str, Any]:
    return object_schema(
        ("dag_id", "task_id", "run_id", "try_number", "map_index"),
        {
            "dag_id": token,
            "task_id": token,
            "run_id": token,
            "try_number": {"type": "integer", "minimum": 1},
            "map_index": {"type": "integer", "minimum": -1},
        },
    )


def _workflow_outcome(
    *,
    schema_name: str,
    exact: bool,
    digest: Mapping[str, Any],
    token: Mapping[str, Any],
    object_schema: ObjectSchemaFactory,
    deployment_identity_schema: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    task = {
        "type": "object",
        "required": ["task_id", "state"],
        "additionalProperties": False,
        "properties": {
            "task_id": token,
            "state": {"const": "success"},
            "mapped_instances": {"type": "integer", "minimum": 1},
        },
    }
    artifact = object_schema(
        ("category", "logical_id", "sha256", "bytes"),
        {
            "category": {"enum": ["airflow", "dbt"]},
            "logical_id": token,
            "sha256": digest,
            "bytes": {"type": "integer", "minimum": 1},
        },
    )
    required = [
        "schema",
        "status",
        "code",
        "workflow_id",
        "release_id",
        "deployment_id",
        "dag_run_id",
        "tasks",
        "evidence_set_id",
        "artifacts",
    ]
    properties: dict[str, Any] = {
        "schema": {"const": schema_name},
        "status": {"const": "passed"},
        "code": {"const": "DPONE_DBT_WORKFLOW_PASSED"},
        "workflow_id": token,
        "release_id": digest,
        "deployment_id": digest,
        "dag_run_id": token,
        "tasks": {"type": "array", "minItems": 1, "items": task},
        "evidence_set_id": digest,
        "artifacts": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": artifact,
        },
    }
    if exact:
        required.append("deployment_identity")
        properties["deployment_identity"] = deployment_identity_schema()
    schema = object_schema(tuple(required), properties)
    if exact:
        schema["x-dpone-semantic-validator"] = (
            "dpone.services.dbt_dev_airflow_evidence_contracts.validate_workflow_outcome_contract"
        )
    return schema


__all__ = ["airflow_evidence_schema_contracts"]
