from __future__ import annotations

from typing import Any

from dpone.contracts.airflow_run_identity_schema import airflow_run_identity_schema
from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    documented_contract,
)

_DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"


def airflow_run_identity_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-run-identity",
        kind="dpone.airflow-run-identity.v1",
        title="dpone GitOps Airflow run identity contract",
        required=tuple(airflow_run_identity_schema()["required"]),
        properties=dict(airflow_run_identity_schema()["properties"]),
        additional_properties=False,
    )


def airflow_rerun_plan_contract() -> GitOpsSchemaContract:
    contract = documented_contract(
        name="airflow-rerun-plan",
        kind="dpone.airflow-rerun-plan.v1",
        title="dpone GitOps Airflow reproducible rerun plan contract",
        required=(
            "schema",
            "status",
            "critical",
            "source_attempt",
            "selection",
            "resolved",
            "airflow_request",
            "retention_refs",
            "warnings",
            "blockers",
        ),
        properties={
            "schema": {"const": "dpone.airflow-rerun-plan.v1"},
            "status": {"enum": ["ready", "blocked"]},
            "critical": {"type": "boolean"},
            "source_attempt": _source_attempt_schema(),
            "selection": _selection_schema(),
            "resolved": {"anyOf": [airflow_run_identity_schema(), {"type": "null"}]},
            "airflow_request": {"anyOf": [_airflow_request_schema(), {"type": "null"}]},
            "retention_refs": _retention_refs_schema(min_items=0),
            "warnings": {"type": "array", "items": _rerun_issue_schema()},
            "blockers": {"type": "array", "items": _rerun_issue_schema()},
        },
        additional_properties=False,
    )
    contract.schema["allOf"] = [
        {
            "if": {"properties": {"status": {"const": "ready"}}},
            "then": {
                "properties": {
                    "resolved": airflow_run_identity_schema(),
                    "airflow_request": _airflow_request_schema(),
                    "retention_refs": _retention_refs_schema(min_items=1),
                }
            },
        }
    ]
    return contract


def _selection_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["bundle", "artifacts"],
        "additionalProperties": False,
        "properties": {
            "bundle": {"enum": ["original", "latest"]},
            "artifacts": {"enum": ["original", "latest"]},
        },
    }


def _source_attempt_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "dag_id": {"type": "string"},
            "task_id": {"type": "string"},
            "run_id": {"type": "string"},
        },
    }


def _airflow_request_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["run_on_latest_version", "execution_mode"],
        "additionalProperties": False,
        "properties": {
            "run_on_latest_version": {"type": "boolean"},
            "execution_mode": {"enum": ["clear_existing_run", "create_pinned_rerun"]},
        },
    }


def _retention_refs_schema(*, min_items: int) -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["release_ids", "deployment_ids"],
        "additionalProperties": False,
        "properties": {
            "release_ids": {"type": "array", "items": _digest_schema(), "minItems": min_items},
            "deployment_ids": {"type": "array", "items": _digest_schema(), "minItems": min_items},
        },
    }


def _rerun_issue_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["schema", "code", "stage", "severity", "message", "path"],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.error.v1"},
            "code": {"type": "string", "minLength": 1},
            "stage": {"const": "airflow_rerun_plan"},
            "severity": {"enum": ["warning", "error"]},
            "message": {"type": "string", "minLength": 1},
            "path": {"type": "string", "minLength": 1},
        },
    }


def _digest_schema() -> dict[str, str]:
    return {"type": "string", "pattern": _DIGEST_PATTERN}


__all__ = [
    "airflow_rerun_plan_contract",
    "airflow_run_identity_contract",
    "airflow_run_identity_schema",
]
