from __future__ import annotations

from typing import Any

from dpone.contracts.airflow_run_identity import airflow_deployment_identity_schema
from dpone.contracts.airflow_run_identity_schema import airflow_run_identity_schema

_DIGEST = {"type": "string", "pattern": r"^sha256:[0-9a-f]{64}$"}


def passed_airflow_xcom_summary_schema(*, exact_activation: bool) -> dict[str, Any]:
    """Return the closed passed-XCom shape embedded in immutable evidence."""

    required = [
        "kind",
        "schema_version",
        "producer",
        "status",
        "run_spec_path",
        "runtime_evidence_path",
        "run_identity",
        "blockers",
    ]
    if exact_activation:
        required.append("deployment_identity")
    properties: dict[str, Any] = {
        "kind": {"const": "gitops.airflow_xcom_summary"},
        "schema_version": {"const": "1"},
        "producer": _text_schema(),
        "status": {"const": "passed"},
        "runtime_profile_path": _text_schema(),
        "run_spec_path": _text_schema(),
        "runtime_evidence_path": _text_schema(),
        "runtime_evidence_sha256": _DIGEST,
        "runtime_evidence": {"type": "object"},
        "failed_step": {"type": ["string", "null"]},
        "step_counts": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
        "artifact_paths": {"type": "object", "additionalProperties": _text_schema()},
        "warnings": {"type": "array", "items": _issue_schema()},
        "interval": {"type": "object"},
        "backfill": {"type": "object"},
        "recovery": {"type": ["object", "null"]},
        "blockers": {"type": "array", "maxItems": 0, "items": _issue_schema()},
        "run_identity": airflow_run_identity_schema(),
        "deployment_identity": airflow_deployment_identity_schema(),
        "dbt_execution_evidence_ref": _dbt_evidence_ref_schema(),
    }
    return {
        "type": "object",
        "required": required,
        "additionalProperties": False,
        "properties": properties,
    }


def _issue_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["code", "message", "path", "source"],
        "additionalProperties": False,
        "properties": {
            "code": _text_schema(),
            "message": {"type": "string"},
            "path": {"type": "string"},
            "source": _text_schema(),
        },
    }


def _dbt_evidence_ref_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["schema", "workflow_id", "sha256", "bytes", "storage_scope"],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.dbt-execution-evidence-ref.v1"},
            "workflow_id": _text_schema(),
            "sha256": _DIGEST,
            "bytes": {"type": "integer", "minimum": 1, "maximum": 16 * 1024 * 1024},
            "storage_scope": {"const": "dbt_spool"},
        },
    }


def _text_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1, "maxLength": 2048}


__all__ = ["passed_airflow_xcom_summary_schema"]
