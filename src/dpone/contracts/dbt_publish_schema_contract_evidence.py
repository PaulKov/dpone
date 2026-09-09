"""Published campaign and provider evidence schemas for dbt self-service."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from dpone.contracts.airflow_run_identity import airflow_deployment_identity_schema
from dpone.contracts.dbt_dev_evidence_campaign import (
    MAX_DBT_DEV_EVIDENCE_BUNDLE_FILES,
    MAX_DBT_DEV_EVIDENCE_WORKFLOWS,
)
from dpone.contracts.dbt_publish_schema_contract_airflow_evidence import (
    airflow_evidence_schema_contracts,
)
from dpone.contracts.dbt_publish_schema_contract_common import (
    DIGEST,
    TOKEN,
    nonempty_tokens,
    object_schema,
)
from dpone.contracts.dbt_publish_schema_contract_evidence_reports import (
    evidence_report_schema_contracts,
)

_SAFE_TEXT = {
    "type": "string",
    "minLength": 1,
    "maxLength": 2048,
    "pattern": "^[^\\u0000-\\u001f\\u007f]+$",
}


def evidence_schema_contracts() -> dict[str, dict[str, Any]]:
    """Return versioned request, runtime and verification evidence schemas."""

    return {
        "dpone.dbt-dev-evidence-request.v1": _campaign_request(),
        "dpone.dbt-dev-evidence-authority.v1": _campaign_authority(),
        "dpone.dbt-execution-evidence-ref.v1": _execution_evidence_ref(),
        **airflow_evidence_schema_contracts(
            digest=DIGEST,
            token=TOKEN,
            object_schema=object_schema,
            deployment_identity_schema=airflow_deployment_identity_schema,
        ),
        "dpone.dbt-dev-evidence-campaign.v1": _campaign_report(),
        "dpone.dbt-dev-evidence-campaign-outcome.v1": _campaign_outcome(),
        "dpone.dbt-dev-evidence-export-report.v1": _export_report(),
        **evidence_report_schema_contracts(
            digest=DIGEST,
            token=TOKEN,
            nonempty_tokens=nonempty_tokens,
            object_schema=object_schema,
            max_bundle_files=MAX_DBT_DEV_EVIDENCE_BUNDLE_FILES,
        ),
    }


def _campaign_request() -> dict[str, Any]:
    workflow = object_schema(
        ("workflow_id", "dag_id", "dag_run_id"),
        {
            "workflow_id": TOKEN,
            "dag_id": TOKEN,
            "dag_run_id": TOKEN,
        },
    )
    return object_schema(
        (
            "schema",
            "evidence_set_id",
            "release_id",
            "deployment_id",
            "producer_repository",
            "producer_workflow",
            "source_commit",
            "orchestration_run_id",
            "orchestration_run_attempt",
            "workflows",
        ),
        {
            "schema": {"const": "dpone.dbt-dev-evidence-request.v1"},
            "evidence_set_id": DIGEST,
            "release_id": DIGEST,
            "deployment_id": DIGEST,
            "producer_repository": deepcopy(_SAFE_TEXT),
            "producer_workflow": deepcopy(_SAFE_TEXT),
            "source_commit": {
                "type": "string",
                "pattern": "^[0-9a-f]{40}$",
            },
            "orchestration_run_id": deepcopy(_SAFE_TEXT),
            "orchestration_run_attempt": {
                "type": "integer",
                "minimum": 1,
                "maximum": 1000,
            },
            "workflows": {
                "type": "array",
                "minItems": 1,
                "maxItems": MAX_DBT_DEV_EVIDENCE_WORKFLOWS,
                "uniqueItems": True,
                "items": workflow,
            },
        },
    )


def _campaign_authority() -> dict[str, Any]:
    return object_schema(
        (
            "schema",
            "request_id",
            "evidence_set_id",
            "release_id",
            "deployment_id",
            "workflow_id",
            "dag_id",
            "dag_run_id",
        ),
        {
            "schema": {"const": "dpone.dbt-dev-evidence-authority.v1"},
            "request_id": DIGEST,
            "evidence_set_id": DIGEST,
            "release_id": DIGEST,
            "deployment_id": DIGEST,
            "workflow_id": TOKEN,
            "dag_id": TOKEN,
            "dag_run_id": TOKEN,
        },
    )


def _execution_evidence_ref() -> dict[str, Any]:
    return object_schema(
        ("schema", "workflow_id", "sha256", "bytes", "storage_scope"),
        {
            "schema": {"const": "dpone.dbt-execution-evidence-ref.v1"},
            "workflow_id": TOKEN,
            "sha256": DIGEST,
            "bytes": {
                "type": "integer",
                "minimum": 1,
                "maximum": 16 * 1024 * 1024,
            },
            "storage_scope": {"const": "dbt_spool"},
        },
    )


def _campaign_report() -> dict[str, Any]:
    schema = _campaign_result("dpone.dbt-dev-evidence-campaign.v1")
    schema["properties"]["elapsed_seconds"] = {
        "type": "number",
        "minimum": 0,
    }
    schema["required"].append("elapsed_seconds")
    return schema


def _campaign_outcome() -> dict[str, Any]:
    schema = _campaign_result("dpone.dbt-dev-evidence-campaign-outcome.v1")
    schema["properties"]["closed"] = {"const": True}
    schema["properties"]["campaign_request_sha256"] = DIGEST
    schema["required"].extend(("closed", "campaign_request_sha256"))
    return schema


def _campaign_result(schema_id: str) -> dict[str, Any]:
    workflow = object_schema(
        ("workflow_id", "state"),
        {
            "workflow_id": TOKEN,
            "state": {
                "enum": [
                    "queued",
                    "running",
                    "success",
                    "failed",
                    "unknown",
                ]
            },
        },
    )
    schema = object_schema(
        (
            "schema",
            "status",
            "passed",
            "code",
            "evidence_set_id",
            "release_id",
            "deployment_id",
            "workflow_states",
        ),
        {
            "schema": {"const": schema_id},
            "status": {"enum": ["passed", "failed", "abandoned"]},
            "passed": {"type": "boolean"},
            "code": TOKEN,
            "evidence_set_id": DIGEST,
            "release_id": DIGEST,
            "deployment_id": DIGEST,
            "workflow_states": {
                "type": "array",
                "minItems": 1,
                "uniqueItems": True,
                "items": workflow,
            },
        },
    )
    schema["oneOf"] = [
        {
            "properties": {
                "status": {"const": "passed"},
                "passed": {"const": True},
                "code": {"const": "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_PASSED"},
                "workflow_states": {
                    "items": {
                        "properties": {"state": {"const": "success"}},
                        "required": ["state"],
                    }
                },
            },
            "required": [
                "status",
                "passed",
                "code",
                "workflow_states",
            ],
        },
        {
            "properties": {
                "status": {"const": "failed"},
                "passed": {"const": False},
                "code": {"const": "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED"},
                "workflow_states": {
                    "contains": {
                        "properties": {"state": {"const": "failed"}},
                        "required": ["state"],
                    },
                    "minContains": 1,
                },
            },
            "required": [
                "status",
                "passed",
                "code",
                "workflow_states",
            ],
        },
        {
            "properties": {
                "status": {"const": "abandoned"},
                "passed": {"const": False},
                "code": {"const": "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED"},
                "workflow_states": {
                    "contains": {
                        "properties": {
                            "state": {
                                "enum": [
                                    "queued",
                                    "running",
                                    "unknown",
                                ]
                            }
                        },
                        "required": ["state"],
                    },
                    "minContains": 1,
                },
            },
            "required": [
                "status",
                "passed",
                "code",
                "workflow_states",
            ],
        },
    ]
    return schema


def _export_report() -> dict[str, Any]:
    return {
        "oneOf": [
            object_schema(
                ("schema", "status"),
                {
                    "schema": {"const": "dpone.dbt-dev-evidence-export-report.v1"},
                    "status": {"const": "not_requested"},
                },
            ),
            object_schema(
                (
                    "schema",
                    "status",
                    "release_id",
                    "deployment_id",
                    "evidence_set_id",
                    "workflow_id",
                    "evidence_root",
                    "file_count",
                    "no_op",
                ),
                {
                    "schema": {"const": "dpone.dbt-dev-evidence-export-report.v1"},
                    "status": {"const": "exported"},
                    "release_id": DIGEST,
                    "deployment_id": DIGEST,
                    "evidence_set_id": DIGEST,
                    "workflow_id": TOKEN,
                    "evidence_root": deepcopy(_SAFE_TEXT),
                    "file_count": {
                        "type": "integer",
                        "minimum": 3,
                    },
                    "no_op": {"type": "boolean"},
                },
            ),
        ]
    }


__all__ = ["evidence_schema_contracts"]
