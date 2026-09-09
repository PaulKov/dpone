"""Schemas for bounded Airflow cache-status publication evidence."""

from __future__ import annotations

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract
from dpone.gitops.schema_deployment_cache_primitives import (
    non_empty_string_schema,
    sha256_schema,
    status_file_name_schema,
)


def airflow_cache_status_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        airflow_cache_status_publication_contract(),
        airflow_cache_status_publication_failure_contract(),
    )


def airflow_cache_status_publication_contract() -> GitOpsSchemaContract:
    contract = documented_contract(
        name="airflow-cache-status-publication",
        kind="dpone.airflow-cache-status-publication.v1",
        title="dpone GitOps Airflow cache status publication report",
        required=(
            "schema",
            "passed",
            "status",
            "source",
            "target",
            "failure_marker",
            "expected_schema",
            "attempted_at",
        ),
        properties={
            "schema": {"const": "dpone.airflow-cache-status-publication.v1"},
            "passed": {"type": "boolean"},
            "status": {"enum": ["published", "published_with_warning", "rejected", "commit_unknown"]},
            "source": status_file_name_schema(),
            "target": status_file_name_schema(),
            "failure_marker": status_file_name_schema(),
            "expected_schema": non_empty_string_schema(),
            "attempted_at": non_empty_string_schema(),
            "source_sha256": sha256_schema(),
            "target_sha256": sha256_schema(),
            "error_code": {"type": "string", "pattern": "^DPONE_AIRFLOW_CACHE_STATUS_[A-Z0-9_]+$"},
            "diagnostic_error_code": {
                "type": "string",
                "pattern": "^DPONE_AIRFLOW_CACHE_STATUS_[A-Z0-9_]+$",
            },
        },
        additional_properties=False,
    )
    contract.schema["allOf"] = [
        {
            "if": {"properties": {"status": {"const": "published"}}, "required": ["status"]},
            "then": {
                "properties": {"passed": {"const": True}},
                "required": ["source_sha256", "target_sha256"],
            },
        },
        {
            "if": {"properties": {"status": {"const": "published_with_warning"}}, "required": ["status"]},
            "then": {
                "properties": {"passed": {"const": True}},
                "required": ["source_sha256", "target_sha256", "error_code"],
            },
        },
        {
            "if": {"properties": {"status": {"const": "rejected"}}, "required": ["status"]},
            "then": {"properties": {"passed": {"const": False}}, "required": ["error_code"]},
        },
        {
            "if": {"properties": {"status": {"const": "commit_unknown"}}, "required": ["status"]},
            "then": {
                "properties": {"passed": {"const": False}},
                "required": ["source_sha256", "target_sha256", "error_code"],
            },
        },
    ]
    return contract


def airflow_cache_status_publication_failure_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-cache-status-publication-failure",
        kind="dpone.airflow-cache-status-publication-failure.v1",
        title="dpone GitOps Airflow cache status publication failure marker",
        required=("schema", "status", "source", "target", "expected_schema", "attempted_at", "error_code"),
        properties={
            "schema": {"const": "dpone.airflow-cache-status-publication-failure.v1"},
            "status": {"enum": ["rejected", "commit_unknown"]},
            "source": status_file_name_schema(),
            "target": status_file_name_schema(),
            "expected_schema": non_empty_string_schema(),
            "attempted_at": non_empty_string_schema(),
            "error_code": {"type": "string", "pattern": "^DPONE_AIRFLOW_CACHE_STATUS_[A-Z0-9_]+$"},
        },
        additional_properties=False,
    )


__all__ = [
    "airflow_cache_status_schema_contracts",
    "airflow_cache_status_publication_contract",
    "airflow_cache_status_publication_failure_contract",
]
