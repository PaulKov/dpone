"""Shared JSON Schema fragments for Airflow desired-state contracts."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract

DIGEST = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
REVISION = {"type": "string", "minLength": 1, "maxLength": 1024}
TIMESTAMP = {
    "type": "string",
    "format": "date-time",
    "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?Z$",
    "maxLength": 40,
}


def source_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "project",
            "ref",
            "pipeline_id",
            "job_id",
            "occurrence_id",
            "git_sha",
        ],
        "additionalProperties": False,
        "properties": {
            "project": project_schema(),
            "ref": bounded_text_schema(256),
            "pipeline_id": positive_integer_text_schema(),
            "job_id": positive_integer_text_schema(),
            "occurrence_id": uuid_schema(),
            "git_sha": {
                "type": "string",
                "pattern": "^(?:[0-9a-f]{40}|[0-9a-f]{64})$",
            },
        },
    }


def publish_candidate_schema() -> dict[str, Any]:
    properties = {
        "environment": safe_name_schema(),
        "authority_sha256": DIGEST,
        "project": project_schema(),
        "source_ref": bounded_text_schema(256),
        "pipeline_id": positive_integer_text_schema(),
        "job_id": positive_integer_text_schema(),
        "git_sha": {
            "type": "string",
            "pattern": "^(?:[0-9a-f]{40}|[0-9a-f]{64})$",
        },
        "registry_scope_id": DIGEST,
        "release_id": DIGEST,
        "deployment_id": DIGEST,
        "airflow_index_sha256": DIGEST,
        "runtime_image_digest": DIGEST,
        "runtime_image_dbt_digest": DIGEST,
        "expected_dag_ids": expected_dag_ids_schema(),
        "publication_evidence_sha256": DIGEST,
        "expected_revision": nullable_revision(),
    }
    required = [key for key in properties if key != "runtime_image_dbt_digest"]
    return {
        "type": "object",
        "required": required,
        "properties": properties,
        "additionalProperties": False,
    }


def publish_intent_schema() -> dict[str, Any]:
    properties = {
        "schema": {"const": "dpone.airflow-desired-state-publish-intent.v1"},
        "candidate_sha256": DIGEST,
        "occurrence_id": uuid_schema(),
        "promoted_at": TIMESTAMP,
    }
    return {
        "type": "object",
        "required": list(properties),
        "properties": properties,
        "additionalProperties": False,
    }


def publish_evidence_v2_properties() -> dict[str, Any]:
    return {
        "schema": {"const": "dpone.airflow-desired-state-publish.v2"},
        "passed": {"const": True},
        "status": {"const": "published"},
        "outcome": {"enum": ["created", "replaced", "idempotent"]},
        "environment": safe_name_schema(),
        "occurrence_id": uuid_schema(),
        "preparation_job_id": positive_integer_text_schema(),
        "publisher_job_id": positive_integer_text_schema(),
        "release_id": DIGEST,
        "deployment_id": DIGEST,
        "desired_state_sha256": DIGEST,
        "previous_revision": nullable_revision(),
        "committed_revision": REVISION,
        "published_at": TIMESTAMP,
        "state_may_have_changed": {"const": False},
    }


def bounded_text_schema(maximum: int) -> dict[str, Any]:
    return {"type": "string", "minLength": 1, "maxLength": maximum}


def project_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "pattern": (
            "^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?(?:/[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?)+$"
        ),
        "maxLength": 512,
    }


def promotion_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "registry_scope_id",
            "release_id",
            "deployment_id",
            "airflow_index_sha256",
            "runtime_image_digest",
            "expected_dag_ids",
            "publication_evidence_sha256",
        ],
        "additionalProperties": False,
        "properties": {
            "registry_scope_id": DIGEST,
            "release_id": DIGEST,
            "deployment_id": DIGEST,
            "airflow_index_sha256": DIGEST,
            "runtime_image_digest": DIGEST,
            "runtime_image_dbt_digest": DIGEST,
            "expected_dag_ids": expected_dag_ids_schema(),
            "publication_evidence_sha256": DIGEST,
        },
    }


def previous_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["revision", "deployment_id"],
        "additionalProperties": False,
        "properties": {
            "revision": nullable_revision(),
            "deployment_id": {"anyOf": [DIGEST, {"type": "null"}]},
        },
        "oneOf": [
            {
                "properties": {
                    "revision": {"type": "null"},
                    "deployment_id": {"type": "null"},
                }
            },
            {
                "properties": {
                    "revision": REVISION,
                    "deployment_id": DIGEST,
                }
            },
        ],
    }


def safe_name_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "pattern": "^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$",
        "not": {"enum": ["current", "latest"]},
    }


def expected_dag_ids_schema() -> dict[str, Any]:
    return {
        "type": "array",
        "minItems": 1,
        "maxItems": 5000,
        "uniqueItems": True,
        "items": {
            "type": "string",
            "pattern": "^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,248}[A-Za-z0-9])?$",
        },
    }


def positive_integer_text_schema() -> dict[str, Any]:
    return {"type": "string", "pattern": "^[0-9]{1,32}$"}


def nullable_revision() -> dict[str, Any]:
    return {"anyOf": [REVISION, {"type": "null"}]}


def uuid_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    }


def authority_contract() -> GitOpsSchemaContract:
    """Return the legacy authority contract retained for wire compatibility."""

    properties = _authority_properties()
    properties["schema"] = {"const": "dpone.airflow-desired-state-authority.v1"}
    return documented_contract(
        name="airflow-desired-state-authority",
        kind="dpone.airflow-desired-state-authority.v1",
        title="dpone GitOps Airflow desired-state trusted authority",
        required=tuple(properties),
        properties=properties,
        additional_properties=False,
    )


def authority_v2_contract() -> GitOpsSchemaContract:
    """Return the authority contract with sealed workspace control binding."""

    properties = _authority_properties()
    properties.update(
        {
            "schema": {"const": "dpone.airflow-desired-state-authority.v2"},
            "workspace_authority_connection_ref": {
                **bounded_text_schema(128),
                "pattern": "^[a-z][a-z0-9_]{0,127}$",
            },
        }
    )
    return documented_contract(
        name="airflow-desired-state-authority-v2",
        kind="dpone.airflow-desired-state-authority.v2",
        title="dpone GitOps Airflow desired-state workspace activation authority",
        required=tuple(properties),
        properties=properties,
        additional_properties=False,
    )


def _authority_properties() -> dict[str, object]:
    return {
        "schema": {},
        "environment": safe_name_schema(),
        "desired_state_uri": {**bounded_text_schema(4096), "pattern": "^s3://[^/?#]+/[^?#]+$"},
        "certified_s3_endpoint_url": {
            **bounded_text_schema(512),
            "pattern": "^https://[^/?#]+(?::[0-9]{1,5})?$",
        },
        "artifact_registry_uri": {
            **bounded_text_schema(4096),
            "pattern": "^s3://[^/?#]+/[^?#]+$",
        },
        "artifact_registry_ref": bounded_text_schema(256),
        "watcher_identity": bounded_text_schema(256),
        "source_project": project_schema(),
        "source_ref": bounded_text_schema(256),
    }


__all__ = [
    "DIGEST",
    "REVISION",
    "TIMESTAMP",
    "bounded_text_schema",
    "authority_contract",
    "authority_v2_contract",
    "expected_dag_ids_schema",
    "nullable_revision",
    "positive_integer_text_schema",
    "previous_schema",
    "project_schema",
    "promotion_schema",
    "safe_name_schema",
    "source_schema",
    "uuid_schema",
]
