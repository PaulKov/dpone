"""Public schemas for exact Airflow desired-state selection."""

from __future__ import annotations

from dpone.gitops import schema_airflow_desired_state_primitives as desired_schema


def airflow_desired_state_schema_contracts() -> tuple[desired_schema.GitOpsSchemaContract, ...]:
    return (
        desired_schema.authority_contract(),
        desired_schema.authority_v2_contract(),
        _desired_deployment_contract(),
        _publish_intent_contract(),
        _publish_preparation_contract(),
        _publish_evidence_contract(),
        _publish_evidence_v2_contract(),
        _fetch_evidence_contract(),
        _reconcile_evidence_contract(),
        _checkpoint_contract(),
        _recovery_contract(),
    )


def _publish_intent_contract() -> desired_schema.GitOpsSchemaContract:
    return desired_schema.documented_contract(
        name="airflow-desired-state-publish-intent",
        kind="dpone.airflow-desired-state-publish-intent.v1",
        title="dpone GitOps Airflow desired-state durable publish intent",
        required=("schema", "candidate_sha256", "occurrence_id", "promoted_at"),
        properties={
            "schema": {"const": "dpone.airflow-desired-state-publish-intent.v1"},
            "candidate_sha256": desired_schema.DIGEST,
            "occurrence_id": desired_schema.uuid_schema(),
            "promoted_at": desired_schema.TIMESTAMP,
        },
        additional_properties=False,
    )


def _publish_preparation_contract() -> desired_schema.GitOpsSchemaContract:
    properties = {
        "schema": {"const": "dpone.airflow-desired-state-publish-preparation.v1"},
        "candidate": desired_schema.publish_candidate_schema(),
        "intent": desired_schema.publish_intent_schema(),
    }
    return desired_schema.documented_contract(
        name="airflow-desired-state-publish-preparation",
        kind="dpone.airflow-desired-state-publish-preparation.v1",
        title="dpone GitOps immutable cross-job desired-state publish preparation",
        required=tuple(properties),
        properties=properties,
        additional_properties=False,
    )


def _desired_deployment_contract() -> desired_schema.GitOpsSchemaContract:
    return desired_schema.documented_contract(
        name="airflow-desired-deployment",
        kind="dpone.airflow-desired-deployment.v1",
        title="dpone GitOps exact Airflow desired deployment",
        required=(
            "schema",
            "environment",
            "source",
            "promotion",
            "previous",
            "promoted_at",
        ),
        properties={
            "schema": {"const": "dpone.airflow-desired-deployment.v1"},
            "environment": desired_schema.safe_name_schema(),
            "source": desired_schema.source_schema(),
            "promotion": desired_schema.promotion_schema(),
            "previous": desired_schema.previous_schema(),
            "promoted_at": desired_schema.TIMESTAMP,
        },
        additional_properties=False,
    )


def _publish_evidence_contract() -> desired_schema.GitOpsSchemaContract:
    return desired_schema.documented_contract(
        name="airflow-desired-state-publish",
        kind="dpone.airflow-desired-state-publish.v1",
        title="dpone GitOps Airflow desired-state publication evidence",
        required=(
            "schema",
            "passed",
            "status",
            "outcome",
            "environment",
            "occurrence_id",
            "release_id",
            "deployment_id",
            "desired_state_sha256",
            "previous_revision",
            "committed_revision",
            "published_at",
            "state_may_have_changed",
        ),
        properties={
            "schema": {"const": "dpone.airflow-desired-state-publish.v1"},
            "passed": {"const": True},
            "status": {"const": "published"},
            "outcome": {"enum": ["created", "replaced", "idempotent"]},
            "environment": desired_schema.safe_name_schema(),
            "occurrence_id": desired_schema.uuid_schema(),
            "release_id": desired_schema.DIGEST,
            "deployment_id": desired_schema.DIGEST,
            "desired_state_sha256": desired_schema.DIGEST,
            "previous_revision": desired_schema.nullable_revision(),
            "committed_revision": desired_schema.REVISION,
            "published_at": desired_schema.TIMESTAMP,
            "state_may_have_changed": {"const": False},
        },
        additional_properties=False,
    )


def _publish_evidence_v2_contract() -> desired_schema.GitOpsSchemaContract:
    properties = desired_schema.publish_evidence_v2_properties()
    return desired_schema.documented_contract(
        name="airflow-desired-state-publish-v2",
        kind="dpone.airflow-desired-state-publish.v2",
        title="dpone GitOps Airflow desired-state publication evidence with job provenance",
        required=tuple(properties),
        properties=properties,
        additional_properties=False,
    )


def _fetch_evidence_contract() -> desired_schema.GitOpsSchemaContract:
    contract = desired_schema.documented_contract(
        name="airflow-desired-state-fetch",
        kind="dpone.airflow-desired-state-fetch.v1",
        title="dpone GitOps Airflow desired-state fetch evidence",
        required=(
            "schema",
            "passed",
            "status",
            "environment",
            "observed_revision",
            "desired_state_sha256",
            "deployment_id",
            "output_path",
        ),
        properties={
            "schema": {"const": "dpone.airflow-desired-state-fetch.v1"},
            "passed": {"const": True},
            "status": {"enum": ["fetched", "unchanged"]},
            "environment": desired_schema.safe_name_schema(),
            "observed_revision": desired_schema.REVISION,
            "desired_state_sha256": {"anyOf": [desired_schema.DIGEST, {"type": "null"}]},
            "deployment_id": {"anyOf": [desired_schema.DIGEST, {"type": "null"}]},
            "output_path": {"type": "string", "minLength": 1, "maxLength": 4096},
        },
        additional_properties=False,
    )
    contract.schema["oneOf"] = [
        {
            "properties": {
                "status": {"const": "fetched"},
                "desired_state_sha256": desired_schema.DIGEST,
                "deployment_id": desired_schema.DIGEST,
            }
        },
        {
            "properties": {
                "status": {"const": "unchanged"},
                "desired_state_sha256": {"type": "null"},
                "deployment_id": {"type": "null"},
            }
        },
    ]
    return contract


def _reconcile_evidence_contract() -> desired_schema.GitOpsSchemaContract:
    contract = desired_schema.documented_contract(
        name="airflow-desired-state-reconcile",
        kind="dpone.airflow-desired-state-reconcile.v1",
        title="dpone GitOps Airflow desired-state reconcile evidence",
        required=(
            "schema",
            "passed",
            "status",
            "environment",
            "observed_revision",
            "desired_state_sha256",
            "registry_scope_id",
            "source_project",
            "source_ref",
            "release_id",
            "deployment_id",
            "occurrence_id",
            "source_git_sha",
            "airflow_index_sha256",
            "runtime_image_digest",
            "expected_dag_ids",
            "activation_id",
            "previous_deployment_id",
            "predecessor_status",
            "materialized",
            "activated",
        ),
        properties={
            "schema": {"const": "dpone.airflow-desired-state-reconcile.v1"},
            "passed": {"const": True},
            "status": {"enum": ["activated", "recovered", "unchanged"]},
            "environment": desired_schema.safe_name_schema(),
            "observed_revision": desired_schema.REVISION,
            "desired_state_sha256": desired_schema.DIGEST,
            "registry_scope_id": desired_schema.DIGEST,
            "source_project": desired_schema.project_schema(),
            "source_ref": desired_schema.bounded_text_schema(256),
            "release_id": desired_schema.DIGEST,
            "deployment_id": desired_schema.DIGEST,
            "occurrence_id": desired_schema.uuid_schema(),
            "source_git_sha": {
                "type": "string",
                "pattern": "^(?:[0-9a-f]{40}|[0-9a-f]{64})$",
            },
            "airflow_index_sha256": desired_schema.DIGEST,
            "runtime_image_digest": desired_schema.DIGEST,
            "expected_dag_ids": desired_schema.expected_dag_ids_schema(),
            "activation_id": desired_schema.uuid_schema(),
            "previous_deployment_id": {"anyOf": [desired_schema.DIGEST, {"type": "null"}]},
            "predecessor_status": {
                "enum": [
                    "bootstrap",
                    "continuous",
                    "recovered",
                    "skipped",
                    "unchanged",
                ]
            },
            "materialized": {"type": "boolean"},
            "activated": {"type": "boolean"},
        },
        additional_properties=False,
    )
    contract.schema["oneOf"] = [
        {
            "properties": {
                "status": {"const": "activated"},
                "materialized": {"const": True},
                "activated": {"const": True},
            }
        },
        {
            "properties": {
                "status": {"const": "unchanged"},
                "materialized": {"const": False},
                "activated": {"const": False},
            }
        },
        {
            "properties": {
                "status": {"const": "recovered"},
                "materialized": {"const": False},
                "activated": {"const": False},
                "predecessor_status": {"const": "recovered"},
            }
        },
    ]
    return contract


def _checkpoint_contract() -> desired_schema.GitOpsSchemaContract:
    return desired_schema.documented_contract(
        name="airflow-desired-state-checkpoint",
        kind="dpone.airflow-desired-state-checkpoint.v1",
        title="dpone GitOps local Airflow desired-state activation checkpoint",
        required=(
            "schema",
            "environment",
            "observed_revision",
            "desired_state_sha256",
            "registry_scope_id",
            "source_project",
            "source_ref",
            "release_id",
            "deployment_id",
            "occurrence_id",
            "source_git_sha",
            "airflow_index_sha256",
            "runtime_image_digest",
            "expected_dag_ids",
            "activation_id",
        ),
        properties={
            "schema": {"const": "dpone.airflow-desired-state-checkpoint.v1"},
            "environment": desired_schema.safe_name_schema(),
            "observed_revision": desired_schema.REVISION,
            "desired_state_sha256": desired_schema.DIGEST,
            "registry_scope_id": desired_schema.DIGEST,
            "source_project": desired_schema.project_schema(),
            "source_ref": desired_schema.bounded_text_schema(256),
            "release_id": desired_schema.DIGEST,
            "deployment_id": desired_schema.DIGEST,
            "occurrence_id": desired_schema.uuid_schema(),
            "source_git_sha": {
                "type": "string",
                "pattern": "^(?:[0-9a-f]{40}|[0-9a-f]{64})$",
            },
            "airflow_index_sha256": desired_schema.DIGEST,
            "runtime_image_digest": desired_schema.DIGEST,
            "expected_dag_ids": desired_schema.expected_dag_ids_schema(),
            "activation_id": desired_schema.uuid_schema(),
        },
        additional_properties=False,
    )


def _recovery_contract() -> desired_schema.GitOpsSchemaContract:
    desired = _desired_deployment_contract().schema
    return desired_schema.documented_contract(
        name="airflow-desired-state-recovery",
        kind="dpone.airflow-desired-state-recovery.v1",
        title="dpone GitOps local Airflow desired-state pre-activation recovery record",
        required=("schema", "observed_revision", "desired_state"),
        properties={
            "schema": {"const": "dpone.airflow-desired-state-recovery.v1"},
            "observed_revision": desired_schema.REVISION,
            "desired_state": {
                "type": "object",
                "required": desired["required"],
                "properties": desired["properties"],
                "additionalProperties": False,
            },
        },
        additional_properties=False,
    )


__all__ = ["airflow_desired_state_schema_contracts"]
