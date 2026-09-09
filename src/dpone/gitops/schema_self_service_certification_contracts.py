"""JSON Schema contracts for Airflow self-service evidence."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract

_DIGEST = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
_COMMIT = {"type": "string", "pattern": "^(?:[0-9a-f]{40}|[0-9a-f]{64})$"}
_STATUS = {"enum": ["PASS", "FAIL", "UNVERIFIED"]}


def self_service_certification_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (_usability_study_contract(), _certification_contract())


def _usability_study_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="self-service-usability-study",
        kind="dpone.self-service-usability-study.v1",
        title="dpone GitOps self-service usability study",
        required=(
            "schema",
            "protocol",
            "target_commit",
            "facilitator_ref",
            "started_at",
            "completed_at",
            "sessions",
        ),
        properties={
            "schema": {"const": "dpone.self-service-usability-study.v1"},
            "protocol": {"const": "airflow_first_dag_and_safe_sample_v1"},
            "target_commit": _COMMIT,
            "facilitator_ref": _text(128),
            "started_at": _date_time(),
            "completed_at": _date_time(),
            "sessions": {"type": "array", "items": {"$ref": "#/$defs/session"}, "maxItems": 100},
        },
        defs={"sha256": _DIGEST, "session": _session()},
        additional_properties=False,
    )


def _certification_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="self-service-certification",
        kind="dpone.self-service-certification.v1",
        title="dpone GitOps self-service certification",
        required=(
            "schema",
            "expected_commit",
            "evaluated_at",
            "overall_status",
            "has_input_failures",
            "usability",
            "reference_deployments",
        ),
        properties={
            "schema": {"const": "dpone.self-service-certification.v1"},
            "expected_commit": _COMMIT,
            "evaluated_at": _date_time(),
            "overall_status": _STATUS,
            "has_input_failures": {"type": "boolean"},
            "usability": {"$ref": "#/$defs/usability"},
            "reference_deployments": {"$ref": "#/$defs/references"},
        },
        defs={
            "sha256": _DIGEST,
            "nullable_sha256": _nullable({"$ref": "#/$defs/sha256"}),
            "nullable_text": _nullable(_text()),
            "nullable_number": _nullable({"type": "number", "minimum": 0}),
            "status": _STATUS,
            "thresholds": _thresholds(),
            "usability": _usability(),
            "reference_proof": _reference_proof(),
            "references": _references(),
        },
        additional_properties=False,
    )


def _session() -> dict[str, Any]:
    fields = (
        "session_id",
        "participant_ref",
        "first_time_dpone_user",
        "consent_recorded",
        "started_at",
        "dag_preview_at",
        "safe_sample_at",
        "finished_at",
        "outcome",
        "commands_used",
        "assistance_events",
        "authored_airflow_python",
        "transcript_sha256",
        "blocker_codes",
    )
    return _object(
        fields,
        {
            "session_id": _text(128),
            "participant_ref": {"$ref": "#/$defs/sha256"},
            "first_time_dpone_user": {"const": True},
            "consent_recorded": {"const": True},
            "started_at": _date_time(),
            "dag_preview_at": _nullable(_date_time()),
            "safe_sample_at": _nullable(_date_time()),
            "finished_at": _date_time(),
            "outcome": {"enum": ["passed", "failed", "abandoned"]},
            "commands_used": _bounded_count(),
            "assistance_events": _bounded_count(),
            "authored_airflow_python": {"type": "boolean"},
            "transcript_sha256": {"$ref": "#/$defs/sha256"},
            "blocker_codes": _texts(maximum=50, text_maximum=128),
        },
    )


def _thresholds() -> dict[str, Any]:
    fields = (
        "minimum_participants",
        "minimum_success_rate",
        "maximum_first_dag_seconds",
        "maximum_safe_sample_seconds",
        "maximum_commands",
    )
    return _object(
        fields,
        {
            "minimum_participants": {"type": "integer", "minimum": 1},
            "minimum_success_rate": {"type": "number", "minimum": 0, "maximum": 1},
            "maximum_first_dag_seconds": {"type": "integer", "minimum": 1},
            "maximum_safe_sample_seconds": {"type": "integer", "minimum": 1},
            "maximum_commands": {"type": "integer", "minimum": 1},
        },
    )


def _usability() -> dict[str, Any]:
    fields = (
        "status",
        "supplied",
        "source_sha256",
        "valid_sessions",
        "successful_sessions",
        "complete_success_rate",
        "first_dag_target_rate",
        "safe_sample_target_rate",
        "no_airflow_python_rate",
        "p50_first_dag_seconds",
        "p50_safe_sample_seconds",
        "thresholds",
        "blockers",
    )
    properties: dict[str, Any] = {
        "status": {"$ref": "#/$defs/status"},
        "supplied": {"type": "boolean"},
        "source_sha256": {"$ref": "#/$defs/nullable_sha256"},
        "valid_sessions": _count(),
        "successful_sessions": _count(),
        "thresholds": {"$ref": "#/$defs/thresholds"},
        "blockers": _texts(),
    }
    for field in (
        "complete_success_rate",
        "first_dag_target_rate",
        "safe_sample_target_rate",
        "no_airflow_python_rate",
    ):
        properties[field] = _nullable({"type": "number", "minimum": 0, "maximum": 1})
    properties["p50_first_dag_seconds"] = {"$ref": "#/$defs/nullable_number"}
    properties["p50_safe_sample_seconds"] = {"$ref": "#/$defs/nullable_number"}
    return _object(fields, properties)


def _reference_proof() -> dict[str, Any]:
    fields = (
        "evidence_set",
        "status",
        "release_id",
        "deployment_id",
        "signer_identity",
        "correlation_id",
        "release_set_sha256",
        "deployment_set_sha256",
        "airflow_evidence_sha256",
        "blockers",
    )
    properties: dict[str, Any] = {
        "evidence_set": _text(128),
        "status": {"enum": ["PASS", "FAIL"]},
        "signer_identity": {"$ref": "#/$defs/nullable_text"},
        "blockers": _texts(),
    }
    for field in fields:
        if field.endswith("_id") or field.endswith("_sha256"):
            properties[field] = {"$ref": "#/$defs/nullable_sha256"}
    return _object(fields, properties)


def _references() -> dict[str, Any]:
    fields = (
        "status",
        "supplied_count",
        "valid_count",
        "distinct_deployment_count",
        "distinct_signer_count",
        "proofs",
        "blockers",
    )
    return _object(
        fields,
        {
            "status": {"$ref": "#/$defs/status"},
            "supplied_count": _count(),
            "valid_count": _count(),
            "distinct_deployment_count": _count(),
            "distinct_signer_count": _count(),
            "proofs": {"type": "array", "items": {"$ref": "#/$defs/reference_proof"}, "maxItems": 100},
            "blockers": _texts(),
        },
    )


def _object(required: tuple[str, ...], properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "required": list(required), "properties": properties, "additionalProperties": False}


def _nullable(schema: dict[str, Any]) -> dict[str, Any]:
    return {"anyOf": [schema, {"type": "null"}]}


def _text(maximum: int = 1024) -> dict[str, Any]:
    return {"type": "string", "minLength": 1, "maxLength": maximum}


def _date_time() -> dict[str, str]:
    return {"type": "string", "format": "date-time"}


def _count() -> dict[str, Any]:
    return {"type": "integer", "minimum": 0}


def _bounded_count() -> dict[str, Any]:
    return {"type": "integer", "minimum": 0, "maximum": 100}


def _texts(*, maximum: int = 1000, text_maximum: int = 1024) -> dict[str, Any]:
    return {
        "type": "array",
        "items": _text(text_maximum),
        "uniqueItems": True,
        "maxItems": maximum,
    }


__all__ = ["self_service_certification_schema_contracts"]
