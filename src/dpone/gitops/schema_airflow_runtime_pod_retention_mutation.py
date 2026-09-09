"""Apply-report and ordered mutation-event schemas for runtime Pod retention."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract


def minimum_age_schema() -> dict[str, int | str]:
    return {"type": "integer", "minimum": 300, "maximum": 2_592_000}


def page_size_schema() -> dict[str, int | str]:
    return {"type": "integer", "minimum": 1, "maximum": 1_000}


def date_time_schema() -> dict[str, str]:
    return {"type": "string", "format": "date-time"}


def text_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


def unique_texts_schema() -> dict[str, Any]:
    return {"type": "array", "items": text_schema(), "uniqueItems": True, "maxItems": 10_000}


def sha256_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


def apply_contract() -> GitOpsSchemaContract:
    contract = documented_contract(
        name="airflow-runtime-pod-retention-apply",
        kind="dpone.airflow-runtime-pod-retention-apply.v1",
        title="dpone GitOps Airflow runtime Pod retention apply report",
        required=(
            "schema",
            "status",
            "operation_id",
            "evidence_status",
            "evidence_durability",
            "namespace",
            "actor",
            "actor_source",
            "authorization_authority",
            "credential_mode",
            "deletion_evidence",
            "observed_at",
            "minimum_age_seconds",
            "max_delete_count",
            "age_basis",
            "terminal_age_exact",
            "items",
            "delete_accepted_pod_names",
            "skipped_pod_names",
            "failed_pod_names",
        ),
        properties={
            "schema": {"const": "dpone.airflow-runtime-pod-retention-apply.v1"},
            "status": {"enum": ["ok", "partial", "failed"]},
            "operation_id": sha256_schema(),
            "evidence_status": {"enum": ["complete", "incomplete"]},
            "evidence_durability": {"enum": ["process_ordered", "durable_acknowledged"]},
            "namespace": text_schema(),
            "actor": text_schema(),
            "actor_source": {"const": "operator_acknowledgement"},
            "authorization_authority": {"const": "kubernetes_api_rbac"},
            "credential_mode": {"enum": ["in-cluster", "kubeconfig"]},
            "credential_context": {"type": ["string", "null"], "minLength": 1, "maxLength": 253},
            "deletion_evidence": {"const": "api_request_accepted_not_observed"},
            "observed_at": date_time_schema(),
            "minimum_age_seconds": minimum_age_schema(),
            "max_delete_count": {"type": "integer", "minimum": 1, "maximum": 1_000},
            "age_basis": {"const": "creation_timestamp_fallback"},
            "terminal_age_exact": {"const": False},
            "items": {"type": "array", "items": {"$ref": "#/$defs/apply_item"}, "maxItems": 10_000},
            "delete_accepted_pod_names": unique_texts_schema(),
            "skipped_pod_names": unique_texts_schema(),
            "failed_pod_names": unique_texts_schema(),
        },
        defs={"apply_item": _apply_item_schema()},
        additional_properties=False,
    )
    contract.schema["allOf"] = [
        {
            "if": {
                "properties": {"evidence_status": {"const": "incomplete"}},
                "required": ["evidence_status"],
            },
            "then": {"properties": {"status": {"const": "failed"}}},
        },
        {
            "if": {"properties": {"status": {"const": "ok"}}, "required": ["status"]},
            "then": {
                "properties": {
                    "evidence_status": {"const": "complete"},
                    "failed_pod_names": {"maxItems": 0},
                    "items": {
                        "items": {
                            "properties": {
                                "action": {"enum": ["delete_accepted", "skipped", "preserved"]},
                                "reason": {
                                    "enum": [
                                        "stale_terminal",
                                        "already_absent",
                                        "terminating",
                                        "minimum_age",
                                    ]
                                },
                            },
                        }
                    },
                }
            },
        },
    ]
    contract.schema["x-dpone-authoritative-field"] = "items"
    return contract


def event_contract() -> GitOpsSchemaContract:
    nullable_text = {"type": ["string", "null"]}
    contract = documented_contract(
        name="airflow-runtime-pod-retention-event",
        kind="dpone.airflow-runtime-pod-retention-event.v1",
        title="dpone GitOps Airflow runtime Pod retention mutation event",
        required=(
            "schema",
            "operation_id",
            "sequence",
            "event",
            "namespace",
            "observed_at",
            "actor",
            "credential_mode",
            "selected_count",
            "pod_ref",
            "precondition_ref",
            "pod_name",
            "outcome",
            "error_code",
        ),
        properties={
            "schema": {"const": "dpone.airflow-runtime-pod-retention-event.v1"},
            "operation_id": sha256_schema(),
            "sequence": {"type": "integer", "minimum": 1, "maximum": 10_000},
            "event": {"enum": ["operation_started", "delete_intent", "delete_outcome", "operation_completed"]},
            "namespace": text_schema(),
            "observed_at": date_time_schema(),
            "actor": text_schema(),
            "credential_mode": {"enum": ["in-cluster", "kubeconfig"]},
            "selected_count": {"type": "integer", "minimum": 0, "maximum": 1_000},
            "pod_ref": {"anyOf": [sha256_schema(), {"type": "null"}]},
            "precondition_ref": {"anyOf": [sha256_schema(), {"type": "null"}]},
            "pod_name": nullable_text,
            "outcome": {
                "enum": [
                    "started",
                    "intent_recorded",
                    "delete_accepted",
                    "already_absent",
                    "changed_since_plan",
                    "delete_failed",
                    "interrupted",
                    "ok",
                    "partial",
                    "failed",
                ]
            },
            "error_code": nullable_text,
        },
        additional_properties=False,
    )
    contract.schema["allOf"] = [
        _event_shape("operation_started", outcome={"const": "started"}, pod_fields=_null_pod_fields()),
        _event_shape("delete_intent", outcome={"const": "intent_recorded"}, pod_fields=_required_pod_fields()),
        _event_shape(
            "delete_outcome",
            outcome={
                "enum": [
                    "delete_accepted",
                    "already_absent",
                    "changed_since_plan",
                    "delete_failed",
                    "interrupted",
                ]
            },
            pod_fields=_required_pod_fields(),
        ),
        _event_shape(
            "operation_completed",
            outcome={"enum": ["ok", "partial", "failed"]},
            pod_fields=_null_pod_fields(),
        ),
        _delete_outcome_error_shape(("delete_failed", "interrupted"), error_code=text_schema()),
        _delete_outcome_error_shape(
            ("delete_accepted", "already_absent", "changed_since_plan"),
            error_code={"type": "null"},
        ),
    ]
    return contract


def _event_shape(event: str, *, outcome: dict[str, Any], pod_fields: dict[str, Any]) -> dict[str, Any]:
    return {
        "if": {"properties": {"event": {"const": event}}, "required": ["event"]},
        "then": {
            "properties": {
                **pod_fields,
                "outcome": outcome,
                "error_code": {"type": ["string", "null"]} if event == "delete_outcome" else {"type": "null"},
            }
        },
    }


def _null_pod_fields() -> dict[str, Any]:
    return {name: {"type": "null"} for name in ("pod_ref", "precondition_ref", "pod_name")}


def _required_pod_fields() -> dict[str, Any]:
    return {
        "pod_ref": sha256_schema(),
        "precondition_ref": sha256_schema(),
        "pod_name": text_schema(),
    }


def _delete_outcome_error_shape(outcomes: tuple[str, ...], *, error_code: dict[str, Any]) -> dict[str, Any]:
    return {
        "if": {
            "properties": {"event": {"const": "delete_outcome"}, "outcome": {"enum": list(outcomes)}},
            "required": ["event", "outcome"],
        },
        "then": {"properties": {"error_code": error_code}},
    }


def _apply_item_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["sequence", "pod_ref", "precondition_ref", "pod_name", "phase", "action", "reason"],
        "additionalProperties": False,
        "properties": {
            "sequence": {"type": "integer", "minimum": 1, "maximum": 10_000},
            "pod_ref": sha256_schema(),
            "precondition_ref": sha256_schema(),
            "pod_name": text_schema(),
            "phase": text_schema(),
            "action": {"enum": ["delete_accepted", "skipped", "failed", "preserved"]},
            "reason": {
                "enum": [
                    "stale_terminal",
                    "already_absent",
                    "changed_since_plan",
                    "delete_failed",
                    "evidence_unavailable",
                    "batch_limit",
                    "not_attempted_after_failure",
                    "inventory_phase_conflict",
                    "invalid_phase",
                    "invalid_identity",
                    "terminating",
                    "invalid_ownership",
                    "missing_correlation",
                    "timestamp_missing",
                    "future_timestamp",
                    "minimum_age",
                ]
            },
            "error_code": {"type": "string", "pattern": "^DPONE_AIRFLOW_RUNTIME_POD_RETENTION_[A-Z0-9_]+$"},
            "http_status": {"type": "integer", "minimum": 100, "maximum": 599},
        },
    }


__all__ = [
    "apply_contract",
    "date_time_schema",
    "event_contract",
    "minimum_age_schema",
    "page_size_schema",
    "sha256_schema",
    "text_schema",
]
