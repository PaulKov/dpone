"""Closed canonical codec for protected MSSQL admission authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from dpone.contracts.semantic_refresh_attempt_binding import SemanticRefreshAttemptBinding
from dpone.contracts.semantic_refresh_execution_binding import (
    SemanticRefreshWorkflowExecutionBinding,
)
from dpone.contracts.semantic_refresh_operation_plan import SemanticRefreshOperationPlan
from dpone.contracts.semantic_refresh_workflow_plan import SemanticRefreshWorkflowPlan
from dpone.contracts.semantic_refresh_workflow_replacement import (
    SemanticRefreshWorkflowReplacementPlan,
)
from dpone.ports.semantic_refresh_mssql_authority_codec_values import (
    closed_mapping as _closed_mapping,
)
from dpone.ports.semantic_refresh_mssql_authority_codec_values import (
    mapping_int as _mapping_int,
)
from dpone.ports.semantic_refresh_mssql_authority_codec_values import (
    mapping_text as _mapping_text,
)
from dpone.ports.semantic_refresh_mssql_authority_codec_values import (
    sequence as _sequence,
)
from dpone.ports.semantic_refresh_mssql_authority_models import (
    MssqlCanonicalAdmissionBundle,
)
from dpone.ports.semantic_refresh_mssql_authority_records import (
    MssqlCanonicalAuthorityRecord,
)
from dpone.ports.semantic_refresh_mssql_authority_resource_codec import (
    model_resource_from_mapping as _model_resource_from_mapping,
)
from dpone.ports.semantic_refresh_mssql_primitives import (
    MssqlGuardClaim,
    MssqlWorkflowResourceBudget,
)

_SHA256_PREFIX = "sha256:"
_AUTHORITY_FIELDS = {
    "attempt_bindings",
    "authority_sha256",
    "controller_id",
    "execution_binding",
    "model_resources",
    "operation_plans",
    "owner_id",
    "replacement_plan",
    "reservation_id",
    "resource_budget",
    "resource_guards",
    "schema",
    "workflow_execution_id",
    "workflow_guard",
    "workflow_plan",
}


def authority_sha256(bundle: MssqlCanonicalAdmissionBundle) -> str:
    """Digest all protected runtime coordinates used to derive admission."""

    raw = json.dumps(
        _authority_unsigned(bundle),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return _SHA256_PREFIX + hashlib.sha256(raw).hexdigest()


def authority_json(bundle: MssqlCanonicalAdmissionBundle) -> str:
    """Serialize one closed canonical authority document for protected storage."""

    payload = {**_authority_unsigned(bundle), "authority_sha256": bundle.authority_sha256}
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def authority_from_record(
    record: MssqlCanonicalAuthorityRecord,
) -> MssqlCanonicalAdmissionBundle:
    """Parse and authenticate one ACTIVE control-table authority record."""

    _require_digest(record.workflow_execution_binding_sha256, "workflow_execution_binding_sha256")
    _require_digest(record.authority_sha256, "authority_sha256")
    if record.status != "ACTIVE":
        raise ValueError("canonical admission authority is not ACTIVE")
    try:
        raw = json.loads(record.authority_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("canonical admission authority JSON is invalid") from exc
    if not isinstance(raw, Mapping) or set(raw) != _AUTHORITY_FIELDS:
        raise ValueError("canonical admission authority fields are not closed")
    if raw["authority_sha256"] != record.authority_sha256:
        raise ValueError("canonical admission authority record digest differs")
    unsigned = {key: value for key, value in raw.items() if key != "authority_sha256"}
    encoded = json.dumps(unsigned, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    if _SHA256_PREFIX + hashlib.sha256(encoded).hexdigest() != record.authority_sha256:
        raise ValueError("canonical admission authority document digest differs")
    bundle = _bundle_from_mapping(raw)
    if bundle.workflow_execution_id != record.workflow_execution_id:
        raise ValueError("canonical authority workflow execution identity differs from control row")
    if bundle.execution_binding.workflow_execution_binding_sha256 != record.workflow_execution_binding_sha256:
        raise ValueError("canonical authority execution binding differs from lookup identity")
    if bundle.authority_sha256 != record.authority_sha256:
        raise ValueError("canonical authority typed content digest differs")
    return bundle


def canonical_authority_record(
    *,
    workflow_execution_binding_sha256: str,
    workflow_execution_id: str,
    authority_sha256: str,
    authority_json: str,
    status: str,
) -> MssqlCanonicalAuthorityRecord:
    """Construct the record consumed by this codec from primitive DB values."""

    return MssqlCanonicalAuthorityRecord(
        workflow_execution_binding_sha256=workflow_execution_binding_sha256,
        workflow_execution_id=workflow_execution_id,
        authority_sha256=authority_sha256,
        authority_json=authority_json,
        status=status,
    )


def _bundle_from_mapping(raw: Mapping[str, object]) -> MssqlCanonicalAdmissionBundle:
    return MssqlCanonicalAdmissionBundle(
        workflow_execution_id=_mapping_text(raw, "workflow_execution_id"),
        workflow_plan=SemanticRefreshWorkflowPlan.from_mapping(raw["workflow_plan"]),
        execution_binding=SemanticRefreshWorkflowExecutionBinding.from_mapping(raw["execution_binding"]),
        operation_plans=tuple(
            SemanticRefreshOperationPlan.from_mapping(item) for item in _sequence(raw, "operation_plans")
        ),
        attempt_bindings=tuple(
            SemanticRefreshAttemptBinding.from_mapping(item) for item in _sequence(raw, "attempt_bindings")
        ),
        workflow_guard=_guard_from_mapping(raw["workflow_guard"]),
        resource_guards=tuple(_guard_from_mapping(item) for item in _sequence(raw, "resource_guards")),
        model_resources=tuple(_model_resource_from_mapping(item) for item in _sequence(raw, "model_resources")),
        controller_id=_mapping_text(raw, "controller_id"),
        owner_id=_mapping_text(raw, "owner_id"),
        reservation_id=_mapping_text(raw, "reservation_id"),
        resource_budget=_resource_budget_from_mapping(raw["resource_budget"]),
        replacement_plan=(
            None
            if raw["replacement_plan"] is None
            else SemanticRefreshWorkflowReplacementPlan.from_mapping(raw["replacement_plan"])
        ),
    )


def _authority_unsigned(bundle: MssqlCanonicalAdmissionBundle) -> dict[str, object]:
    return {
        "attempt_bindings": [item.to_dict() for item in bundle.attempt_bindings],
        "controller_id": bundle.controller_id,
        "execution_binding": bundle.execution_binding.to_dict(),
        "model_resources": [
            {
                "baseline_receipt_sha256": item.baseline_receipt_sha256,
                "baseline_kind": item.baseline_kind,
                "baseline_receipt_json": item.baseline_receipt_json,
                "baseline_status": item.baseline_status,
                "baseline_clickhouse_generation": item.baseline_clickhouse_generation,
                "baseline_clickhouse_target_uuid": item.baseline_clickhouse_target_uuid,
                "baseline_target_predecessor_generation_id": (item.baseline_target_predecessor_generation_id),
                "artifact_authority": _dataclass_mapping(item.artifact_authority),
                "clickhouse_cluster_authority_id": item.clickhouse_cluster_authority_id,
                "clickhouse_target_uuid": item.clickhouse_target_uuid,
                "ddl_freeze_assurance_receipt_sha256": item.ddl_freeze_assurance_receipt_sha256,
                "ddl_epoch": item.ddl_epoch,
                "effective_key_mapping_sha256": item.effective_key_mapping_sha256,
                "effective_key_template_sha256": item.effective_key_template_sha256,
                "model_definition_proof_sha256": item.model_definition_proof_sha256,
                "model_unique_id": item.model_unique_id,
                "mssql_control_database": item.mssql_control_database,
                "mssql_control_schema": item.mssql_control_schema,
                "mssql_image_schema": item.mssql_image_schema,
                "mssql_connection_authority_id": item.mssql_connection_authority_id,
                "mssql_target_authority_id": item.mssql_target_authority_id,
                "publication_database": item.publication_database,
                "publication_scope_id": item.publication_scope_id,
                "publication_target_table": item.publication_target_table,
                "resource_policy": _dataclass_mapping(item.resource_policy),
                "route_certification_receipt_sha256": item.route_certification_receipt_sha256,
                "scope_image_namespace_policy_sha256": item.scope_image_namespace_policy_sha256,
                "strategy_template_sha256": item.strategy_template_sha256,
                "target_authority_id": item.target_authority_id,
                "target_head_authority_receipt_sha256": (item.target_head_authority_receipt_sha256),
                "target_head_terminal_receipt_sha256": (item.target_head_terminal_receipt_sha256),
                "target_predecessor_generation": item.target_predecessor_generation,
                "target_predecessor_generation_id": item.target_predecessor_generation_id,
                "target_predecessor_operation_id": item.target_predecessor_operation_id,
                "target_resource_id": item.target_resource_id,
                "utc_semantics_assurance_receipt_sha256": item.utc_semantics_assurance_receipt_sha256,
                "writable_columns": [_dataclass_mapping(column) for column in item.writable_columns],
                "writable_schema_sha256": item.writable_schema_sha256,
                "writer_exclusivity_assurance_receipt_sha256": (item.writer_exclusivity_assurance_receipt_sha256),
            }
            for item in bundle.model_resources
        ],
        "operation_plans": [item.to_dict() for item in bundle.operation_plans],
        "owner_id": bundle.owner_id,
        "replacement_plan": (bundle.replacement_plan.to_dict() if bundle.replacement_plan is not None else None),
        "reservation_id": bundle.reservation_id,
        "resource_budget": {
            field_name: getattr(bundle.resource_budget, field_name)
            for field_name in bundle.resource_budget.__dataclass_fields__
        },
        "resource_guards": [_guard_mapping(item) for item in bundle.resource_guards],
        "schema": "dpone.semantic-refresh-mssql-canonical-authority.v1",
        "workflow_execution_id": bundle.workflow_execution_id,
        "workflow_guard": _guard_mapping(bundle.workflow_guard),
        "workflow_plan": bundle.workflow_plan.to_dict(),
    }


def _guard_mapping(claim: MssqlGuardClaim) -> dict[str, object]:
    return {
        "expected_predecessor_epoch": claim.expected_predecessor_epoch,
        "fencing_epoch": claim.fencing_epoch,
        "resource_id": claim.resource_id,
    }


def _guard_from_mapping(value: object) -> MssqlGuardClaim:
    raw = _closed_mapping(
        value,
        "guard",
        {"expected_predecessor_epoch", "fencing_epoch", "resource_id"},
    )
    return MssqlGuardClaim(
        resource_id=_mapping_text(raw, "resource_id"),
        expected_predecessor_epoch=_mapping_int(raw, "expected_predecessor_epoch"),
        fencing_epoch=_mapping_int(raw, "fencing_epoch"),
    )


def _dataclass_mapping(value: object) -> dict[str, object]:
    fields = getattr(value, "__dataclass_fields__", None)
    if not isinstance(fields, dict):
        raise TypeError("canonical authority value must be a dataclass")
    return {field_name: getattr(value, field_name) for field_name in fields}


def _resource_budget_from_mapping(value: object) -> MssqlWorkflowResourceBudget:
    fields = {
        "max_workflow_clickhouse_staging_bytes",
        "max_workflow_peak_bytes",
        "max_workflow_prepared_models",
        "max_workflow_sealed_extract_bytes",
        "max_workflow_shadow_bytes",
    }
    raw = _closed_mapping(value, "resource_budget", fields)
    return MssqlWorkflowResourceBudget(
        max_workflow_prepared_models=_mapping_int(raw, "max_workflow_prepared_models"),
        max_workflow_sealed_extract_bytes=_mapping_int(raw, "max_workflow_sealed_extract_bytes"),
        max_workflow_clickhouse_staging_bytes=_mapping_int(raw, "max_workflow_clickhouse_staging_bytes"),
        max_workflow_shadow_bytes=_mapping_int(raw, "max_workflow_shadow_bytes"),
        max_workflow_peak_bytes=_mapping_int(raw, "max_workflow_peak_bytes"),
    )


def _require_digest(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith(_SHA256_PREFIX)
        or len(value) != len(_SHA256_PREFIX) + 64
        or any(character not in "0123456789abcdef" for character in value[len(_SHA256_PREFIX) :])
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")


__all__ = [
    "authority_from_record",
    "authority_json",
    "authority_sha256",
    "canonical_authority_record",
]
