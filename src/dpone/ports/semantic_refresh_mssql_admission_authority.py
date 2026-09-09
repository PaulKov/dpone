"""Deterministic admission and attempt-strategy authority composition."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from dpone.contracts.semantic_refresh_types import ReplacementAction, WorkflowMode
from dpone.ports.semantic_refresh_mssql import (
    MssqlAdmissionRequest,
    MssqlImageKeyColumn,
    MssqlJournalPreparation,
    MssqlPrerequisiteAuthorityClaim,
    MssqlRuntimeAssuranceClaim,
    MssqlTargetOwnerClaim,
    mssql_guard_set_sha256,
    mssql_journal_set_sha256,
    mssql_strategy_authority_sha256,
)
from dpone.ports.semantic_refresh_mssql_authority_codec import (
    authority_from_record,
    authority_json,
)
from dpone.ports.semantic_refresh_mssql_recovery_heads import (
    MssqlAdmissionTargetHeadAuthority,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_authority_models import (
        MssqlCanonicalAdmissionBundle,
        MssqlModelResourceAuthority,
    )
    from dpone.ports.semantic_refresh_mssql_authority_records import (
        MssqlCanonicalAuthorityRecord,
    )

_DECIMAL = re.compile(r"decimal\(([1-9][0-9]?),([0-9]|[1-9][0-9]?)\)")


def compose_admission(bundle: MssqlCanonicalAdmissionBundle) -> MssqlAdmissionRequest:
    """Build exact journals and guard closure from authenticated bundle data."""

    attempts = {item.operation_id: item for item in bundle.attempt_bindings}
    resources = {item.model_unique_id: item for item in bundle.model_resources}
    guards = {item.resource_id: item for item in bundle.resource_guards}
    actions = _replacement_actions(bundle)
    journals = tuple(
        sorted(
            (
                _journal(
                    bundle,
                    operation,
                    attempts[operation.operation_id],
                    resources[operation.model_unique_id],
                    guards[resources[operation.model_unique_id].target_resource_id].fencing_epoch,
                    actions[operation.model_unique_id],
                )
                for operation in bundle.operation_plans
            ),
            key=lambda item: item.operation_id,
        )
    )
    return MssqlAdmissionRequest(
        workflow_id=bundle.workflow_execution_id,
        workflow_execution_id=bundle.workflow_execution_id,
        workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
        workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
        canonical_authority_sha256=bundle.authority_sha256,
        canonical_authority_json=authority_json(bundle),
        controller_id=bundle.controller_id,
        owner_id=bundle.owner_id,
        reservation_id=bundle.reservation_id,
        resource_budget=bundle.resource_budget,
        expected_guard_set_sha256=mssql_guard_set_sha256(
            bundle.workflow_guard,
            bundle.resource_guards,
        ),
        expected_journal_set_sha256=mssql_journal_set_sha256(journals),
        workflow_guard=bundle.workflow_guard,
        resource_guards=bundle.resource_guards,
        journals=journals,
        target_heads=tuple(
            sorted(
                MssqlAdmissionTargetHeadAuthority(
                    target_resource_id=resource.target_resource_id,
                    model_unique_id=resource.model_unique_id,
                    clickhouse_target_authority_id=resource.target_authority_id,
                    target_generation=resource.target_predecessor_generation,
                    target_generation_id=resource.target_predecessor_generation_id,
                    target_uuid=resource.clickhouse_target_uuid,
                    owner_operation_id=resource.target_predecessor_operation_id,
                    terminal_receipt_sha256=resource.target_head_terminal_receipt_sha256,
                    head_authority_receipt_sha256=resource.target_head_authority_receipt_sha256,
                )
                for resource in resources.values()
            )
        ),
        target_owners=tuple(
            MssqlTargetOwnerClaim(
                target_authority_id=resources[item.model_unique_id].target_authority_id,
                model_unique_id=item.model_unique_id,
                deployment_id=item.deployment_id,
                owner_generation=item.owner_generation,
            )
            for item in bundle.operation_plans
        ),
        prerequisite_authorities=tuple(
            mssql_prerequisite_authority(operation, resources[operation.model_unique_id])
            for operation in bundle.operation_plans
        ),
    )


def compose_admission_from_record(
    record: MssqlCanonicalAuthorityRecord,
) -> tuple[MssqlCanonicalAdmissionBundle, MssqlAdmissionRequest]:
    """Authenticate a stored authority and materialize its exact admission."""

    bundle = authority_from_record(record)
    return bundle, compose_admission(bundle)


def _journal(bundle, operation, attempt, resource, fencing_epoch, action) -> MssqlJournalPreparation:
    strategy_json = _strategy_authority_json(
        bundle=bundle,
        operation=operation,
        attempt=attempt,
        resource=resource,
        fencing_epoch=fencing_epoch,
        replacement_action=action,
    )
    return MssqlJournalPreparation(
        model_unique_id=operation.model_unique_id,
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        attempt_binding_sha256=attempt.attempt_binding_sha256,
        strategy_authority_json=strategy_json,
        strategy_authority_sha256=mssql_strategy_authority_sha256(strategy_json),
        baseline_receipt_sha256=resource.baseline_receipt_sha256,
        baseline_kind=resource.baseline_kind,
        baseline_receipt_json=resource.baseline_receipt_json,
        baseline_status=resource.baseline_status,
        image_key_columns=tuple(
            MssqlImageKeyColumn(
                name=item.name,
                order_encoding=("UUID_TEXT" if item.source_type == "uniqueidentifier" else "NATIVE"),
            )
            for item in operation.effective_key_columns
        ),
        target_resource_id=resource.target_resource_id,
        publication_database=resource.publication_database,
        publication_target_table=resource.publication_target_table,
        publication_scope_id=resource.publication_scope_id,
        target_predecessor_generation_id=operation.target_predecessor_generation_id,
        scope_predecessor_operation_id=operation.scope_predecessor_operation_id,
        fencing_epoch=fencing_epoch,
        replaces_failed_operation_id=operation.replaces_failed_operation_id,
    )


def _strategy_authority_json(
    *,
    bundle,
    operation,
    attempt,
    resource: MssqlModelResourceAuthority,
    fencing_epoch: int,
    replacement_action: ReplacementAction,
) -> str:
    target_database, target_schema, target_identifier = _target_parts(resource.target_resource_id)
    suffix = f"{operation.operation_id[7:31]}_{attempt.attempt_binding_sha256[7:23]}"
    payload = {
        "after_image_relation": _relation(
            resource.mssql_control_database,
            resource.mssql_image_schema,
            f"dpone_sr_after_{suffix}",
        ),
        "artifact_authority": _dataclass_mapping(resource.artifact_authority),
        "attempt_binding_sha256": attempt.attempt_binding_sha256,
        "before_image_relation": _relation(
            resource.mssql_control_database,
            resource.mssql_image_schema,
            f"dpone_sr_before_{suffix}",
        ),
        "clickhouse_cluster_authority_id": resource.clickhouse_cluster_authority_id,
        "effective_key_mapping_sha256": operation.effective_key_mapping_sha256,
        "effective_key_template_sha256": operation.effective_key_template_sha256,
        "effective_keys": [_macro_key(item) for item in operation.effective_key_columns],
        "event_time_column": operation.event_time_column,
        "fencing_epoch": fencing_epoch,
        "guard_relation": _relation(
            resource.mssql_control_database,
            resource.mssql_control_schema,
            "semantic_refresh_guards",
        ),
        "guard_resource_id": resource.target_resource_id,
        "model_unique_id": operation.model_unique_id,
        "mssql_connection_authority_id": resource.mssql_connection_authority_id,
        "mssql_target_authority_id": resource.mssql_target_authority_id,
        "operation_id": operation.operation_id,
        "operation_plan_sha256": operation.operation_plan_sha256,
        "ordered_writable_schema_sha256": resource.writable_schema_sha256,
        "owner_id": bundle.owner_id,
        "receipt_relation": _relation(
            resource.mssql_control_database,
            resource.mssql_control_schema,
            "semantic_refresh_receipts",
        ),
        "replacement_action": replacement_action.value,
        "resource_policy": _dataclass_mapping(resource.resource_policy),
        "route_certification_receipt_sha256": resource.route_certification_receipt_sha256,
        "ddl_freeze_assurance_receipt_sha256": (resource.ddl_freeze_assurance_receipt_sha256),
        "ddl_epoch": resource.ddl_epoch,
        "schema": "dpone.semantic-refresh-mssql-attempt-strategy-authority.v1",
        "scope_end_utc": operation.scope_end,
        "scope_image_namespace_policy_sha256": (resource.scope_image_namespace_policy_sha256),
        "scope_start_utc": operation.scope_start,
        "strategy_template_sha256": resource.strategy_template_sha256,
        "target_relation": _relation(target_database, target_schema, target_identifier),
        "writable_columns": [_dataclass_mapping(item) for item in resource.writable_columns],
        "writer_exclusivity_assurance_receipt_sha256": (resource.writer_exclusivity_assurance_receipt_sha256),
        "workflow_execution_binding_sha256": (bundle.execution_binding.workflow_execution_binding_sha256),
        "workflow_execution_id": bundle.workflow_execution_id,
    }
    if resource.utc_semantics_assurance_receipt_sha256 is not None:
        payload["utc_semantics_assurance_receipt_sha256"] = resource.utc_semantics_assurance_receipt_sha256
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def mssql_prerequisite_authority(
    operation,
    resource: MssqlModelResourceAuthority,
) -> MssqlPrerequisiteAuthorityClaim:
    common = {
        "database": resource.target_resource_id.partition(".")[0],
        "deployment_id": operation.deployment_id,
        "effective_key_template_sha256": operation.effective_key_template_sha256,
        "environment": operation.environment,
        "model_definition_proof_sha256": operation.model_definition_proof_sha256,
        "model_unique_id": operation.model_unique_id,
        "mssql_control_database": resource.mssql_control_database,
        "mssql_control_schema": resource.mssql_control_schema,
        "mssql_image_schema": resource.mssql_image_schema,
        "mssql_target_authority_id": resource.mssql_target_authority_id,
        "release_id": operation.release_id,
        "route_certification_receipt_sha256": resource.route_certification_receipt_sha256,
        "scope_image_namespace_policy_sha256": resource.scope_image_namespace_policy_sha256,
        "sqlserver_lifecycle_policy_sha256": operation.sqlserver_lifecycle_policy_sha256,
        "writable_schema_sha256": resource.writable_schema_sha256,
    }
    receipts = {
        "ddl_freeze": resource.ddl_freeze_assurance_receipt_sha256,
        "writer_exclusivity": resource.writer_exclusivity_assurance_receipt_sha256,
    }
    if resource.utc_semantics_assurance_receipt_sha256 is not None:
        receipts["utc_semantics"] = resource.utc_semantics_assurance_receipt_sha256
    claims = tuple(
        MssqlRuntimeAssuranceClaim(
            assurance_kind=kind,
            receipt_sha256=digest,
            subject_json=json.dumps(
                {
                    "assurance_kind": kind,
                    "subject_type": "column" if kind == "utc_semantics" else "target",
                    **common,
                    **({"column_name": operation.event_time_column} if kind == "utc_semantics" else {}),
                },
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        for kind, digest in sorted(receipts.items())
    )
    return MssqlPrerequisiteAuthorityClaim(
        release_id=operation.release_id,
        deployment_id=operation.deployment_id,
        model_unique_id=operation.model_unique_id,
        route_certification_receipt_sha256=resource.route_certification_receipt_sha256,
        runtime_assurances=claims,
    )


def _replacement_actions(
    bundle: MssqlCanonicalAdmissionBundle,
) -> dict[str, ReplacementAction]:
    if bundle.workflow_plan.workflow_mode is not WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT:
        return {item.model_unique_id: ReplacementAction.BUILD_FRESH for item in bundle.operation_plans}
    if bundle.replacement_plan is None:
        raise ValueError("replacement strategy requires an authenticated replacement plan")
    return {item.action_id: item.action for item in bundle.replacement_plan.replacement_actions}


def _macro_key(value) -> dict[str, object]:
    source_type = value.source_type
    decimal = _DECIMAL.fullmatch(source_type)
    result: dict[str, object] = {"name": value.name}
    if decimal is not None:
        result.update(
            {
                "data_type": "decimal",
                "precision": int(decimal.group(1)),
                "scale": int(decimal.group(2)),
            }
        )
    else:
        result["data_type"] = source_type
    if value.domain_min is not None:
        result["domain_min"] = value.domain_min
    if value.domain_max is not None:
        result["domain_max"] = value.domain_max
    if value.utc_assurance_sha256 is not None:
        result["utc_assurance_sha256"] = value.utc_assurance_sha256
    return result


def _target_parts(target_resource_id: str) -> tuple[str, str, str]:
    parts = target_resource_id.split(".")
    if len(parts) != 3 or any(not item for item in parts):
        raise ValueError("target_resource_id must be database.schema.identifier")
    return parts[0], parts[1], parts[2]


def _relation(database: str, schema: str, identifier: str) -> dict[str, str]:
    return {"database": database, "identifier": identifier, "schema": schema}


def _dataclass_mapping(value: object) -> dict[str, object]:
    fields = getattr(value, "__dataclass_fields__", None)
    if not isinstance(fields, dict):
        raise TypeError("protected strategy value must be a dataclass")
    return {name: getattr(value, name) for name in fields}


__all__ = [
    "compose_admission",
    "compose_admission_from_record",
    "mssql_prerequisite_authority",
]
