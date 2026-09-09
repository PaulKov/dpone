"""Protected-authority adapters for ClickHouse publication composition."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_artifact_store import operation_artifact_prefix
from dpone.ports.semantic_refresh_clickhouse_authority import (
    ClickHousePublicationAuthority,
    clickhouse_authority_rows_sha256,
    clickhouse_plain_mergetree_physical_authority_rows,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql import (
        SemanticRefreshMssqlPrerequisiteAuthorityPort,
    )
    from dpone.ports.semantic_refresh_mssql_authority import (
        SemanticRefreshMssqlCanonicalAuthorityPort,
        SemanticRefreshMssqlProtectedAuthorityPort,
    )


@dataclass(frozen=True)
class StaticSemanticRefreshClickHousePublicationAuthority:
    """Immutable authority adapter for tests and protected composition inputs."""

    records: tuple[ClickHousePublicationAuthority, ...]

    def load(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> ClickHousePublicationAuthority:
        matches = tuple(
            record
            for record in self.records
            if record.workflow_execution_binding_sha256 == workflow_execution_binding_sha256
            and record.operation_id == operation_id
        )
        if len(matches) != 1:
            raise ValueError("protected ClickHouse publication authority is absent or ambiguous")
        return matches[0]


@dataclass(frozen=True)
class MssqlProtectedSemanticRefreshClickHouseAuthority:
    """Derive ClickHouse coordinates from authenticated MSSQL operation authority."""

    protected: SemanticRefreshMssqlProtectedAuthorityPort
    canonical: SemanticRefreshMssqlCanonicalAuthorityPort
    prerequisites: SemanticRefreshMssqlPrerequisiteAuthorityPort

    def load(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> ClickHousePublicationAuthority:
        value = self.protected.load_operation(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        if value.prerequisite_authority is None:
            raise ValueError("protected publication prerequisite authority is absent")
        self.prerequisites.require_current(value.prerequisite_authority)
        canonical = self.canonical.load(workflow_execution_binding_sha256)
        if canonical.workflow_execution_binding_sha256 != value.workflow_execution_binding_sha256:
            raise ValueError("canonical and protected publication authorities differ")
        schema_rows = tuple(
            (column.name, _clickhouse_column_type(column.target_type, nullable=column.nullable), ordinal, "", "", "")
            for ordinal, column in enumerate(value.writable_columns, start=1)
        )
        effective_keys = tuple(
            column.name for column in value.writable_columns if column.writable_role != "MUTABLE_VALUE"
        )
        event_time_columns = tuple(
            column.name for column in value.writable_columns if column.writable_role == "EFFECTIVE_KEY_EVENT_TIME"
        )
        if len(event_time_columns) != 1:
            raise ValueError("protected publication event-time column is absent or ambiguous")
        sorting_key = ", ".join(effective_keys)
        return ClickHousePublicationAuthority(
            authority_sha256=canonical.authority_sha256,
            workflow_execution_id=value.workflow_execution_id,
            operation_id=value.operation_id,
            operation_plan_sha256=value.operation_plan_sha256,
            workflow_plan_sha256=value.workflow_plan_sha256,
            workflow_execution_binding_sha256=value.workflow_execution_binding_sha256,
            attempt_binding_sha256=value.attempt_binding_sha256,
            fencing_epoch=value.fencing_epoch,
            owner_id=value.owner_id,
            guard_resource_id=value.guard_resource_id,
            guard_status=value.guard_status,
            target_resource_id=value.target_resource_id,
            target_authority_id=value.target_authority_id,
            clickhouse_cluster_authority_id=value.clickhouse_cluster_authority_id,
            database=value.publication_database,
            target_table=value.publication_target_table,
            scope_id=value.publication_scope_id,
            scope_start=value.scope_start,
            scope_end=value.scope_end,
            scope_revision=value.scope_revision,
            target_predecessor_generation_id=value.target_predecessor_generation_id,
            scope_predecessor_operation_id=value.scope_predecessor_operation_id,
            predecessor_target_generation=value.predecessor_target_generation,
            predecessor_target_uuid=value.predecessor_target_uuid,
            predecessor_target_operation_id=value.predecessor_target_operation_id,
            predecessor_scope_revision=value.predecessor_scope_revision,
            predecessor_checkpoint_sha256=value.predecessor_checkpoint_sha256,
            predecessor_checkpoint_operation_id=value.predecessor_checkpoint_operation_id,
            predecessor_checkpoint_version=value.predecessor_checkpoint_version,
            expected_target_uuid=value.clickhouse_target_uuid,
            expected_schema_sha256=clickhouse_authority_rows_sha256(schema_rows),
            expected_physical_sha256=clickhouse_authority_rows_sha256(
                clickhouse_plain_mergetree_physical_authority_rows(sorting_key)
            ),
            business_columns=tuple(column.name for column in value.writable_columns),
            effective_key_columns=effective_keys,
            event_time_column=event_time_columns[0],
            effective_key_mapping_sha256=value.effective_key_mapping_sha256,
            route_certification_receipt_sha256=value.route_certification_receipt_sha256,
            artifact_prefix=operation_artifact_prefix(
                value.artifact_authority.artifact_prefix,
                value.operation_id,
            ),
            artifact_provider=value.artifact_authority.provider,
            encryption_policy_sha256=value.artifact_authority.encryption_policy_sha256,
            retention_policy_sha256=value.artifact_authority.retention_policy_sha256,
            max_artifact_bytes=value.artifact_authority.max_artifact_bytes,
            max_staging_rows=value.resource_policy.max_after_image_rows,
            max_target_scope_rows=value.resource_policy.max_target_scope_rows,
            max_staging_bytes=value.resource_policy.max_clickhouse_staging_bytes,
            max_shadow_bytes=value.resource_policy.max_clickhouse_shadow_bytes,
            max_retained_backup_bytes=value.resource_policy.max_clickhouse_retained_backup_bytes,
            max_total_transient_bytes=value.resource_policy.max_clickhouse_total_transient_bytes,
        )


def _clickhouse_column_type(target_type: str, *, nullable: bool) -> str:
    """Project the protected nullability flag into system.columns type text."""

    already_nullable = target_type.startswith("Nullable(") and target_type.endswith(")")
    if nullable and not already_nullable:
        return f"Nullable({target_type})"
    if not nullable and already_nullable:
        raise ValueError("protected ClickHouse target type contradicts column nullability")
    return target_type


__all__ = [
    "MssqlProtectedSemanticRefreshClickHouseAuthority",
    "StaticSemanticRefreshClickHousePublicationAuthority",
]
