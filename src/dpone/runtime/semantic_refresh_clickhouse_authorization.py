"""Protected target/scope authorization for ClickHouse publication plans."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_clickhouse_authority import (
    ClickHousePublicationAuthority,
    SemanticRefreshClickHousePublicationAuthorityPort,
    clickhouse_operation_table_names,
)
from dpone.runtime.semantic_refresh_clickhouse_plan_factory import (
    ClickHouseHeadPublicationPlanFactory,
    ClickHousePlanFactoryError,
)

if TYPE_CHECKING:
    from dpone.runtime.semantic_refresh_clickhouse_models import (
        ClickHouseHeadPublicationPlan,
        ClickHousePreparedReceipt,
        ClickHousePreparePlan,
    )


class ClickHousePublicationAuthorityError(RuntimeError):
    """Raised before side effects when protected publication coordinates differ."""


class ClickHousePublicationAuthorization:
    """Load immutable authority and compare the full plan/head projection."""

    def __init__(self, authority: SemanticRefreshClickHousePublicationAuthorityPort) -> None:
        self._authority = authority

    def prepare(self, plan: ClickHousePreparePlan) -> ClickHousePublicationAuthority:
        record = self._load(plan)
        staging_table, shadow_table = clickhouse_operation_table_names(record.target_table, record.operation_id)
        expected = {
            "workflow_execution_id": record.workflow_execution_id,
            "operation_id": record.operation_id,
            "operation_plan_sha256": record.operation_plan_sha256,
            "workflow_plan_sha256": record.workflow_plan_sha256,
            "workflow_execution_binding_sha256": record.workflow_execution_binding_sha256,
            "attempt_binding_sha256": record.attempt_binding_sha256,
            "fence_epoch": record.fencing_epoch,
            "target_resource_id": record.target_resource_id,
            "target_authority_id": record.target_authority_id,
            "clickhouse_cluster_authority_id": record.clickhouse_cluster_authority_id,
            "database": record.database,
            "target_table": record.target_table,
            "staging_table": staging_table,
            "shadow_table": shadow_table,
            "scope_id": record.scope_id,
            "scope_start": record.scope_start,
            "scope_end": record.scope_end,
            "scope_revision": record.scope_revision,
            "expected_target_uuid": record.expected_target_uuid,
            "expected_schema_sha256": record.expected_schema_sha256,
            "expected_physical_sha256": record.expected_physical_sha256,
            "business_columns": record.business_columns,
            "effective_key_columns": record.effective_key_columns,
            "event_time_column": record.event_time_column,
            "max_staging_rows": record.max_staging_rows,
            "max_target_scope_rows": record.max_target_scope_rows,
            "max_staging_bytes": record.max_staging_bytes,
            "max_shadow_bytes": record.max_shadow_bytes,
            "max_retained_backup_bytes": record.max_retained_backup_bytes,
            "max_total_transient_bytes": record.max_total_transient_bytes,
        }
        if any(getattr(plan, field) != value for field, value in expected.items()):
            raise ClickHousePublicationAuthorityError("ClickHouse plan target authority differs")
        return record

    def commit(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        heads: ClickHouseHeadPublicationPlan,
    ) -> ClickHousePublicationAuthority:
        record = self.prepare(plan)
        try:
            expected = ClickHouseHeadPublicationPlanFactory.build(record, prepared)
        except ClickHousePlanFactoryError as exc:
            raise ClickHousePublicationAuthorityError("ClickHouse successor authority is invalid") from exc
        if heads != expected:
            raise ClickHousePublicationAuthorityError("ClickHouse scope/head authority differs")
        return record

    def _load(self, plan: ClickHousePreparePlan) -> ClickHousePublicationAuthority:
        try:
            return self._authority.load(
                plan.workflow_execution_binding_sha256,
                plan.operation_id,
            )
        except ClickHousePublicationAuthorityError:
            raise
        except Exception as exc:
            raise ClickHousePublicationAuthorityError("ClickHouse publication authority is unavailable") from exc


__all__ = [
    "ClickHousePublicationAuthorization",
    "ClickHousePublicationAuthorityError",
]
