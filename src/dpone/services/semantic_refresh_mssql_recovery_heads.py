"""Protected recovery-head service for complete-scope replay planning."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_mssql_authority_codec import authority_from_record
from dpone.ports.semantic_refresh_mssql_recovery_heads import (
    MssqlDurableRecoveryAuthority,
    MssqlRecoveryHeadLocator,
    MssqlRecoveryHeadReadRequest,
    SemanticRefreshMssqlRecoveryHeadStatePort,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_authority import (
        SemanticRefreshMssqlCanonicalAuthorityPort,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlRecoveryHeadService:
    """Resolve replay heads using only a protected predecessor run binding."""

    authority: SemanticRefreshMssqlCanonicalAuthorityPort
    state: SemanticRefreshMssqlRecoveryHeadStatePort

    def load(
        self,
        workflow_execution_binding_sha256: str,
    ) -> MssqlDurableRecoveryAuthority:
        """Return the durable completed summary and current head authority."""

        record = self.authority.load(workflow_execution_binding_sha256)
        bundle = authority_from_record(record)
        resources = {item.model_unique_id: item for item in bundle.model_resources}
        locators = tuple(
            sorted(
                MssqlRecoveryHeadLocator(
                    model_unique_id=operation.model_unique_id,
                    clickhouse_target_authority_id=resources[operation.model_unique_id].target_authority_id,
                    database_name=resources[operation.model_unique_id].publication_database,
                    target_table=resources[operation.model_unique_id].publication_target_table,
                    predecessor_operation_id=operation.operation_id,
                )
                for operation in bundle.operation_plans
            )
        )
        durable = self.state.load_recovery_authority(
            MssqlRecoveryHeadReadRequest(
                workflow_execution_id=bundle.workflow_execution_id,
                workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
                workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
                canonical_authority_sha256=bundle.authority_sha256,
                locators=locators,
            )
        )
        expected_models = tuple(item.model_unique_id for item in locators)
        if (
            tuple(item.model_unique_id for item in durable.predecessor_publications) != expected_models
            or tuple(item.model_unique_id for item in durable.target_heads) != expected_models
        ):
            raise ValueError("durable recovery head closure differs from canonical authority")
        return durable


__all__ = ["SemanticRefreshMssqlRecoveryHeadService"]
