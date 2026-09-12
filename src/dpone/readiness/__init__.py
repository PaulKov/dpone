"""Lazy readiness primitives with stable public object and module identities."""

from __future__ import annotations

from importlib import import_module as _import_module
from typing import TYPE_CHECKING
from typing import Any as _Any

from dpone.lazy_exports import exported_dir as _exported_dir
from dpone.lazy_exports import resolve_export as _resolve_export

if TYPE_CHECKING:
    from dpone.readiness.cdc import (
        CDCBackend as CDCBackend,
    )
    from dpone.readiness.cdc import (
        CDCConfig as CDCConfig,
    )
    from dpone.readiness.cdc import (
        CDCOffset as CDCOffset,
    )
    from dpone.readiness.cdc import (
        build_mssql_cdc_enable_sql as build_mssql_cdc_enable_sql,
    )
    from dpone.readiness.cdc import (
        build_mssql_change_tracking_enable_sql as build_mssql_change_tracking_enable_sql,
    )
    from dpone.readiness.cdc import (
        build_postgres_slot_sql as build_postgres_slot_sql,
    )
    from dpone.readiness.certification import (
        CertificationMatrix as CertificationMatrix,
    )
    from dpone.readiness.certification import (
        CertificationResult as CertificationResult,
    )
    from dpone.readiness.certification import (
        CertificationStatus as CertificationStatus,
    )
    from dpone.readiness.ddl_execution import (
        GovernedDdlExecutor as GovernedDdlExecutor,
    )
    from dpone.readiness.ddl_governance import (
        DdlCapabilityRegistry as DdlCapabilityRegistry,
    )
    from dpone.readiness.ddl_governance import (
        DdlGovernancePolicy as DdlGovernancePolicy,
    )
    from dpone.readiness.ddl_governance import (
        OnlineSchemaPlanner as OnlineSchemaPlanner,
    )
    from dpone.readiness.ddl_governance import (
        SchemaChangeLedger as SchemaChangeLedger,
    )
    from dpone.readiness.managed import (
        ConnectorScaffoldService as ConnectorScaffoldService,
    )
    from dpone.readiness.managed import (
        ExecutionPlanService as ExecutionPlanService,
    )
    from dpone.readiness.managed import (
        PerformanceAdvisor as PerformanceAdvisor,
    )
    from dpone.readiness.managed import (
        QualityService as QualityService,
    )
    from dpone.readiness.managed import (
        RunArtifactWriter as RunArtifactWriter,
    )
    from dpone.readiness.managed import (
        StateInspectorService as StateInspectorService,
    )
    from dpone.readiness.observability import (
        ErrorClassifier as ErrorClassifier,
    )
    from dpone.readiness.observability import (
        PipelineRunMetrics as PipelineRunMetrics,
    )
    from dpone.readiness.resumability import (
        JsonPartitionManifestStore as JsonPartitionManifestStore,
    )
    from dpone.readiness.resumability import (
        PartitionManifest as PartitionManifest,
    )
    from dpone.readiness.resumability import (
        PartitionRunStatus as PartitionRunStatus,
    )
    from dpone.readiness.schema_evolution import (
        ColumnDef as ColumnDef,
    )
    from dpone.readiness.schema_evolution import (
        SchemaChange as SchemaChange,
    )
    from dpone.readiness.schema_evolution import (
        SchemaComparator as SchemaComparator,
    )
    from dpone.readiness.schema_evolution import (
        SchemaEvolutionPolicy as SchemaEvolutionPolicy,
    )
    from dpone.readiness.schema_evolution import (
        SchemaPlan as SchemaPlan,
    )
    from dpone.readiness.schema_history import (
        SchemaHistoryRegistry as SchemaHistoryRegistry,
    )
    from dpone.readiness.schema_notifications import (
        SchemaNotificationService as SchemaNotificationService,
    )
    from dpone.readiness.schema_workflows import (
        ExpandContractService as ExpandContractService,
    )
    from dpone.readiness.schema_workflows import (
        SchemaApprovalService as SchemaApprovalService,
    )

_EXPORTS: dict[str, str] = {
    "CDCBackend": "dpone.readiness.cdc:CDCBackend",
    "CDCConfig": "dpone.readiness.cdc:CDCConfig",
    "CDCOffset": "dpone.readiness.cdc:CDCOffset",
    "build_mssql_cdc_enable_sql": "dpone.readiness.cdc:build_mssql_cdc_enable_sql",
    "build_mssql_change_tracking_enable_sql": "dpone.readiness.cdc:build_mssql_change_tracking_enable_sql",
    "build_postgres_slot_sql": "dpone.readiness.cdc:build_postgres_slot_sql",
    "CertificationMatrix": "dpone.readiness.certification:CertificationMatrix",
    "CertificationResult": "dpone.readiness.certification:CertificationResult",
    "CertificationStatus": "dpone.readiness.certification:CertificationStatus",
    "ColumnDef": "dpone.readiness.schema_evolution:ColumnDef",
    "DdlCapabilityRegistry": "dpone.readiness.ddl_governance:DdlCapabilityRegistry",
    "DdlGovernancePolicy": "dpone.readiness.ddl_governance:DdlGovernancePolicy",
    "GovernedDdlExecutor": "dpone.readiness.ddl_execution:GovernedDdlExecutor",
    "ExpandContractService": "dpone.readiness.schema_workflows:ExpandContractService",
    "SchemaChange": "dpone.readiness.schema_evolution:SchemaChange",
    "SchemaPlan": "dpone.readiness.schema_evolution:SchemaPlan",
    "ErrorClassifier": "dpone.readiness.observability:ErrorClassifier",
    "JsonPartitionManifestStore": "dpone.readiness.resumability:JsonPartitionManifestStore",
    "PartitionManifest": "dpone.readiness.resumability:PartitionManifest",
    "PartitionRunStatus": "dpone.readiness.resumability:PartitionRunStatus",
    "PipelineRunMetrics": "dpone.readiness.observability:PipelineRunMetrics",
    "ConnectorScaffoldService": "dpone.readiness.managed:ConnectorScaffoldService",
    "ExecutionPlanService": "dpone.readiness.managed:ExecutionPlanService",
    "PerformanceAdvisor": "dpone.readiness.managed:PerformanceAdvisor",
    "QualityService": "dpone.readiness.managed:QualityService",
    "RunArtifactWriter": "dpone.readiness.managed:RunArtifactWriter",
    "StateInspectorService": "dpone.readiness.managed:StateInspectorService",
    "OnlineSchemaPlanner": "dpone.readiness.ddl_governance:OnlineSchemaPlanner",
    "SchemaChangeLedger": "dpone.readiness.ddl_governance:SchemaChangeLedger",
    "SchemaApprovalService": "dpone.readiness.schema_workflows:SchemaApprovalService",
    "SchemaHistoryRegistry": "dpone.readiness.schema_history:SchemaHistoryRegistry",
    "SchemaNotificationService": "dpone.readiness.schema_notifications:SchemaNotificationService",
    "SchemaComparator": "dpone.readiness.schema_evolution:SchemaComparator",
    "SchemaEvolutionPolicy": "dpone.readiness.schema_evolution:SchemaEvolutionPolicy",
}

# Preserve the submodule attributes supplied by the former eager initializer.
# Additional explicit submodule imports keep Python's normal import fallback.
_MODULE_EXPORTS = {
    name: f"dpone.readiness.{name}"
    for name in (
        "cdc",
        "certification",
        "ddl_execution",
        "ddl_governance",
        "ddl_policy_decisions",
        "managed",
        "managed_artifacts",
        "managed_bulk_path",
        "managed_models",
        "managed_native_projection",
        "managed_native_transfer_plan",
        "managed_performance",
        "managed_plan_warnings",
        "managed_planning",
        "managed_planning_r1",
        "managed_planning_snapshot",
        "managed_quality",
        "managed_scaffold",
        "managed_source_impact",
        "managed_state",
        "managed_templates",
        "managed_utils",
        "mssql_native_planning",
        "native_snapshot_planning",
        "observability",
        "physical_design",
        "physical_design_models",
        "physical_reconciliation_approval",
        "postgres_mssql_correctness_profile",
        "postgres_mssql_correctness_route",
        "resolved_process_route",
        "resumability",
        "schema_contracts",
        "schema_evolution",
        "schema_evolution_ddl",
        "schema_evolution_models",
        "schema_history",
        "schema_notifications",
        "schema_type_compatibility",
        "schema_workflows",
        "studio_ui_assets",
        "target_type_resolvers",
    )
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> _Any:
    if name in _MODULE_EXPORTS:
        module = _import_module(_MODULE_EXPORTS[name])
        globals()[name] = module
        return module
    return _resolve_export(name, exports=_EXPORTS, namespace=globals(), module_name=__name__)


def __dir__() -> list[str]:
    return _exported_dir(globals(), _EXPORTS | _MODULE_EXPORTS)
