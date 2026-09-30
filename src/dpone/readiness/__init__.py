"""Readiness primitives and on-demand managed service compatibility exports."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.readiness import managed as managed
from dpone.readiness.cdc import (
    CDCBackend,
    CDCConfig,
    CDCOffset,
    build_mssql_cdc_enable_sql,
    build_mssql_change_tracking_enable_sql,
    build_postgres_slot_sql,
)
from dpone.readiness.certification import CertificationMatrix, CertificationResult, CertificationStatus
from dpone.readiness.ddl_execution import GovernedDdlExecutor
from dpone.readiness.ddl_governance import (
    DdlCapabilityRegistry,
    DdlGovernancePolicy,
    OnlineSchemaPlanner,
    SchemaChangeLedger,
)
from dpone.readiness.observability import ErrorClassifier, PipelineRunMetrics
from dpone.readiness.resumability import JsonPartitionManifestStore, PartitionManifest, PartitionRunStatus
from dpone.readiness.schema_evolution import (
    ColumnDef,
    SchemaChange,
    SchemaComparator,
    SchemaEvolutionPolicy,
    SchemaPlan,
)
from dpone.readiness.schema_history import SchemaHistoryRegistry
from dpone.readiness.schema_notifications import SchemaNotificationService
from dpone.readiness.schema_workflows import ExpandContractService, SchemaApprovalService

if TYPE_CHECKING:
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

_MANAGED_EXPORTS = (
    "ConnectorScaffoldService",
    "ExecutionPlanService",
    "PerformanceAdvisor",
    "QualityService",
    "RunArtifactWriter",
    "StateInspectorService",
)


def __getattr__(name: str) -> Any:
    """Resolve only the historical package exports through their existing owner."""
    if name not in _MANAGED_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(managed, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Expose service names to discovery without initializing their implementations."""
    return sorted(set(globals()) | set(_MANAGED_EXPORTS))


__all__ = [
    "CDCBackend",
    "CDCConfig",
    "CDCOffset",
    "build_mssql_cdc_enable_sql",
    "build_mssql_change_tracking_enable_sql",
    "build_postgres_slot_sql",
    "CertificationMatrix",
    "CertificationResult",
    "CertificationStatus",
    "ColumnDef",
    "DdlCapabilityRegistry",
    "DdlGovernancePolicy",
    "GovernedDdlExecutor",
    "ExpandContractService",
    "SchemaChange",
    "SchemaPlan",
    "ErrorClassifier",
    "JsonPartitionManifestStore",
    "PartitionManifest",
    "PartitionRunStatus",
    "PipelineRunMetrics",
    *_MANAGED_EXPORTS,
    "OnlineSchemaPlanner",
    "SchemaChangeLedger",
    "SchemaApprovalService",
    "SchemaHistoryRegistry",
    "SchemaNotificationService",
    "SchemaComparator",
    "SchemaEvolutionPolicy",
]
