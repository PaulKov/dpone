"""Readiness primitives for production-grade dpone pipelines."""

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
from dpone.readiness.managed import (
    ConnectorScaffoldService,
    ExecutionPlanService,
    PerformanceAdvisor,
    QualityService,
    RunArtifactWriter,
    StateInspectorService,
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
    "ConnectorScaffoldService",
    "ExecutionPlanService",
    "PerformanceAdvisor",
    "QualityService",
    "RunArtifactWriter",
    "StateInspectorService",
    "OnlineSchemaPlanner",
    "SchemaChangeLedger",
    "SchemaApprovalService",
    "SchemaHistoryRegistry",
    "SchemaNotificationService",
    "SchemaComparator",
    "SchemaEvolutionPolicy",
]
