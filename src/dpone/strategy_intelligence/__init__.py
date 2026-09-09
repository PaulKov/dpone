"""Lazy public facade for strategy intelligence services."""

from __future__ import annotations

from typing import Any

from dpone.lazy_exports import exported_dir, resolve_export

_EXPORTS: dict[str, str] = {
    "AdaptiveBatchController": "dpone.strategy_intelligence.adaptive:AdaptiveBatchController",
    "BatchObservation": "dpone.strategy_intelligence.adaptive:BatchObservation",
    "BigQueryReplayBackend": "dpone.strategy_intelligence.live_backends:BigQueryReplayBackend",
    "ClickHouseReplayBackend": "dpone.strategy_intelligence.live_backends:ClickHouseReplayBackend",
    "CertificationEvidenceBundleArtifact": (
        "dpone.strategy_intelligence.certification_bundle:CertificationEvidenceBundleArtifact"
    ),
    "CertificationEvidenceInput": "dpone.strategy_intelligence.certification_bundle:CertificationEvidenceInput",
    "FaultInjectionOperations": "dpone.strategy_intelligence.fault_injection:FaultInjectionOperations",
    "FaultInjectionSnapshot": "dpone.strategy_intelligence.fault_injection:FaultInjectionSnapshot",
    "FaultInjectionStage": "dpone.strategy_intelligence.fault_injection:FaultInjectionStage",
    "KafkaLiveReplayBackend": "dpone.strategy_intelligence.live_backends:KafkaLiveReplayBackend",
    "MssqlReplayBackend": "dpone.strategy_intelligence.live_backends:MssqlReplayBackend",
    "NativeBenchmarkCertificationPolicy": (
        "dpone.strategy_intelligence.benchmark_certification:NativeBenchmarkCertificationPolicy"
    ),
    "NativeBenchmarkCertificationResult": (
        "dpone.strategy_intelligence.benchmark_certification:NativeBenchmarkCertificationResult"
    ),
    "NativeBenchmarkCertificationService": (
        "dpone.strategy_intelligence.benchmark_certification:NativeBenchmarkCertificationService"
    ),
    "NativeFastPathCatalog": "dpone.strategy_intelligence.native_paths:NativeFastPathCatalog",
    "NativeFastPathPreflightService": "dpone.strategy_intelligence.preflight:NativeFastPathPreflightService",
    "NativeTransferFaultInjectionScenario": (
        "dpone.strategy_intelligence.fault_injection:NativeTransferFaultInjectionScenario"
    ),
    "NativeTransferFaultInjectionWorkflow": (
        "dpone.strategy_intelligence.fault_injection:NativeTransferFaultInjectionWorkflow"
    ),
    "NativeTransferPartitioningPlan": "dpone.strategy_intelligence.native_transfer:NativeTransferPartitioningPlan",
    "NativeTransferPlan": "dpone.strategy_intelligence.native_transfer:NativeTransferPlan",
    "NativeTransferPlanBuilder": "dpone.strategy_intelligence.native_transfer:NativeTransferPlanBuilder",
    "NativeTransferRequest": "dpone.strategy_intelligence.native_transfer:NativeTransferRequest",
    "NativeTransferResumeCertificationRequest": (
        "dpone.strategy_intelligence.resume_certification:NativeTransferResumeCertificationRequest"
    ),
    "NativeTransferResumeCertificationResult": (
        "dpone.strategy_intelligence.resume_certification:NativeTransferResumeCertificationResult"
    ),
    "NativeTransferResumeCertificationService": (
        "dpone.strategy_intelligence.resume_certification:NativeTransferResumeCertificationService"
    ),
    "NativeTransferResumeEvidenceWriter": (
        "dpone.strategy_intelligence.resume_certification:NativeTransferResumeEvidenceWriter"
    ),
    "PostgresReplayBackend": "dpone.strategy_intelligence.live_backends:PostgresReplayBackend",
    "RepairPlanService": "dpone.strategy_intelligence.repair:RepairPlanService",
    "RepairRequest": "dpone.strategy_intelligence.repair:RepairRequest",
    "ReplayBackendConnection": "dpone.strategy_intelligence.replay_clients:ReplayBackendConnection",
    "ReplayEvidenceWriter": "dpone.strategy_intelligence.replay_evidence:ReplayEvidenceWriter",
    "ReplayExecutionRequest": "dpone.strategy_intelligence.replay:ReplayExecutionRequest",
    "ReplayExecutionService": "dpone.strategy_intelligence.replay:ReplayExecutionService",
    "RuntimeKafkaReplayClient": "dpone.strategy_intelligence.replay_clients:RuntimeKafkaReplayClient",
    "RuntimeReplayBackendFactory": "dpone.strategy_intelligence.replay_clients:RuntimeReplayBackendFactory",
    "RuntimeSqlReplayClient": "dpone.strategy_intelligence.replay_clients:RuntimeSqlReplayClient",
    "StrategyAdvisor": "dpone.strategy_intelligence.advisor:StrategyAdvisor",
    "StrategyAutoCompiler": "dpone.strategy_intelligence.compiler:StrategyAutoCompiler",
    "StrategyCertificationArtifactWriter": (
        "dpone.strategy_intelligence.certification:StrategyCertificationArtifactWriter"
    ),
    "StrategyCertificationEvidenceBundleWriter": (
        "dpone.strategy_intelligence.certification_bundle:StrategyCertificationEvidenceBundleWriter"
    ),
    "StrategyCertificationMatrixService": (
        "dpone.strategy_intelligence.certification:StrategyCertificationMatrixService"
    ),
    "StrategyContext": "dpone.strategy_intelligence.advisor:StrategyContext",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_export(name, exports=_EXPORTS, namespace=globals(), module_name=__name__)


def __dir__() -> list[str]:
    return exported_dir(globals(), _EXPORTS)
