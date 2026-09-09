"""Benchmark and certification artifact service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.benchmark_baseline import BenchmarkBaselineService
from dpone.ops.benchmark_slo_gate import BenchmarkSloGateService
from dpone.ops.certification_pack import ConnectorCertificationPackService
from dpone.ops.live_certification import LiveCertificationAutomationService
from dpone.ops.performance_certification import PerformanceCertificationService


@dataclass(frozen=True, slots=True)
class ArtifactCertificationCatalog:
    """Factory catalog for benchmark and certification evidence services."""

    @classmethod
    def default(cls) -> ArtifactCertificationCatalog:
        return cls()

    def benchmark_baseline(self) -> BenchmarkBaselineService:
        return BenchmarkBaselineService()

    def benchmark_slo_gate(self) -> BenchmarkSloGateService:
        return BenchmarkSloGateService()

    def connector_certification_pack(self) -> ConnectorCertificationPackService:
        return ConnectorCertificationPackService()

    def live_certification(self) -> LiveCertificationAutomationService:
        return LiveCertificationAutomationService()

    def performance_certification(self) -> PerformanceCertificationService:
        return PerformanceCertificationService()


__all__ = ["ArtifactCertificationCatalog"]
