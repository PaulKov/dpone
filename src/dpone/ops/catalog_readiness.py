"""Readiness and certification operational service facade."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from dpone.ops.catalog_readiness_assessment import ReadinessAssessmentCatalog
from dpone.ops.catalog_readiness_certification import ReadinessCertificationCatalog
from dpone.ops.catalog_readiness_refresh import ReadinessRefreshCatalog
from dpone.ops.catalog_readiness_state import ReadinessStateCatalog


@dataclass(frozen=True, slots=True)
class ReadinessOpsCatalog:
    """Factory catalog for production readiness and route certification services."""

    assessment: ReadinessAssessmentCatalog = field(default_factory=ReadinessAssessmentCatalog.default)
    certification: ReadinessCertificationCatalog = field(default_factory=ReadinessCertificationCatalog.default)
    refresh: ReadinessRefreshCatalog = field(default_factory=ReadinessRefreshCatalog.default)
    state: ReadinessStateCatalog = field(default_factory=ReadinessStateCatalog.default)

    @classmethod
    def default(cls) -> ReadinessOpsCatalog:
        return cls()

    @property
    def default_required_domains(self) -> tuple[str, ...]:
        return self.assessment.default_required_domains

    @property
    def default_industrial_domains(self) -> tuple[str, ...]:
        return self.assessment.default_industrial_domains

    def industrial_readiness(self) -> object:
        return self.assessment.industrial_readiness()

    def production_maturity(self) -> object:
        return self.assessment.production_maturity()

    def route_readiness(self) -> object:
        return self.assessment.route_readiness()

    def route_schema_evolution(self) -> object:
        return self.assessment.route_schema_evolution()

    def route_reconciliation_repair(self) -> object:
        return self.assessment.route_reconciliation_repair()

    def route_data_quality(self) -> object:
        return self.assessment.route_data_quality()

    def route_refresh_plan(self) -> object:
        return self.refresh.route_refresh_plan()

    def route_refresh_execute(
        self,
        *,
        executor: object | None = None,
        executor_backend: str | None = None,
        executor_config_json: str | Path | None = None,
    ) -> object:
        return self.refresh.route_refresh_execute(
            executor=executor,
            executor_backend=executor_backend,
            executor_config_json=executor_config_json,
        )

    def route_refresh_capture_snapshots(
        self,
        *,
        source_rows_json: str | Path | None = None,
        sink_rows_json: str | Path | None = None,
        executor_backend: str | None = None,
        executor_config_json: str | Path | None = None,
    ) -> object:
        return self.refresh.route_refresh_capture_snapshots(
            source_rows_json=source_rows_json,
            sink_rows_json=sink_rows_json,
            executor_backend=executor_backend,
            executor_config_json=executor_config_json,
        )

    def route_refresh_verify(
        self,
        *,
        source_snapshot_json: str | Path | None = None,
        sink_snapshot_json: str | Path | None = None,
    ) -> object:
        return self.refresh.route_refresh_verify(
            source_snapshot_json=source_snapshot_json,
            sink_snapshot_json=sink_snapshot_json,
        )

    def route_live_certification(self) -> object:
        return self.certification.route_live_certification()

    def route_rc_orchestrator(self) -> object:
        return self.certification.route_rc_orchestrator()

    def route_rc_executor(self) -> object:
        return self.certification.route_rc_executor()

    def route_release_gate(self) -> object:
        return self.certification.route_release_gate()

    def route_run_supervisor(self) -> object:
        return self.certification.route_run_supervisor()

    def route_execution_ledger(
        self,
        *,
        store_backend: str = "local_json",
        store_uri: str | None = None,
    ) -> object:
        return self.state.route_execution_ledger(store_backend=store_backend, store_uri=store_uri)

    def route_state_promotion(
        self,
        *,
        state_backend: str = "local_json",
        state_uri: str | None = None,
    ) -> object:
        return self.state.route_state_promotion(state_backend=state_backend, state_uri=state_uri)

    def route_certification_pack(self) -> object:
        return self.certification.route_certification_pack()

    def route_certify(self) -> object:
        return self.certification.route_certify()

    def route_certify_release(self) -> object:
        return self.certification.route_certify_release()

    def route_release_finalize(self) -> object:
        return self.certification.route_release_finalize()


__all__ = ["ReadinessOpsCatalog"]
