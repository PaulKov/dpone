"""Small release catalog sections used by the public release facade."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from dpone.ops.catalog_protocols import (
    AuditService,
    BuildService,
    CaptureService,
    CertifyService,
    CloseService,
    CollectService,
    CompareService,
    CreateService,
    EvaluateService,
    ExecuteService,
    FinalizeService,
    PlanService,
    PromoteService,
    RecordService,
    RouteExecutionLedgerService,
    RouteStatePromotionServicePort,
    RunService,
    VerifyService,
)
from dpone.ops.catalog_readiness import ReadinessOpsCatalog
from dpone.ops.catalog_release_gates import ReleaseGateCatalog
from dpone.ops.catalog_release_governance import ReleaseGovernanceCatalog
from dpone.ops.catalog_release_risk import ReleaseRiskCatalog
from dpone.ops.routes.refresh_execution_executor import RouteRefreshExecutor


class ReleaseGateOps:
    """Release gate service factories exposed by ``ReleaseOpsCatalog``."""

    __slots__ = ()

    gates: ReleaseGateCatalog

    def evidence_bundle_from_payload(self, payload: Mapping[str, Any]) -> Any:
        return self.gates.evidence_bundle_from_payload(payload)

    def go_live_gate(self) -> EvaluateService:
        return self.gates.go_live_gate()

    def policy(self) -> EvaluateService:
        return self.gates.policy()

    def release_gate(self) -> EvaluateService:
        return self.gates.release_gate()

    def release_rc_finalizer(self) -> FinalizeService:
        return self.gates.release_rc_finalizer()

    def release_rc_collector(self) -> CollectService:
        return cast(CollectService, self.gates.release_rc_collector())

    def release_orchestrator(self) -> RunService:
        return cast(RunService, self.gates.release_orchestrator())


class ReleaseGovernanceOps:
    """Release governance service factories exposed by ``ReleaseOpsCatalog``."""

    __slots__ = ()

    governance: ReleaseGovernanceCatalog

    def approval_record(self) -> RecordService:
        return cast(RecordService, self.governance.approval_record())

    def change_request(self) -> CreateService:
        return cast(CreateService, self.governance.change_request())

    def deployment_record(self) -> RecordService:
        return cast(RecordService, self.governance.deployment_record())

    def environment_drift(self) -> CompareService:
        return cast(CompareService, self.governance.environment_drift())

    def post_deploy_verify(self) -> VerifyService:
        return cast(VerifyService, self.governance.post_deploy_verify())

    def release_close(self) -> CloseService:
        return cast(CloseService, self.governance.release_close())

    def release_promotion(self) -> PromoteService:
        return cast(PromoteService, self.governance.release_promotion())


class ReleaseReadinessOps:
    """Production readiness service factories exposed by ``ReleaseOpsCatalog``."""

    __slots__ = ()

    readiness: ReadinessOpsCatalog

    @property
    def default_required_domains(self) -> tuple[str, ...]:
        return self.readiness.default_required_domains

    @property
    def default_industrial_domains(self) -> tuple[str, ...]:
        return self.readiness.default_industrial_domains

    def industrial_readiness(self) -> EvaluateService:
        return self.readiness.industrial_readiness()

    def production_maturity(self) -> EvaluateService:
        return self.readiness.production_maturity()

    def route_readiness(self) -> EvaluateService:
        return self.readiness.route_readiness()

    def route_schema_evolution(self) -> EvaluateService:
        return self.readiness.route_schema_evolution()

    def route_reconciliation_repair(self) -> EvaluateService:
        return self.readiness.route_reconciliation_repair()

    def route_data_quality(self) -> EvaluateService:
        return self.readiness.route_data_quality()

    def route_refresh_plan(self) -> PlanService:
        return cast(PlanService, self.readiness.route_refresh_plan())

    def route_refresh_execute(
        self,
        *,
        executor: RouteRefreshExecutor | None = None,
        executor_backend: str | None = None,
        executor_config_json: str | Path | None = None,
    ) -> ExecuteService:
        return cast(
            ExecuteService,
            self.readiness.route_refresh_execute(
                executor=executor,
                executor_backend=executor_backend,
                executor_config_json=executor_config_json,
            ),
        )

    def route_refresh_capture_snapshots(
        self,
        *,
        source_rows_json: str | Path | None = None,
        sink_rows_json: str | Path | None = None,
        executor_backend: str | None = None,
        executor_config_json: str | Path | None = None,
    ) -> CaptureService:
        return cast(
            CaptureService,
            self.readiness.route_refresh_capture_snapshots(
                source_rows_json=source_rows_json,
                sink_rows_json=sink_rows_json,
                executor_backend=executor_backend,
                executor_config_json=executor_config_json,
            ),
        )

    def route_refresh_verify(
        self,
        *,
        source_snapshot_json: str | Path | None = None,
        sink_snapshot_json: str | Path | None = None,
    ) -> VerifyService:
        return cast(
            VerifyService,
            self.readiness.route_refresh_verify(
                source_snapshot_json=source_snapshot_json,
                sink_snapshot_json=sink_snapshot_json,
            ),
        )

    def route_live_certification(self) -> BuildService:
        return cast(BuildService, self.readiness.route_live_certification())

    def route_rc_orchestrator(self) -> RunService:
        return cast(RunService, self.readiness.route_rc_orchestrator())

    def route_rc_executor(self) -> ExecuteService:
        return cast(ExecuteService, self.readiness.route_rc_executor())

    def route_release_gate(self) -> EvaluateService:
        return self.readiness.route_release_gate()

    def route_run_supervisor(self) -> EvaluateService:
        return self.readiness.route_run_supervisor()

    def route_execution_ledger(
        self,
        *,
        store_backend: str = "local_json",
        store_uri: str | None = None,
    ) -> RouteExecutionLedgerService:
        return cast(
            RouteExecutionLedgerService,
            self.readiness.route_execution_ledger(store_backend=store_backend, store_uri=store_uri),
        )

    def route_state_promotion(
        self,
        *,
        state_backend: str = "local_json",
        state_uri: str | None = None,
    ) -> RouteStatePromotionServicePort:
        return cast(
            RouteStatePromotionServicePort,
            self.readiness.route_state_promotion(state_backend=state_backend, state_uri=state_uri),
        )

    def route_certification_pack(self) -> BuildService:
        return cast(BuildService, self.readiness.route_certification_pack())

    def route_certify(self) -> CertifyService:
        return cast(CertifyService, self.readiness.route_certify())

    def route_certify_release(self) -> EvaluateService:
        return cast(EvaluateService, self.readiness.route_certify_release())

    def route_release_finalize(self) -> FinalizeService:
        return cast(FinalizeService, self.readiness.route_release_finalize())


class ReleaseRiskOps:
    """Release risk service factories exposed by ``ReleaseOpsCatalog``."""

    __slots__ = ()

    risk: ReleaseRiskCatalog

    def diff(self) -> CompareService:
        return cast(CompareService, self.risk.diff())

    def incident_pack(self) -> BuildService:
        return cast(BuildService, self.risk.incident_pack())

    def security_audit(self) -> AuditService:
        return cast(AuditService, self.risk.security_audit())

    def slo(self) -> EvaluateService:
        return self.risk.slo()


class ReleaseCdcOps:
    """CDC service catalog access exposed by ``ReleaseOpsCatalog``."""

    __slots__ = ()

    def cdc(self) -> Any:
        return import_module("dpone.ops.catalog_cdc").CdcOpsCatalog.default()


__all__ = [
    "ReleaseCdcOps",
    "ReleaseGateOps",
    "ReleaseGovernanceOps",
    "ReleaseReadinessOps",
    "ReleaseRiskOps",
]
