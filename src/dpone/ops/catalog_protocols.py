from __future__ import annotations

from typing import Any, Protocol


class BuildService(Protocol):
    def build(self, **kwargs: Any) -> Any: ...


class BootstrapService(Protocol):
    def bootstrap(self, **kwargs: Any) -> Any: ...


class CaptureService(Protocol):
    def capture(self, **kwargs: Any) -> Any: ...


class AuditService(Protocol):
    def audit(self, **kwargs: Any) -> Any: ...


class AppendService(Protocol):
    def append(self, **kwargs: Any) -> Any: ...


class ApplyService(Protocol):
    def apply(self, **kwargs: Any) -> Any: ...


class CertifyService(Protocol):
    def certify(self, **kwargs: Any) -> Any: ...


class CheckService(Protocol):
    def check(self, **kwargs: Any) -> Any: ...


class CatalogService(Protocol):
    def catalog(self) -> Any: ...


class CloseService(Protocol):
    def close(self, **kwargs: Any) -> Any: ...


class CollectService(Protocol):
    def collect(self, **kwargs: Any) -> Any: ...


class CompareService(Protocol):
    def compare(self, **kwargs: Any) -> Any: ...


class CreateService(Protocol):
    def create(self, **kwargs: Any) -> Any: ...


class DiagnoseService(Protocol):
    def diagnose(self, **kwargs: Any) -> Any: ...


class DiscoverService(Protocol):
    def discover(self, **kwargs: Any) -> Any: ...


class EvaluateService(Protocol):
    def evaluate(self, *args: Any, **kwargs: Any) -> Any: ...


class ExportService(Protocol):
    def export(self, **kwargs: Any) -> Any: ...


class ExecuteService(Protocol):
    def execute(self, **kwargs: Any) -> Any: ...


class FinalizeService(Protocol):
    def finalize(self, **kwargs: Any) -> Any: ...


class GenerateService(Protocol):
    def generate(self, **kwargs: Any) -> Any: ...


class InspectService(Protocol):
    def inspect(self, **kwargs: Any) -> Any: ...


class LoadPackageServicePort(Protocol):
    def start(self, **kwargs: Any) -> Any: ...

    def mark_committed(self, *args: Any, **kwargs: Any) -> Any: ...


class MaterializeService(Protocol):
    def materialize(self, **kwargs: Any) -> Any: ...


class PlanService(Protocol):
    def plan(self, **kwargs: Any) -> Any: ...


class PromoteService(Protocol):
    def promote(self, **kwargs: Any) -> Any: ...


class PublishService(Protocol):
    def publish(self, **kwargs: Any) -> Any: ...


class ReconcileService(Protocol):
    def reconcile(self, **kwargs: Any) -> Any: ...


class RecordService(Protocol):
    def record(self, **kwargs: Any) -> Any: ...


class RouteExecutionLedgerService(Protocol):
    def record_step(self, **kwargs: Any) -> Any: ...


class RouteStatePromotionServicePort(Protocol):
    def promote(self, **kwargs: Any) -> Any: ...


class ReplayStoreService(Protocol):
    def export(self, **kwargs: Any) -> Any: ...

    def replay(self, **kwargs: Any) -> Any: ...


class RenderService(Protocol):
    def render(self, **kwargs: Any) -> Any: ...


class RunService(Protocol):
    def run(self, **kwargs: Any) -> Any: ...


class RunMockContractService(Protocol):
    def run_mock_contract(self, **kwargs: Any) -> Any: ...


class VerifyService(Protocol):
    def verify(self, **kwargs: Any) -> Any: ...


__all__ = [
    "AuditService",
    "AppendService",
    "ApplyService",
    "BootstrapService",
    "BuildService",
    "CaptureService",
    "CatalogService",
    "CertifyService",
    "CheckService",
    "CloseService",
    "CollectService",
    "CompareService",
    "CreateService",
    "DiagnoseService",
    "DiscoverService",
    "EvaluateService",
    "ExportService",
    "ExecuteService",
    "FinalizeService",
    "GenerateService",
    "InspectService",
    "LoadPackageServicePort",
    "MaterializeService",
    "PlanService",
    "PromoteService",
    "PublishService",
    "ReconcileService",
    "RecordService",
    "ReplayStoreService",
    "RenderService",
    "RouteExecutionLedgerService",
    "RouteStatePromotionServicePort",
    "RunService",
    "RunMockContractService",
    "VerifyService",
]
