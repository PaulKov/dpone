"""Release gate and evidence service factories."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone.ops.evidence import OpsEvidenceBundle
from dpone.ops.gate import GoLiveGateService
from dpone.ops.policy import OpsPolicyService
from dpone.ops.release_gate import ReleaseGateService
from dpone.ops.release_orchestrator import ReleaseOrchestratorService


@dataclass(frozen=True, slots=True)
class ReleaseGateCatalog:
    """Factory catalog for release gate and policy services."""

    @classmethod
    def default(cls) -> ReleaseGateCatalog:
        return cls()

    def evidence_bundle_from_payload(self, payload: Mapping[str, Any]) -> OpsEvidenceBundle:
        return OpsEvidenceBundle.from_dict(payload)

    def go_live_gate(self) -> GoLiveGateService:
        return GoLiveGateService()

    def policy(self) -> OpsPolicyService:
        return OpsPolicyService()

    def release_gate(self) -> ReleaseGateService:
        return ReleaseGateService()

    def release_rc_finalizer(self) -> Any:
        return import_module("dpone.ops.release_rc_finalizer").ReleaseRcFinalizerService()

    def release_rc_collector(self) -> Any:
        return import_module("dpone.ops.release_rc_collector").ReleaseRcCollectorService()

    def release_orchestrator(self) -> ReleaseOrchestratorService:
        return ReleaseOrchestratorService()


__all__ = ["ReleaseGateCatalog"]
