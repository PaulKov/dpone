"""Release governance service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.approval_record import ApprovalRecordService
from dpone.ops.change_request import ChangeRequestService
from dpone.ops.deployment_record import DeploymentRecordService
from dpone.ops.env_drift import EnvironmentDriftService
from dpone.ops.post_deploy_verify import PostDeployVerifyService
from dpone.ops.release_close import ReleaseCloseService
from dpone.ops.release_promote import ReleasePromotionService


@dataclass(frozen=True, slots=True)
class ReleaseGovernanceCatalog:
    """Factory catalog for change, approval, deployment and promotion services."""

    @classmethod
    def default(cls) -> ReleaseGovernanceCatalog:
        return cls()

    def approval_record(self) -> ApprovalRecordService:
        return ApprovalRecordService()

    def change_request(self) -> ChangeRequestService:
        return ChangeRequestService()

    def deployment_record(self) -> DeploymentRecordService:
        return DeploymentRecordService()

    def environment_drift(self) -> EnvironmentDriftService:
        return EnvironmentDriftService()

    def post_deploy_verify(self) -> PostDeployVerifyService:
        return PostDeployVerifyService()

    def release_close(self) -> ReleaseCloseService:
        return ReleaseCloseService()

    def release_promotion(self) -> ReleasePromotionService:
        return ReleasePromotionService()


__all__ = ["ReleaseGovernanceCatalog"]
