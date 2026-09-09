"""Secret-free execution planning for safe sample runs."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.safe_sample_policy import SafeSamplePolicyResult, TemporaryTargetPlan


from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.runtime_artifact_delivery import missing_init_fetch_delivery_paths
from dpone.services.safe_sample_deployment_context import (
    AirflowDeploymentContext,
    AirflowDeploymentContextResolution,
    ProjectionPayloadLoader,
    airflow_deployment_context_from_projection,
    load_current_airflow_deployment_context,
    resolve_airflow_deployment_context_from_projection,
    resolve_current_airflow_deployment_context,
)


@dataclass(frozen=True, slots=True)
class SafeSampleSourceSnapshot:
    """Secret-free canonical identity of the checked primary authoring source."""

    pipeline_id: str
    path: str
    sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "pipeline_id": self.pipeline_id,
            "path": self.path,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class SafeSampleExecutionPlan:
    sample_rows: int
    environment: str
    runnable: bool
    policy_result: SafeSamplePolicyResult
    temporary_target_plan: TemporaryTargetPlan | None
    deployment_context: AirflowDeploymentContext | None
    blockers: tuple[dict[str, Any], ...]
    source_snapshot: SafeSampleSourceSnapshot | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.safe-sample-execution-plan.v1",
            "sample_rows": self.sample_rows,
            "environment": self.environment,
            "runnable": self.runnable,
            "source_snapshot": self.source_snapshot.to_dict() if self.source_snapshot is not None else None,
            "policy_result": self.policy_result.to_dict(),
            "temporary_target_plan": self.temporary_target_plan.to_dict()
            if self.temporary_target_plan is not None
            else None,
            "deployment_context": self.deployment_context.to_dict() if self.deployment_context else None,
            "artifact_pinning": _artifact_pinning(self.temporary_target_plan, self.deployment_context),
            "blockers": [dict(blocker) for blocker in self.blockers],
        }


class SafeSampleExecutionPlanBuilder:
    """Compose the local, parse-safe plan that precedes runtime sample execution."""

    def build(
        self,
        *,
        sample_rows: int,
        environment: str,
        policy_result: SafeSamplePolicyResult,
        temporary_target_plan: TemporaryTargetPlan | None,
        deployment_context: AirflowDeploymentContext | None,
        preparation_error: Mapping[str, Any] | None = None,
        expected_release_id: str | None = None,
        expected_deployment_id: str | None = None,
        source_snapshot: SafeSampleSourceSnapshot | None = None,
    ) -> SafeSampleExecutionPlan:
        preparation_blockers = [dict(preparation_error)] if preparation_error is not None else []
        blockers = [
            *policy_result.errors,
            *_target_plan_blockers(temporary_target_plan),
            *preparation_blockers,
        ]
        if not preparation_blockers and policy_result.passed and temporary_target_plan is not None:
            blockers.extend(_deployment_blockers(deployment_context))
            blockers.extend(
                _deployment_identity_blockers(
                    deployment_context,
                    expected_release_id=expected_release_id,
                    expected_deployment_id=expected_deployment_id,
                )
            )
            blockers.extend(
                _deployment_environment_blockers(
                    requested_environment=environment,
                    deployment_context=deployment_context,
                )
            )
        return SafeSampleExecutionPlan(
            sample_rows=sample_rows,
            environment=environment,
            runnable=not blockers,
            policy_result=policy_result,
            temporary_target_plan=temporary_target_plan,
            deployment_context=deployment_context,
            blockers=tuple(blockers),
            source_snapshot=source_snapshot,
        )


def _target_plan_blockers(temporary_target_plan: TemporaryTargetPlan | None) -> list[dict[str, Any]]:
    if temporary_target_plan is not None:
        return []
    return [
        _error(
            "DPONE_RUNTIME_TEMPORARY_TARGET_PLAN_MISSING",
            "Temporary target plan is required before a safe sample run can execute.",
        )
    ]


def _deployment_blockers(deployment_context: AirflowDeploymentContext | None) -> list[dict[str, Any]]:
    if deployment_context is None or not deployment_context.release_id or not deployment_context.deployment_id:
        return [
            _error(
                "DPONE_DEPLOYMENT_CURRENT_NOT_FOUND",
                "Current Airflow deployment index with release_id and deployment_id was not found.",
            )
        ]
    missing_delivery_paths = missing_init_fetch_delivery_paths(deployment_context.runtime_artifact_delivery)
    if missing_delivery_paths:
        return [
            _error(
                "DPONE_AIRFLOW_INDEX_DELIVERY_INVALID",
                "Current Airflow deployment index has incomplete init_fetch delivery fields: "
                + ", ".join(missing_delivery_paths)
                + ".",
            )
        ]
    if not deployment_context.runnable:
        return [
            _error(
                "DPONE_DEPLOYMENT_NOT_RUNNABLE",
                "Current deployment is a non-runnable preview projection.",
            )
        ]
    return []


def _deployment_environment_blockers(
    *,
    requested_environment: str,
    deployment_context: AirflowDeploymentContext | None,
) -> list[dict[str, Any]]:
    if deployment_context is None or deployment_context.environment is None:
        return []
    if not deployment_context.runnable:
        return []
    if _normalized_environment(requested_environment) == _normalized_environment(deployment_context.environment):
        return []
    return [
        _error(
            "DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH",
            "Current Airflow deployment environment does not match the requested safe sample environment.",
        )
    ]


def _deployment_identity_blockers(
    deployment_context: AirflowDeploymentContext | None,
    *,
    expected_release_id: str | None,
    expected_deployment_id: str | None,
) -> list[dict[str, Any]]:
    if deployment_context is None or not expected_release_id or not expected_deployment_id:
        return []
    if (
        deployment_context.release_id == expected_release_id
        and deployment_context.deployment_id == expected_deployment_id
    ):
        return []
    return [
        _error(
            "DPONE_SAFE_SAMPLE_DEPLOYMENT_ID_MISMATCH",
            "Verified current deployment does not match the deployment prepared for this safe sample run.",
        )
    ]


def _normalized_environment(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if text in {"dev", "development", "local"}:
        return "development"
    if text in {"prod", "production"}:
        return "production"
    return text


def _artifact_pinning(
    temporary_target_plan: TemporaryTargetPlan | None,
    deployment_context: AirflowDeploymentContext | None,
) -> dict[str, Any] | None:
    if temporary_target_plan is None or deployment_context is None:
        return None
    release_id = deployment_context.release_id
    deployment_id = deployment_context.deployment_id
    if not release_id or not deployment_id:
        return None
    return {
        "release_id": release_id,
        "deployment_id": deployment_id,
        "pinned_workload_uri": f"cached://deployments/{deployment_id}/workloads/{temporary_target_plan.process}",
        "workload_packs": [
            {"id": str(pack.get("id") or ""), "sha256": str(pack.get("sha256") or "")}
            for pack in deployment_context.workload_packs
        ],
    }


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_execution_plan",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


# Re-export context helpers for the historical public import path.
__all__ = [
    "AirflowDeploymentContext",
    "AirflowDeploymentContextResolution",
    "ProjectionPayloadLoader",
    "SafeSampleExecutionPlan",
    "SafeSampleExecutionPlanBuilder",
    "SafeSampleSourceSnapshot",
    "airflow_deployment_context_from_projection",
    "load_current_airflow_deployment_context",
    "resolve_airflow_deployment_context_from_projection",
    "resolve_current_airflow_deployment_context",
]
