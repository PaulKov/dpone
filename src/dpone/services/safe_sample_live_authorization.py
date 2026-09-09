"""Runtime re-authorization for explicit safe-sample live execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan


from collections.abc import Iterable, Mapping

from dpone.services.safe_sample_capabilities import SourceSamplingCapabilityDetector
from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlanBuilder
from dpone.services.safe_sample_policy import (
    SafeSamplePolicyEvaluator,
    SafeSamplePolicySet,
    SampleRunRequest,
    SampleTarget,
)


class SafeSampleLiveAuthorizationError(ValueError):
    """Raised before live I/O when a persisted plan cannot be authorized."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class SafeSampleLiveExecutionAuthorizer:
    """Rebuild a live policy decision from pipeline facts and trusted inputs."""

    def authorize(
        self,
        plan: SafeSampleExecutionPlan,
        *,
        pipeline_source: Mapping[str, object],
        verified_route_ids: Iterable[str] = (),
    ) -> SafeSampleExecutionPlan:
        _validate_serialized_plan(plan)
        selected_pipeline = _pipeline_for_plan(plan, pipeline_source)
        capabilities = SourceSamplingCapabilityDetector(verified_route_ids=verified_route_ids).detect(selected_pipeline)
        request = SampleRunRequest(
            sample_rows=plan.sample_rows,
            target=SampleTarget.TEMPORARY,
            environment=plan.environment,
        )
        policy_result = SafeSamplePolicyEvaluator().evaluate(
            request,
            SafeSamplePolicySet.default().for_environment(plan.environment),
            capabilities,
        )
        if not policy_result.passed:
            codes = ", ".join(str(error.get("code") or "unknown") for error in policy_result.errors)
            raise SafeSampleLiveAuthorizationError(
                "DPONE_SAFE_SAMPLE_LIVE_POLICY_NOT_AUTHORIZED",
                f"Live safe-sample policy is not authorized ({codes or 'policy_failed'}).",
            )
        rebuilt = SafeSampleExecutionPlanBuilder().build(
            sample_rows=plan.sample_rows,
            environment=plan.environment,
            policy_result=policy_result,
            temporary_target_plan=plan.temporary_target_plan,
            deployment_context=plan.deployment_context,
            source_snapshot=plan.source_snapshot,
        )
        if not rebuilt.runnable or rebuilt.blockers:
            codes = ", ".join(str(error.get("code") or "unknown") for error in rebuilt.blockers)
            raise SafeSampleLiveAuthorizationError(
                "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID",
                f"Live safe-sample execution plan failed runtime validation ({codes or 'plan_not_runnable'}).",
            )
        return rebuilt


def _validate_serialized_plan(plan: SafeSampleExecutionPlan) -> None:
    request = plan.policy_result.request
    policy = plan.policy_result.policy
    target = plan.temporary_target_plan
    errors: list[str] = []
    if not plan.runnable or plan.blockers:
        errors.append("plan is not runnable")
    if plan.sample_rows <= 0 or request.sample_rows != plan.sample_rows:
        errors.append("sample row budgets do not match")
    if request.target is not SampleTarget.TEMPORARY:
        errors.append("target is not temporary")
    if _environment(request.environment) != _environment(plan.environment):
        errors.append("request environment does not match plan")
    if _environment(policy.environment) != _environment(plan.environment):
        errors.append("policy environment does not match plan")
    if target is None:
        errors.append("temporary target plan is missing")
    elif _normalized(target.mode) != "temporary":
        errors.append("target plan is not temporary")
    elif target.cleanup_required is not True:
        errors.append("temporary target cleanup is not required")
    elif target.pii_policy != "masked":
        errors.append("temporary target PII policy is not masked")
    if errors:
        raise SafeSampleLiveAuthorizationError(
            "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID",
            "Persisted live safe-sample policy is inconsistent: " + "; ".join(errors) + ".",
        )


def _pipeline_for_plan(
    plan: SafeSampleExecutionPlan,
    pipeline_source: Mapping[str, object],
) -> dict[str, object]:
    target = plan.temporary_target_plan
    if target is None:
        raise SafeSampleLiveAuthorizationError(
            "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID",
            "Persisted live safe-sample policy has no temporary target plan.",
        )
    processes = pipeline_source.get("processes")
    if not isinstance(processes, list):
        raise _pipeline_mismatch("pipeline processes are missing")
    process = next(
        (item for item in processes if isinstance(item, Mapping) and str(item.get("name") or "") == target.process),
        None,
    )
    if process is None:
        raise _pipeline_mismatch("temporary target process is missing")
    sink = process.get("sink")
    if not isinstance(sink, Mapping):
        raise _pipeline_mismatch("selected process sink is missing")
    if _normalized(sink.get("type")) != _normalized(target.sink_type):
        raise _pipeline_mismatch("sink type does not match")
    if str(sink.get("connection_ref") or "") != target.connection_ref:
        raise _pipeline_mismatch("sink connection_ref does not match")
    if _table(sink.get("table")) != target.original_table:
        raise _pipeline_mismatch("sink table does not match")
    return {**dict(pipeline_source), "processes": [dict(process)]}


def _pipeline_mismatch(reason: str) -> SafeSampleLiveAuthorizationError:
    return SafeSampleLiveAuthorizationError(
        "DPONE_SAFE_SAMPLE_TEMPORARY_TARGET_CONNECTION_MISMATCH",
        f"Pipeline source does not match the pinned temporary target ({reason}).",
    )


def _table(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {"schema": "", "name": ""}
    return {"schema": str(value.get("schema") or ""), "name": str(value.get("name") or "")}


def _normalized(value: object) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _environment(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"dev", "development", "local"}:
        return "development"
    if normalized in {"prod", "production"}:
        return "production"
    return normalized


__all__ = ["SafeSampleLiveAuthorizationError", "SafeSampleLiveExecutionAuthorizer"]
