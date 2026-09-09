"""Serialization helpers for secret-free safe sample execution plans."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.services.safe_sample_execution_plan import (
    AirflowDeploymentContext,
    SafeSampleExecutionPlan,
    SafeSampleSourceSnapshot,
)
from dpone.services.safe_sample_policy import (
    SafeSamplePolicy,
    SafeSamplePolicyResult,
    SampleRunRequest,
    SampleSourceCapabilities,
    SampleTarget,
    TemporaryTargetPlan,
)


@dataclass(frozen=True, slots=True)
class SafeSampleExecutionPlanValidationIssue:
    code: str
    message: str
    path: str
    source: str


SafeSampleExecutionPlanPayloadValidator = Callable[
    [Mapping[str, Any]],
    Iterable[SafeSampleExecutionPlanValidationIssue],
]


@dataclass(frozen=True, slots=True)
class SafeSampleExecutionPlanValidationError(ValueError):
    """Raised when a persisted safe-sample execution plan violates its public contract."""

    issues: tuple[SafeSampleExecutionPlanValidationIssue, ...]

    def __str__(self) -> str:
        preview = "; ".join(f"{issue.path}: {issue.message}" for issue in self.issues[:5])
        suffix = f"; +{len(self.issues) - 5} more" if len(self.issues) > 5 else ""
        return f"safe sample execution plan is invalid: {preview}{suffix}"


def load_safe_sample_execution_plan(
    path: str | Path,
    *,
    validator: SafeSampleExecutionPlanPayloadValidator | None = None,
) -> SafeSampleExecutionPlan:
    """Load a ``dpone.safe-sample-execution-plan.v1`` JSON document."""

    plan_path = Path(path)
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("safe sample execution plan must be a JSON object")
    if validator is not None:
        _validate_public_contract(payload, validator)
    return safe_sample_execution_plan_from_dict(payload)


def safe_sample_execution_plan_from_dict(payload: Mapping[str, Any]) -> SafeSampleExecutionPlan:
    """Rehydrate a safe sample execution plan from its public JSON contract."""

    if payload.get("schema") != "dpone.safe-sample-execution-plan.v1":
        raise ValueError("unsupported safe sample execution plan schema")
    return SafeSampleExecutionPlan(
        sample_rows=_int(payload.get("sample_rows")),
        environment=str(payload.get("environment") or "development"),
        runnable=bool(payload.get("runnable")),
        policy_result=_policy_result(_mapping(payload.get("policy_result"))),
        temporary_target_plan=_temporary_target_plan_or_none(payload.get("temporary_target_plan")),
        deployment_context=_deployment_context_or_none(payload.get("deployment_context")),
        blockers=tuple(_error_mappings(payload.get("blockers"))),
        source_snapshot=_source_snapshot_or_none(payload.get("source_snapshot")),
    )


def _policy_result(payload: Mapping[str, Any]) -> SafeSamplePolicyResult:
    request = _mapping(payload.get("request"))
    policy = _mapping(payload.get("policy"))
    capabilities = _mapping(payload.get("capabilities"))
    return SafeSamplePolicyResult(
        passed=bool(payload.get("passed")),
        request=SampleRunRequest(
            sample_rows=_int(request.get("sample_rows")),
            target=SampleTarget(str(request.get("target") or SampleTarget.TEMPORARY.value)),
            environment=str(request.get("environment") or "development"),
        ),
        policy=SafeSamplePolicy(
            environment=str(policy.get("environment") or "development"),
            require_pushdown=bool(policy.get("require_pushdown")),
            allow_full_scan=bool(policy.get("allow_full_scan")),
            max_bytes=_int(policy.get("max_bytes")),
            timeout_seconds=_int(policy.get("timeout_seconds")),
        ),
        capabilities=SampleSourceCapabilities(
            supports_pushdown_sampling=_optional_bool(capabilities.get("supports_pushdown_sampling")),
            full_scan_required=_optional_bool(capabilities.get("full_scan_required")),
            estimated_read_bytes=_optional_int(capabilities.get("estimated_read_bytes")),
            proof=_optional_string(capabilities.get("proof")),
            mode=_optional_string(capabilities.get("mode")),
        ),
        errors=tuple(_error_mappings(payload.get("errors"))),
    )


def _temporary_target_plan_or_none(value: object) -> TemporaryTargetPlan | None:
    if not isinstance(value, Mapping):
        return None
    return TemporaryTargetPlan(
        mode=str(value.get("mode") or "temporary"),
        pipeline_id=str(value.get("pipeline_id") or "pipeline"),
        process=str(value.get("process") or value.get("pipeline_id") or "pipeline"),
        sink_type=str(value.get("sink_type") or ""),
        connection_ref=str(value.get("connection_ref") or ""),
        original_table=_string_mapping(value.get("original_table")),
        temporary_table=_string_mapping(value.get("temporary_table")),
        ttl_seconds=_int(value.get("ttl_seconds")),
        cleanup_required=bool(value.get("cleanup_required")),
        pii_policy=str(value.get("pii_policy") or "masked"),
    )


def _deployment_context_or_none(value: object) -> AirflowDeploymentContext | None:
    if not isinstance(value, Mapping):
        return None
    return AirflowDeploymentContext(
        release_id=str(value.get("release_id") or ""),
        deployment_id=str(value.get("deployment_id") or ""),
        environment=_optional_string(value.get("environment")),
        deployment_type=str(value.get("deployment_type") or "unknown"),
        runnable=bool(value.get("runnable")),
        runtime_artifact_delivery=_mapping(value.get("runtime_artifact_delivery")),
        workload_packs=tuple(_dict_mappings(value.get("workload_packs"))),
        index_path=str(value.get("index_path") or ""),
        deployment_path=_optional_string(value.get("deployment_path")),
        binding_set_ref=_optional_string(value.get("binding_set_ref")),
        connection_registry_ref=_optional_string(value.get("connection_registry_ref")),
        credential_runtime_ref=_optional_string(value.get("credential_runtime_ref")),
        runtime_image_digest=_optional_string(value.get("runtime_image_digest")),
        airflow_bundle_ref=_optional_string(value.get("airflow_bundle_ref")),
    )


def _source_snapshot_or_none(value: object) -> SafeSampleSourceSnapshot | None:
    if not isinstance(value, Mapping):
        return None
    pipeline_id = str(value.get("pipeline_id") or "")
    path = str(value.get("path") or "")
    sha256 = str(value.get("sha256") or "")
    if not pipeline_id or not path or not sha256:
        return None
    return SafeSampleSourceSnapshot(pipeline_id=pipeline_id, path=path, sha256=sha256)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_mapping(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def _dict_mappings(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _error_mappings(value: object) -> list[dict[str, Any]]:
    return _dict_mappings(value)


def _optional_string(value: object) -> str | None:
    text = str(value or "")
    return text or None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return _int(value)


def _int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "0").strip()
    return int(text or "0")


def _validate_public_contract(
    payload: Mapping[str, Any],
    validator: SafeSampleExecutionPlanPayloadValidator,
) -> None:
    issues = tuple(validator(payload))
    if issues:
        raise SafeSampleExecutionPlanValidationError(issues=issues)


__all__ = [
    "SafeSampleExecutionPlanPayloadValidator",
    "SafeSampleExecutionPlanValidationError",
    "SafeSampleExecutionPlanValidationIssue",
    "load_safe_sample_execution_plan",
    "safe_sample_execution_plan_from_dict",
]
