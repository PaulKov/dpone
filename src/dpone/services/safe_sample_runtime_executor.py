"""Runtime-side safe sample execution skeleton.

The executor is intentionally dependency-injected: artifact fetching, temporary
target lifecycle, and future source-copy logic stay behind small protocols so
the orchestration can be tested without source systems, Vault, Airflow, or
Kubernetes.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

from dpone.services.safe_sample_data_copy_validation import (
    data_copy_contract_error,
    data_copy_errors,
    plan_row_budget_matches,
    safe_error_message,
    safe_mapping,
    validate_data_copy,
)
from dpone.services.safe_sample_data_copy_validation import (
    data_outcome as derive_data_outcome,
)
from dpone.services.safe_sample_route_attestation_evidence import safe_verified_route_receipt
from dpone.services.safe_sample_runtime_result import SafeSampleRuntimeExecutionResult
from dpone.services.safe_sample_source_request import SafeSampleSourceRequestBuilder
from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleError

if TYPE_CHECKING:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_policy import TemporaryTargetPlan
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor


class SafeSampleArtifactFetcher(Protocol):
    """Fetch pinned runtime artifacts for a safe sample execution plan."""

    def fetch(self, plan: SafeSampleExecutionPlan) -> Mapping[str, Any]:
        """Return secret-free init-fetch metadata for ``plan``."""


class SafeSampleDataCopier(Protocol):
    """Copy a bounded source sample into the prepared temporary target."""

    def copy(
        self,
        *,
        plan: SafeSampleExecutionPlan,
        target_plan: TemporaryTargetPlan,
        init_fetch: dict[str, Any],
    ) -> Mapping[str, Any]:
        """Return secret-free data-copy evidence."""


class FailClosedSafeSampleDataCopier:
    """Default data-copy boundary that refuses source IO until a certified copier is injected."""

    def copy(
        self,
        *,
        plan: SafeSampleExecutionPlan,
        target_plan: TemporaryTargetPlan,
        init_fetch: dict[str, Any],
    ) -> Mapping[str, Any]:
        del init_fetch
        source_request = SafeSampleSourceRequestBuilder().build(plan.policy_result, target_plan)
        return {
            "schema": "dpone.safe-sample-data-copy.v1",
            "status": "blocked",
            "source_request": source_request.to_dict(),
            "rows_read": 0,
            "rows_written": 0,
            "bytes_read": 0,
            "pii_policy": "masked",
            "errors": [
                _error(
                    "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED",
                    "Source sampling and copy into the temporary target are not implemented in this runtime slice.",
                )
            ],
        }


class SafeSampleRuntimeExecutor:
    """Run the safe sample runtime prelude and stop before source data copy."""

    def __init__(
        self,
        *,
        artifact_fetcher: SafeSampleArtifactFetcher,
        temporary_target_executor: TemporaryTargetLifecycleExecutor,
        data_copier: SafeSampleDataCopier | None = None,
        route_attestation_verification: Mapping[str, Any] | None = None,
    ) -> None:
        self._artifact_fetcher = artifact_fetcher
        self._temporary_target_executor = temporary_target_executor
        self._data_copier = data_copier or FailClosedSafeSampleDataCopier()
        self._route_attestation_verification = safe_verified_route_receipt(route_attestation_verification)

    def execute(self, plan: SafeSampleExecutionPlan) -> SafeSampleRuntimeExecutionResult:
        pinning = _pinning(plan)
        errors: list[dict[str, Any]] = [dict(error) for error in plan.blockers]
        if not plan_row_budget_matches(plan):
            errors.append(data_copy_contract_error())
        if errors or not plan.runnable:
            return _result(
                execution_status="failed",
                data_outcome="unknown",
                pinning=pinning,
                errors=errors,
                source_snapshot=_source_snapshot(plan),
                route_attestation_verification=self._route_attestation_verification,
            )
        target_plan = plan.temporary_target_plan
        if target_plan is None:
            return _result(
                execution_status="failed",
                data_outcome="unknown",
                pinning=pinning,
                errors=[
                    _error(
                        "DPONE_RUNTIME_TEMPORARY_TARGET_PLAN_MISSING",
                        "Temporary target plan is required before a safe sample run can execute.",
                    )
                ],
                source_snapshot=_source_snapshot(plan),
                route_attestation_verification=self._route_attestation_verification,
            )
        source_error = _source_snapshot_error(plan, target_plan)
        if source_error is not None:
            return _result(
                execution_status="failed",
                data_outcome="unknown",
                pinning=pinning,
                errors=[source_error],
                source_snapshot=_source_snapshot(plan),
                route_attestation_verification=self._route_attestation_verification,
            )

        init_fetch: dict[str, Any] | None = None
        prepare: dict[str, Any] | None = None
        data_copy: dict[str, Any] | None = None
        cleanup: dict[str, Any] | None = None
        try:
            init_fetch = safe_mapping(self._artifact_fetcher.fetch(plan))
            prepare = self._temporary_target_executor.prepare(target_plan).to_dict()
            data_copy = validate_data_copy(
                plan,
                target_plan,
                self._data_copier.copy(
                    plan=plan,
                    target_plan=target_plan,
                    init_fetch=init_fetch,
                ),
            )
            errors.extend(data_copy_errors(data_copy))
        except TemporaryTargetLifecycleError as exc:
            errors.append(_error(exc.code, safe_error_message(exc)))
        except Exception as exc:  # noqa: BLE001 - runtime evidence needs stable structured errors.
            errors.append(
                _error("DPONE_SAFE_SAMPLE_RUNTIME_FAILED", "safe sample runtime failed: " + safe_error_message(exc))
            )
        finally:
            cleanup = _cleanup_if_needed(self._temporary_target_executor, target_plan, prepared=prepare is not None)
            errors.extend(_cleanup_errors(cleanup))
        return _result(
            execution_status="failed" if errors else "succeeded",
            data_outcome="unknown",
            pinning=pinning,
            init_fetch=init_fetch,
            prepare=prepare,
            data_copy=data_copy,
            cleanup=cleanup,
            errors=errors,
            source_snapshot=_source_snapshot(plan),
            route_attestation_verification=self._route_attestation_verification,
        )


def _cleanup_if_needed(
    executor: TemporaryTargetLifecycleExecutor,
    target_plan: TemporaryTargetPlan,
    *,
    prepared: bool,
) -> dict[str, Any] | None:
    if not prepared or not target_plan.cleanup_required:
        return None
    try:
        return executor.cleanup(target_plan).to_dict()
    except TemporaryTargetLifecycleError as exc:
        return {
            "schema": "dpone.temporary-target-lifecycle.v1",
            "status": "cleanup_failed",
            "error": _error(exc.code, safe_error_message(exc)),
        }


def _cleanup_errors(cleanup: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(cleanup, Mapping) or cleanup.get("status") != "cleanup_failed":
        return []
    error = cleanup.get("error")
    return [dict(error)] if isinstance(error, Mapping) else []


def _result(
    *,
    execution_status: str,
    data_outcome: str,
    pinning: Mapping[str, Any],
    errors: list[dict[str, Any]],
    init_fetch: dict[str, Any] | None = None,
    prepare: dict[str, Any] | None = None,
    data_copy: dict[str, Any] | None = None,
    cleanup: dict[str, Any] | None = None,
    route_attestation_verification: dict[str, Any] | None = None,
    source_snapshot: dict[str, str] | None = None,
) -> SafeSampleRuntimeExecutionResult:
    return SafeSampleRuntimeExecutionResult(
        execution_status=execution_status,
        data_outcome=derive_data_outcome(data_copy, fallback=data_outcome),
        release_id=pinning.get("release_id"),
        deployment_id=pinning.get("deployment_id"),
        execution_mode="live_copy" if route_attestation_verification is not None else "local_handoff",
        deployment_identity=_deployment_identity(pinning),
        source_snapshot=source_snapshot,
        init_fetch=init_fetch,
        temporary_target_prepare=safe_mapping(prepare) if prepare is not None else None,
        data_copy=safe_mapping(data_copy) if data_copy is not None else None,
        temporary_target_cleanup=safe_mapping(cleanup) if cleanup is not None else None,
        errors=tuple(safe_mapping(error) for error in errors),
        route_attestation_verification=(
            safe_mapping(route_attestation_verification) if route_attestation_verification is not None else None
        ),
    )


def _source_snapshot(plan: SafeSampleExecutionPlan) -> dict[str, str] | None:
    return plan.source_snapshot.to_dict() if plan.source_snapshot is not None else None


def _source_snapshot_error(
    plan: SafeSampleExecutionPlan,
    target_plan: TemporaryTargetPlan,
) -> dict[str, Any] | None:
    snapshot = plan.source_snapshot
    if snapshot is None:
        return _error(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING",
            "Safe-sample runtime requires a canonical source snapshot; regenerate the execution plan.",
        )
    if snapshot.pipeline_id != target_plan.pipeline_id:
        return _error(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
            "Pipeline source snapshot identity does not match the selected target plan.",
        )
    return None


def _pinning(plan: SafeSampleExecutionPlan) -> dict[str, Any]:
    context = plan.deployment_context
    pinning = plan.to_dict().get("artifact_pinning")
    if not isinstance(pinning, dict):
        return _pinning_from_context(context)
    values = _pinning_from_context(context)
    for key in ("release_id", "deployment_id"):
        values[key] = str(pinning.get(key) or "") or values.get(key)
    return values


def _pinning_from_context(plan_context: object) -> dict[str, Any]:
    context = plan_context.to_dict() if hasattr(plan_context, "to_dict") else {}
    return {
        "release_id": _optional_string(context.get("release_id")),
        "deployment_id": _optional_string(context.get("deployment_id")),
        "binding_set_ref": _optional_string(context.get("binding_set_ref")),
        "connection_registry_ref": _optional_string(context.get("connection_registry_ref")),
        "credential_runtime_ref": _optional_string(context.get("credential_runtime_ref")),
        "runtime_image_digest": _optional_string(context.get("runtime_image_digest")),
        "airflow_bundle_ref": _optional_string(context.get("airflow_bundle_ref")),
        "workload_packs": list(context.get("workload_packs") or []),
        "runtime_artifact_delivery": dict(context.get("runtime_artifact_delivery") or {}),
    }


def _deployment_identity(pinning: Mapping[str, Any]) -> dict[str, Any]:
    return safe_mapping(
        {
            "release_id": pinning.get("release_id"),
            "deployment_id": pinning.get("deployment_id"),
            "binding_set_ref": pinning.get("binding_set_ref"),
            "connection_registry_ref": pinning.get("connection_registry_ref"),
            "credential_runtime_ref": pinning.get("credential_runtime_ref"),
            "runtime_image_digest": pinning.get("runtime_image_digest"),
            "airflow_bundle_ref": pinning.get("airflow_bundle_ref"),
            "workload_packs": pinning.get("workload_packs") or [],
            "runtime_artifact_delivery": pinning.get("runtime_artifact_delivery") or {},
        }
    )


def _optional_string(value: Any) -> str | None:
    text = str(value or "")
    return text or None


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_runtime_execution",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


__all__ = [
    "FailClosedSafeSampleDataCopier",
    "SafeSampleArtifactFetcher",
    "SafeSampleDataCopier",
    "SafeSampleRuntimeExecutionResult",
    "SafeSampleRuntimeExecutor",
]
