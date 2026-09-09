"""Self-service composition and error mapping for runtime Pod retention."""

from __future__ import annotations

import shlex
from typing import Literal

from dpone_airflow_pack.provider_execution_contract import (
    RUNTIME_POD_CONTRACT_KEY,
    RUNTIME_POD_CONTRACT_VALUE,
    RUNTIME_POD_LABEL_SELECTOR,
    RUNTIME_POD_MANAGED_BY_KEY,
    RUNTIME_POD_MANAGED_BY_VALUE,
    WORKLOAD_ID_METADATA_KEY,
)

from dpone.adapters.kubernetes_airflow_runtime_pod_retention import (
    build_kubernetes_airflow_runtime_pod_retention_adapter,
)
from dpone.contracts.airflow_runtime_pod_metadata import AirflowRuntimePodOwnershipContract
from dpone.contracts.airflow_runtime_pod_retention import (
    KUBE_AUTH_MODES,
    MINIMUM_AGE_MAX,
    MINIMUM_AGE_MIN,
    PAGE_SIZE_MAX,
    AirflowRuntimePodRetentionApplyRequest,
    AirflowRuntimePodRetentionError,
    AirflowRuntimePodRetentionPlanRequest,
    validate_apply_kubernetes_authority,
)
from dpone.ports.airflow_runtime_pod_retention import AirflowRuntimePodRetentionEvidencePublisher
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error, manual_fix
from dpone.services.airflow_runtime_pod_retention import AirflowRuntimePodRetentionService

_SECURITY_CODES = frozenset(
    {
        "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CONFIRMATION_REQUIRED",
        "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACTOR_MISSING",
        "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACTOR_UNAUTHORIZED",
        "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACCESS_DENIED",
        "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INVENTORY_INVALID",
        "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_METADATA_ONLY_UNSUPPORTED",
    }
)
_OWNERSHIP = AirflowRuntimePodOwnershipContract(
    managed_by_key=RUNTIME_POD_MANAGED_BY_KEY,
    managed_by_value=RUNTIME_POD_MANAGED_BY_VALUE,
    runtime_contract_key=RUNTIME_POD_CONTRACT_KEY,
    runtime_contract_value=RUNTIME_POD_CONTRACT_VALUE,
    workload_id_key=WORKLOAD_ID_METADATA_KEY,
)


def runtime_pod_retention_plan_command_result(
    *,
    namespace: str,
    minimum_age_seconds: int,
    page_size: int,
    auth_mode: str,
    kube_context: str | None,
) -> SelfServiceResult:
    try:
        request = AirflowRuntimePodRetentionPlanRequest(namespace, minimum_age_seconds, page_size)
        adapter = _adapter(auth_mode=auth_mode, kube_context=kube_context)
        report = AirflowRuntimePodRetentionService(
            inventory=adapter,
            ownership=_OWNERSHIP,
        ).plan(request)
        passed = report["status"] != "needs_attention"
        return SelfServiceResult(passed=passed, details=report, exit_code=0 if passed else 1)
    except Exception as exc:  # noqa: BLE001 - public boundary redacts vendor failures.
        return runtime_pod_retention_error_result(
            exc,
            namespace=namespace,
            minimum_age_seconds=minimum_age_seconds,
            page_size=page_size,
            auth_mode=auth_mode,
            kube_context=kube_context,
        )


def runtime_pod_retention_apply_command_result(
    *,
    namespace: str,
    minimum_age_seconds: int,
    page_size: int,
    max_delete_count: int,
    actor: str,
    allowed_actors: tuple[str, ...],
    confirm_delete: bool,
    auth_mode: str,
    kube_context: str | None,
    evidence: AirflowRuntimePodRetentionEvidencePublisher,
) -> SelfServiceResult:
    try:
        validate_apply_kubernetes_authority(auth_mode, kube_context)
        request = AirflowRuntimePodRetentionApplyRequest(
            namespace=namespace,
            minimum_age_seconds=minimum_age_seconds,
            page_size=page_size,
            max_delete_count=max_delete_count,
            actor=actor,
            allowed_actors=allowed_actors,
            confirm_delete=confirm_delete,
            kube_auth_mode=auth_mode,
        )
        request.require_authorized()
        adapter = _adapter(auth_mode=auth_mode, kube_context=kube_context)
        report = AirflowRuntimePodRetentionService(
            inventory=adapter,
            ownership=_OWNERSHIP,
            deletion=adapter,
            credentials=adapter,
            evidence=evidence,
        ).apply(request)
        status = report["status"]
        access_denied = any(
            item.get("error_code") == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACCESS_DENIED" for item in report["items"]
        )
        return SelfServiceResult(
            passed=status == "ok",
            details=report,
            exit_code=4 if access_denied else 0 if status == "ok" else 1 if status == "partial" else 3,
        )
    except Exception as exc:  # noqa: BLE001 - public boundary redacts vendor failures.
        return runtime_pod_retention_error_result(
            exc,
            namespace=namespace,
            minimum_age_seconds=minimum_age_seconds,
            page_size=page_size,
            auth_mode=auth_mode,
            kube_context=kube_context,
        )


def runtime_pod_retention_error_result(
    exc: BaseException,
    *,
    namespace: str,
    retry_operation: Literal["plan", "render"] = "plan",
    minimum_age_seconds: int | None = None,
    page_size: int | None = None,
    auth_mode: str | None = None,
    kube_context: str | None = None,
) -> SelfServiceResult:
    if isinstance(exc, AirflowRuntimePodRetentionError):
        code = exc.code
        message = str(exc)
    elif isinstance(exc, ValueError):
        code = "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INPUT_INVALID"
        message = str(exc)
    else:
        code = "DPONE_INTERNAL_AIRFLOW_RUNTIME_POD_RETENTION_FAILED"
        message = "Airflow runtime Pod retention failed unexpectedly."
    if code == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_SDK_UNAVAILABLE":
        fixes = [manual_fix("install_kubernetes_support", command="python -m pip install 'dpone[kubernetes]'")]
    elif retry_operation == "render":
        fixes = [
            manual_fix(
                "review_runtime_pod_retention_render_help",
                command="dpone airflow runtime-pod-retention-render --help",
            )
        ]
    else:
        fixes = [
            manual_fix(
                "rerun_runtime_pod_retention_plan",
                command=_plan_retry_command(
                    namespace=namespace,
                    minimum_age_seconds=minimum_age_seconds,
                    page_size=page_size,
                    auth_mode=auth_mode,
                    kube_context=kube_context,
                ),
            )
        ]
    return SelfServiceResult(
        passed=False,
        errors=(dpone_error(code, message, stage="airflow_runtime_pod_retention", fixes=fixes),),
        exit_code=_exit_code(code),
    )


def _plan_retry_command(
    *,
    namespace: str,
    minimum_age_seconds: int | None,
    page_size: int | None,
    auth_mode: str | None,
    kube_context: str | None,
) -> str:
    try:
        AirflowRuntimePodRetentionPlanRequest(namespace, 86_400, 500)
    except AirflowRuntimePodRetentionError:
        namespace = "airflow-example"
    if (
        minimum_age_seconds is None
        or isinstance(minimum_age_seconds, bool)
        or not MINIMUM_AGE_MIN <= minimum_age_seconds <= MINIMUM_AGE_MAX
    ):
        minimum_age_seconds = 86_400
    if page_size is None or isinstance(page_size, bool) or not 1 <= page_size <= PAGE_SIZE_MAX:
        page_size = 500
    if auth_mode not in KUBE_AUTH_MODES:
        auth_mode = "kubeconfig" if kube_context else "auto"
    if auth_mode == "in-cluster" and kube_context is not None:
        kube_context = None
    command = ["dpone", "airflow", "runtime-pod-retention-plan", "--namespace", namespace]
    command.extend(("--minimum-age-seconds", str(minimum_age_seconds)))
    command.extend(("--page-size", str(page_size)))
    command.extend(("--kube-auth", auth_mode))
    if kube_context is not None:
        command.extend(("--kube-context", kube_context))
    return shlex.join(command)


def _adapter(*, auth_mode: str, kube_context: str | None):
    if auth_mode == "in-cluster" and kube_context:
        raise AirflowRuntimePodRetentionError(
            "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INPUT_INVALID",
            "--kube-context cannot be combined with --kube-auth in-cluster.",
        )
    try:
        return build_kubernetes_airflow_runtime_pod_retention_adapter(
            auth_mode=auth_mode,
            kube_context=kube_context,
            label_selector=RUNTIME_POD_LABEL_SELECTOR,
        )
    except Exception as exc:  # noqa: BLE001 - mapper below consumes safe adapter exceptions.
        from dpone.ports.airflow_runtime_pod_retention import AirflowRuntimePodRetentionApiError
        from dpone.services.airflow_runtime_pod_retention import error_from_api

        if isinstance(exc, AirflowRuntimePodRetentionApiError):
            raise error_from_api(exc) from None
        raise


def _exit_code(code: str) -> int:
    if code.endswith("_INPUT_INVALID"):
        return 2
    if code in _SECURITY_CODES:
        return 4
    if code.endswith(
        (
            "_SDK_UNAVAILABLE",
            "_DEPENDENCY_UNAVAILABLE",
            "_EVIDENCE_UNAVAILABLE",
            "_INVENTORY_EXPIRED",
        )
    ):
        return 3
    return 5


__all__ = [
    "runtime_pod_retention_apply_command_result",
    "runtime_pod_retention_error_result",
    "runtime_pod_retention_plan_command_result",
]
