"""Self-service result facade for Airflow Connection Secret GC."""

from __future__ import annotations

import shlex

from dpone.adapters.kubernetes_airflow_connection_secret_gc import (
    build_kubernetes_airflow_connection_secret_gc_adapter,
)
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error, manual_fix
from dpone.services.airflow_connection_secret_gc import (
    AirflowConnectionSecretGcApplyRequest,
    AirflowConnectionSecretGcError,
    AirflowConnectionSecretGcPlanRequest,
    AirflowConnectionSecretGcService,
    airflow_connection_secret_gc_error_from_exception,
)

_SECURITY_CODES = frozenset(
    {
        "DPONE_AIRFLOW_SECRET_GC_CONFIRMATION_REQUIRED",
        "DPONE_AIRFLOW_SECRET_GC_ACTOR_MISSING",
        "DPONE_AIRFLOW_SECRET_GC_ACTOR_UNAUTHORIZED",
        "DPONE_AIRFLOW_SECRET_GC_ACCESS_DENIED",
        "DPONE_AIRFLOW_SECRET_GC_INVENTORY_INVALID",
        "DPONE_AIRFLOW_SECRET_GC_METADATA_ONLY_UNSUPPORTED",
        "DPONE_AIRFLOW_SECRET_GC_POD_METADATA_INVALID",
    }
)
_DEPENDENCY_CODES = frozenset(
    {
        "DPONE_AIRFLOW_SECRET_GC_DEPENDENCY_UNAVAILABLE",
        "DPONE_AIRFLOW_SECRET_GC_INVENTORY_EXPIRED",
        "DPONE_AIRFLOW_SECRET_GC_SDK_UNAVAILABLE",
    }
)


def connection_secret_gc_plan_command_result(
    *,
    namespace: str,
    minimum_age_seconds: int,
    page_size: int,
    auth_mode: str,
    kube_context: str | None,
) -> SelfServiceResult:
    """Compose and run one platform plan without leaking vendor exceptions."""

    safe_namespace: str | None = None
    try:
        request = AirflowConnectionSecretGcPlanRequest(
            namespace=namespace,
            minimum_age_seconds=minimum_age_seconds,
            page_size=page_size,
        )
        safe_namespace = request.namespace
        _validate_auth_options(auth_mode=auth_mode, kube_context=kube_context)
        adapter = build_kubernetes_airflow_connection_secret_gc_adapter(
            auth_mode=auth_mode,
            kube_context=kube_context,
        )
        return connection_secret_gc_plan_result(
            AirflowConnectionSecretGcService(inventory=adapter, deletion=adapter),
            request,
        )
    except Exception as exc:  # noqa: BLE001 - composition failures are redacted below.
        return connection_secret_gc_error_result(exc, namespace=safe_namespace)


def connection_secret_gc_apply_command_result(
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
) -> SelfServiceResult:
    """Validate local mutation policy before constructing infrastructure clients."""

    safe_namespace: str | None = None
    try:
        request = AirflowConnectionSecretGcApplyRequest(
            namespace=namespace,
            minimum_age_seconds=minimum_age_seconds,
            page_size=page_size,
            max_delete_count=max_delete_count,
            actor=actor,
            allowed_actors=allowed_actors,
            confirm_delete=confirm_delete,
        )
        safe_namespace = request.namespace
        request.require_authorized()
        _validate_auth_options(auth_mode=auth_mode, kube_context=kube_context)
        adapter = build_kubernetes_airflow_connection_secret_gc_adapter(
            auth_mode=auth_mode,
            kube_context=kube_context,
        )
        return connection_secret_gc_apply_result(
            AirflowConnectionSecretGcService(inventory=adapter, deletion=adapter),
            request,
        )
    except Exception as exc:  # noqa: BLE001 - composition failures are redacted below.
        return connection_secret_gc_error_result(exc, namespace=safe_namespace)


def connection_secret_gc_plan_result(
    service: AirflowConnectionSecretGcService,
    request: AirflowConnectionSecretGcPlanRequest,
) -> SelfServiceResult:
    try:
        report = service.plan(request)
    except AirflowConnectionSecretGcError as exc:
        return connection_secret_gc_error_result(exc, namespace=request.namespace)
    needs_attention = report.get("status") == "needs_attention"
    return SelfServiceResult(
        passed=not needs_attention,
        details=report,
        exit_code=1 if needs_attention else 0,
    )


def connection_secret_gc_apply_result(
    service: AirflowConnectionSecretGcService,
    request: AirflowConnectionSecretGcApplyRequest,
) -> SelfServiceResult:
    try:
        report = service.apply(request)
    except AirflowConnectionSecretGcError as exc:
        return connection_secret_gc_error_result(exc, namespace=request.namespace)
    passed = report.get("status") == "ok"
    return SelfServiceResult(passed=passed, details=report, exit_code=_apply_exit_code(report))


def connection_secret_gc_error_result(
    exc: BaseException,
    *,
    namespace: str | None = None,
) -> SelfServiceResult:
    if isinstance(exc, AirflowConnectionSecretGcError):
        code = exc.code
        message = str(exc)
    elif (mapped := airflow_connection_secret_gc_error_from_exception(exc)) is not None:
        code = mapped.code
        message = str(mapped)
    else:
        code = "DPONE_INTERNAL_AIRFLOW_SECRET_GC_FAILED"
        message = "Airflow Connection Secret GC failed unexpectedly."
    if code == "DPONE_AIRFLOW_SECRET_GC_SDK_UNAVAILABLE":
        fixes = [
            manual_fix(
                "install_kubernetes_support",
                command="python -m pip install 'dpone[kubernetes]'",
            )
        ]
    else:
        fix_command = (
            f"dpone airflow connection-secret-gc-plan --namespace {shlex.quote(namespace)}"
            if namespace is not None
            else None
        )
        fixes = [manual_fix("rerun_connection_secret_gc_plan", command=fix_command)]
    error = dpone_error(
        code,
        message,
        stage="airflow_connection_secret_gc",
        fixes=fixes,
    )
    return SelfServiceResult(passed=False, errors=(error,), exit_code=_exit_code(code))


def _exit_code(code: str) -> int:
    if code == "DPONE_AIRFLOW_SECRET_GC_INPUT_INVALID":
        return 2
    if code in _SECURITY_CODES:
        return 4
    if code in _DEPENDENCY_CODES:
        return 3
    return 5


def _apply_exit_code(report: dict[str, object]) -> int:
    status = report.get("status")
    if status == "ok":
        return 0
    if status == "partial":
        return 1
    items = report.get("items")
    if isinstance(items, list) and any(
        isinstance(item, dict) and item.get("error_code") == "DPONE_AIRFLOW_SECRET_GC_ACCESS_DENIED" for item in items
    ):
        return 4
    return 3


def _validate_auth_options(*, auth_mode: str, kube_context: str | None) -> None:
    if auth_mode == "in-cluster" and kube_context:
        raise AirflowConnectionSecretGcError(
            "DPONE_AIRFLOW_SECRET_GC_INPUT_INVALID",
            "--kube-context cannot be combined with --kube-auth in-cluster.",
        )


__all__ = [
    "connection_secret_gc_apply_result",
    "connection_secret_gc_apply_command_result",
    "connection_secret_gc_error_result",
    "connection_secret_gc_plan_command_result",
    "connection_secret_gc_plan_result",
]
