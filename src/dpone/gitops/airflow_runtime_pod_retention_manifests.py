"""Deterministic Kubernetes resources for Airflow runtime Pod retention."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionApplyRequest,
    AirflowRuntimePodRetentionError,
    AirflowRuntimePodRetentionPlanRequest,
    validate_max_delete_count,
)
from dpone.contracts.airflow_runtime_pod_retention_template import (
    RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION,
    runtime_pod_retention_template_sha256,
)
from dpone.contracts.kubernetes_cron import require_kubernetes_cron_schedule
from dpone.kubernetes_names import require_kubernetes_dns_label

_IMAGE_DIGEST = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
_NAME = "dpone-runtime-pod-retention"
_MODE_ANNOTATION = "dpone.dev/runtime-pod-retention-mode"
_TEMPLATE_SHA_ANNOTATION = RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION


@dataclass(frozen=True, slots=True)
class AirflowRuntimePodRetentionRenderRequest:
    namespace: str
    image: str
    schedule: str = "17 * * * *"
    mode: str = "plan"
    minimum_age_seconds: int = 86_400
    page_size: int = 500
    max_delete_count: int = 100
    actor: str = ""
    allowed_actors: tuple[str, ...] = ()
    confirm_delete: bool = False
    alerts: str = "off"
    stale_after_seconds: int | None = None


def render_runtime_pod_retention(request: AirflowRuntimePodRetentionRenderRequest) -> dict[str, Any]:
    _validate_request(request)
    command = _command(request)
    manifests = [
        _service_account(request),
        _role(request),
        _role_binding(request),
        _cron_job(request, command),
    ]
    if request.alerts == "prometheus":
        manifests.append(_prometheus_rule(request))
    canonical = json.dumps(manifests, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    report = {
        "schema": "dpone.airflow-runtime-pod-retention-render.v1",
        "passed": True,
        "status": "rendered",
        "namespace": request.namespace,
        "mode": request.mode,
        "image": request.image,
        "schedule": request.schedule,
        "minimum_age_seconds": request.minimum_age_seconds,
        "page_size": request.page_size,
        "max_delete_count": request.max_delete_count,
        "alerts": request.alerts,
        "manifest_sha256": f"sha256:{hashlib.sha256(canonical).hexdigest()}",
        "resources": [{"kind": item["kind"], "name": item["metadata"]["name"]} for item in manifests],
        "manifests": manifests,
    }
    if request.stale_after_seconds is not None:
        report["stale_after_seconds"] = request.stale_after_seconds
    return report


def _validate_request(request: AirflowRuntimePodRetentionRenderRequest) -> None:
    try:
        require_kubernetes_dns_label(request.namespace, context="namespace")
    except ValueError as exc:
        raise ValueError("namespace must be a Kubernetes DNS label") from exc
    if not _IMAGE_DIGEST.fullmatch(request.image):
        raise ValueError("image must be an immutable @sha256 reference")
    if len(request.schedule) > 128 or any(char in request.schedule for char in "\r\n\0"):
        raise ValueError("schedule must be one bounded single-line cron expression")
    require_kubernetes_cron_schedule(request.schedule)
    if request.mode not in {"plan", "apply"}:
        raise ValueError("mode must be plan or apply")
    if request.alerts not in {"off", "prometheus"}:
        raise ValueError("alerts must be off or prometheus")
    if request.alerts == "prometheus":
        if (
            isinstance(request.stale_after_seconds, bool)
            or request.stale_after_seconds is None
            or not 600 <= request.stale_after_seconds <= 63_244_800
        ):
            raise ValueError("prometheus alerts require stale_after_seconds between 600 and 63244800")
    elif request.stale_after_seconds is not None:
        raise ValueError("stale_after_seconds requires prometheus alerts")
    try:
        AirflowRuntimePodRetentionPlanRequest(
            namespace=request.namespace,
            minimum_age_seconds=request.minimum_age_seconds,
            page_size=request.page_size,
        )
        validate_max_delete_count(request.max_delete_count)
        if request.mode == "apply":
            AirflowRuntimePodRetentionApplyRequest(
                namespace=request.namespace,
                minimum_age_seconds=request.minimum_age_seconds,
                page_size=request.page_size,
                max_delete_count=request.max_delete_count,
                actor=request.actor,
                allowed_actors=request.allowed_actors,
                confirm_delete=request.confirm_delete,
                kube_auth_mode="in-cluster",
            ).require_authorized()
    except AirflowRuntimePodRetentionError as exc:
        raise ValueError(str(exc)) from None
    if request.mode == "plan" and (
        request.actor or request.allowed_actors or request.confirm_delete or request.max_delete_count != 100
    ):
        raise ValueError("plan mode does not accept apply-only actor, confirmation, or delete-count options")


def _command(request: AirflowRuntimePodRetentionRenderRequest) -> list[str]:
    command = [
        "dpone",
        "airflow",
        f"runtime-pod-retention-{request.mode}",
        "--namespace",
        request.namespace,
        "--minimum-age-seconds",
        str(request.minimum_age_seconds),
        "--page-size",
        str(request.page_size),
        "--kube-auth",
        "in-cluster",
        "--format",
        "json",
    ]
    if request.mode == "apply":
        command.extend(["--max-delete-count", str(request.max_delete_count), "--actor", request.actor])
        for actor in request.allowed_actors:
            command.extend(["--allowed-actor", actor])
        command.append("--confirm-delete")
    return command


def _metadata(request: AirflowRuntimePodRetentionRenderRequest) -> dict[str, object]:
    return {
        "name": _NAME,
        "namespace": request.namespace,
        "labels": {"app.kubernetes.io/name": _NAME, "app.kubernetes.io/managed-by": "dpone"},
    }


def _service_account(request: AirflowRuntimePodRetentionRenderRequest) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": _metadata(request)}


def _role(request: AirflowRuntimePodRetentionRenderRequest) -> dict[str, Any]:
    verbs = ["list"] if request.mode == "plan" else ["list", "delete"]
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": _metadata(request),
        "rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": verbs}],
    }


def _role_binding(request: AirflowRuntimePodRetentionRenderRequest) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": _metadata(request),
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": _NAME},
        "subjects": [{"kind": "ServiceAccount", "name": _NAME, "namespace": request.namespace}],
    }


def _cron_job(request: AirflowRuntimePodRetentionRenderRequest, command: list[str]) -> dict[str, Any]:
    labels = _metadata(request)["labels"]
    annotations = {_MODE_ANNOTATION: request.mode}
    job_template = {
        "metadata": {"labels": labels, "annotations": annotations},
        "spec": {
            "activeDeadlineSeconds": 300,
            "ttlSecondsAfterFinished": 86_400,
            "backoffLimit": 0,
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": {
                    "serviceAccountName": _NAME,
                    "automountServiceAccountToken": True,
                    "restartPolicy": "Never",
                    "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                    "containers": [
                        {
                            "name": "retention",
                            "image": request.image,
                            "command": command,
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                                "readOnlyRootFilesystem": True,
                            },
                            "resources": {
                                "requests": {"cpu": "25m", "memory": "64Mi"},
                                "limits": {"cpu": "250m", "memory": "256Mi"},
                            },
                        }
                    ],
                },
            },
        },
    }
    annotations[_TEMPLATE_SHA_ANNOTATION] = runtime_pod_retention_template_sha256(job_template)
    return {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": _metadata(request),
        "spec": {
            "schedule": request.schedule,
            "suspend": False,
            "concurrencyPolicy": "Forbid",
            "successfulJobsHistoryLimit": 1,
            "failedJobsHistoryLimit": 3,
            "jobTemplate": job_template,
        },
    }


def _prometheus_rule(request: AirflowRuntimePodRetentionRenderRequest) -> dict[str, Any]:
    namespace = request.namespace
    selector = f'namespace="{namespace}",cronjob="{_NAME}"'
    last_success = f"kube_cronjob_status_last_successful_time{{{selector}}}"
    created = f"kube_cronjob_created{{{selector}}}"
    assert request.stale_after_seconds is not None
    threshold = request.stale_after_seconds
    stale_success = f"time() - {last_success} > {threshold}"
    never_succeeded = f"(time() - {created} > {threshold}) unless on(namespace, cronjob) {last_success}"
    return {
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PrometheusRule",
        "metadata": _metadata(request),
        "spec": {
            "groups": [
                {
                    "name": "dpone-runtime-pod-retention",
                    "rules": [
                        {
                            "alert": "DponeRuntimePodRetentionJobFailed",
                            "expr": (
                                f'kube_job_status_failed{{namespace="{namespace}",'
                                'job_name=~"dpone-runtime-pod-retention-.*"} > 0'
                            ),
                            "for": "10m",
                            "labels": {"severity": "warning"},
                            "annotations": {"summary": "dpone runtime Pod retention job failed"},
                        },
                        {
                            "alert": "DponeRuntimePodRetentionStale",
                            "expr": f"({stale_success}) or ({never_succeeded})",
                            "for": "15m",
                            "labels": {"severity": "warning"},
                            "annotations": {"summary": "dpone runtime Pod retention has no recent success"},
                        },
                    ],
                }
            ]
        },
    }


__all__ = ["AirflowRuntimePodRetentionRenderRequest", "render_runtime_pod_retention"]
