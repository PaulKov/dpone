"""Resolve launch pins for separate outcome_gate (Kubernetes authority C)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from dpone_airflow_pack.deployment_identity import deployment_identity_error
from dpone_airflow_pack.launch_pin_codes import PIN_INVALID, PIN_MISSING, PIN_UNAVAILABLE
from dpone_airflow_pack.launch_pin_envelope import launch_envelope_from_pod
from dpone_airflow_pack.launch_pin_locator import LaunchPinStoreLocator
from dpone_airflow_pack.launch_pin_pod import fetch_pod_for_launch_pin
from dpone_airflow_pack.launch_pin_resolve_pointer import (
    load_pin_pointer,
    locator_pointer_authority_error,
    required_pointer_store_authority_error,
    resolve_gate_store_locator,
    strict_pointer_error,
    subject_coordinate_error,
)
from dpone_airflow_pack.launch_pin_summary import AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY

AIRFLOW_RUNTIME_LAUNCH_PIN_SCHEMA = "dpone.airflow-runtime-launch-pin.v1"
AIRFLOW_RUN_PINNED_DEPLOYMENT_IDENTITY_XCOM_KEY = "dpone_run_pinned_deployment_identity"

LaunchPinStatus = Literal[
    "PRESENT",
    "PRESENT_LEGACY_PARTIAL",
    "ABSENT_LEGACY",
    "MISSING_REQUIRED",
    "INVALID",
    "UNAVAILABLE",
]


@dataclass(frozen=True)
class LaunchPinResolution:
    """Typed outcome of reading the upstream pack-exec launch pin."""

    status: LaunchPinStatus
    pin: dict[str, Any] | None = None
    store_locator: LaunchPinStoreLocator | None = None
    detail: str = ""

    @property
    def deployment_identity(self) -> dict[str, str] | None:
        if self.pin is None:
            return None
        identity = self.pin.get("deployment_identity")
        return dict(identity) if isinstance(identity, Mapping) else None

    @property
    def run_identity(self) -> dict[str, Any] | None:
        if self.pin is None:
            return None
        identity = self.pin.get("run_identity")
        return dict(identity) if isinstance(identity, Mapping) else None

    @property
    def expected_runtime_evidence_sha256(self) -> str | None:
        if self.pin is None:
            return None
        digest = self.pin.get("expected_runtime_evidence_sha256")
        return digest if isinstance(digest, str) else None


def resolve_launch_pin(
    *,
    ti: Any,
    upstream_task_id: str,
    required: bool = False,
    launch_pin_store: Mapping[str, Any] | LaunchPinStoreLocator | None = None,
    runtime_summary: Mapping[str, Any] | None = None,
) -> LaunchPinResolution:
    """Resolve the upstream launch pin; envelope authority is the live pod."""

    from dpone_airflow_pack.launch_pin import build_launch_pin, launch_pin_error

    if ti is None:
        return LaunchPinResolution(status="UNAVAILABLE", detail="task instance is missing")

    pointer, pointer_error, locator_summary = load_pin_pointer(
        ti=ti,
        upstream_task_id=upstream_task_id,
        required=required,
        launch_pin_store=launch_pin_store,
        runtime_summary=runtime_summary,
    )
    if pointer_error is not None:
        return pointer_error
    if pointer is None:
        return _absent_or_legacy(ti=ti, upstream_task_id=upstream_task_id, required=required)

    error = strict_pointer_error(pointer)
    if error:
        return LaunchPinResolution(status="INVALID", detail=error, pin=dict(pointer))
    if required:
        store_error = required_pointer_store_authority_error(pointer)
        if store_error:
            return LaunchPinResolution(status="INVALID", detail=store_error, pin=dict(pointer))

    subject_error = subject_coordinate_error(
        pointer=pointer,
        ti=ti,
        upstream_task_id=upstream_task_id,
    )
    if subject_error:
        return LaunchPinResolution(status="INVALID", detail=subject_error, pin=dict(pointer))

    locator = resolve_gate_store_locator(
        summary=locator_summary,
        launch_pin_store=launch_pin_store,
    )
    authority_error = locator_pointer_authority_error(
        pointer=pointer,
        locator=locator,
        summary=locator_summary,
    )
    if authority_error:
        return LaunchPinResolution(status="INVALID", detail=authority_error, pin=dict(pointer))
    if required and (not locator.namespace or not str(locator.namespace).strip()):
        return LaunchPinResolution(
            status="INVALID",
            detail=f"{PIN_INVALID}: attempt-pinned launch_pin_store.namespace must be non-empty when required=true",
            pin=dict(pointer),
        )

    conn_id = pointer.get("kubernetes_conn_id")
    kubernetes_conn_id = str(conn_id).strip() if isinstance(conn_id, str) and str(conn_id).strip() else None
    if kubernetes_conn_id is None and locator.kubernetes_conn_id:
        kubernetes_conn_id = locator.kubernetes_conn_id

    try:
        remote = fetch_pod_for_launch_pin(
            namespace=str(pointer["pod_namespace"]),
            name=str(pointer["pod_name"]),
            expected_uid=str(pointer["pod_uid"]),
            kubernetes_conn_id=kubernetes_conn_id,
        )
        envelope = launch_envelope_from_pod(remote)
        pin = build_launch_pin(
            attempt={
                "dag_id": pointer.get("dag_id") or getattr(ti, "dag_id", "") or "",
                "run_id": pointer.get("run_id") or getattr(ti, "run_id", "") or getattr(ti, "dag_run_id", "") or "",
                "task_id": pointer.get("task_id") or upstream_task_id,
                "map_index": int(pointer.get("map_index", -1)),
                "try_number": int(pointer.get("try_number", 1) or 1),
            },
            pod_namespace=str(pointer["pod_namespace"]),
            pod_name=str(pointer["pod_name"]),
            pod_uid=str(pointer["pod_uid"]),
            kubernetes_conn_id=kubernetes_conn_id,
            run_identity=envelope["run_identity"],
            deployment_identity=envelope["deployment_identity"],
            expected_runtime_evidence_sha256=envelope["expected_runtime_evidence_sha256"],
        )
    except RuntimeError as exc:
        detail = str(exc)
        if detail.startswith(PIN_UNAVAILABLE):
            return LaunchPinResolution(status="UNAVAILABLE", detail=detail, pin=dict(pointer))
        if detail.startswith(PIN_MISSING):
            return LaunchPinResolution(
                status="MISSING_REQUIRED" if required else "INVALID",
                detail=detail,
                pin=dict(pointer),
            )
        return LaunchPinResolution(status="INVALID", detail=detail, pin=dict(pointer))
    except Exception as exc:  # noqa: BLE001
        return LaunchPinResolution(
            status="UNAVAILABLE",
            detail=f"{PIN_UNAVAILABLE}: launch pin pod re-fetch failed: {exc}",
            pin=dict(pointer),
        )

    if "pointer_resource_version" in pointer:
        pin["pointer_resource_version"] = pointer.get("pointer_resource_version")
    from dpone_airflow_pack.launch_pin_locator import launch_pin_store_locator_to_mapping

    pin["store_namespace"] = pointer.get("store_namespace")
    pin["store_backend"] = pointer.get("store_backend") or locator.backend
    pin["store_authority_digest"] = pointer.get("store_authority_digest")
    pin["launch_pin_store"] = launch_pin_store_locator_to_mapping(locator)

    pointer_digest = pointer.get("pin_sha256")
    if isinstance(pointer_digest, str) and pointer_digest and pin["pin_sha256"] != pointer_digest:
        return LaunchPinResolution(
            status="INVALID",
            detail=(
                f"{PIN_INVALID}: rebuilt pin_sha256 does not match pointer.pin_sha256 "
                f"(pointer={pointer_digest!r}, rebuilt={pin['pin_sha256']!r})"
            ),
            pin=pin,
        )

    error = launch_pin_error(pin, require_identities=required)
    if error:
        return LaunchPinResolution(
            status="MISSING_REQUIRED" if required and "must be non-null" in error else "INVALID",
            detail=error,
            pin=pin,
        )
    return LaunchPinResolution(status="PRESENT", pin=pin, store_locator=locator)


def raise_for_launch_pin_resolution(resolution: LaunchPinResolution) -> None:
    """Fail closed for non-legacy pin resolution outcomes."""

    if resolution.status in {"PRESENT", "PRESENT_LEGACY_PARTIAL", "ABSENT_LEGACY"}:
        return
    code = {
        "MISSING_REQUIRED": PIN_MISSING,
        "INVALID": PIN_INVALID,
        "UNAVAILABLE": PIN_UNAVAILABLE,
    }[resolution.status]
    raise RuntimeError(
        json.dumps(
            {"code": code, "status": resolution.status, "detail": resolution.detail},
            ensure_ascii=True,
            sort_keys=True,
        )
    )


def _absent_or_legacy(*, ti: Any, upstream_task_id: str, required: bool) -> LaunchPinResolution:
    legacy = _pull_legacy_deployment_pin(ti=ti, upstream_task_id=upstream_task_id)
    if legacy.status == "PRESENT_LEGACY_PARTIAL":
        if required:
            return LaunchPinResolution(
                status="MISSING_REQUIRED",
                detail="legacy deployment-only pin cannot satisfy full-envelope required=true",
            )
        return legacy
    if legacy.status != "ABSENT_LEGACY":
        return legacy
    return LaunchPinResolution(
        status="MISSING_REQUIRED" if required else "ABSENT_LEGACY",
        detail="launch pin is absent",
    )


def _pull_legacy_deployment_pin(*, ti: Any, upstream_task_id: str) -> LaunchPinResolution:
    from dpone_airflow_pack.launch_pin import build_launch_pin

    if not hasattr(ti, "xcom_pull"):
        return LaunchPinResolution(status="ABSENT_LEGACY", detail="legacy deployment pin absent")
    try:
        raw = ti.xcom_pull(task_ids=upstream_task_id, key=AIRFLOW_RUN_PINNED_DEPLOYMENT_IDENTITY_XCOM_KEY)
    except Exception as exc:  # noqa: BLE001
        return LaunchPinResolution(status="UNAVAILABLE", detail=str(exc) or exc.__class__.__name__)
    if raw is None:
        return LaunchPinResolution(status="ABSENT_LEGACY", detail="legacy deployment pin absent")
    if not isinstance(raw, Mapping) or deployment_identity_error(raw):
        return LaunchPinResolution(status="INVALID", detail="legacy deployment pin is invalid")
    pin = build_launch_pin(
        attempt={
            "dag_id": "legacy",
            "run_id": "legacy",
            "task_id": upstream_task_id,
            "map_index": -1,
            "try_number": 1,
        },
        pod_namespace="legacy",
        pod_name="legacy",
        pod_uid="legacy",
        run_identity=None,
        deployment_identity={key: str(raw[key]) for key in ("schema", "release_id", "deployment_id", "activation_id")},
        expected_runtime_evidence_sha256=None,
    )
    return LaunchPinResolution(status="PRESENT_LEGACY_PARTIAL", pin=pin, detail="legacy deployment-only pin")


__all__ = [
    "AIRFLOW_RUNTIME_LAUNCH_PIN_SCHEMA",
    "AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY",
    "AIRFLOW_RUN_PINNED_DEPLOYMENT_IDENTITY_XCOM_KEY",
    "LaunchPinResolution",
    "LaunchPinStatus",
    "raise_for_launch_pin_resolution",
    "resolve_launch_pin",
]
