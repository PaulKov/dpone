"""Pointer loading and validation for launch pin resolution (authority C)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone_airflow_pack.launch_pin_codes import PIN_INVALID, PIN_MISSING, PIN_UNAVAILABLE
from dpone_airflow_pack.launch_pin_locator import (
    AIRFLOW_TASK_STATE_BACKEND,
    LaunchPinStoreLocator,
    attempt_pinned_store_locator_from_evidence,
    frozen_launch_pin_store_locator,
    remote_store_allowed,
    store_authority_digest,
)
from dpone_airflow_pack.launch_pin_summary import pull_locator_summary, summary_pointer_mismatch

if TYPE_CHECKING:
    from dpone_airflow_pack.launch_pin_resolve import LaunchPinResolution


def load_pin_pointer(
    *,
    ti: Any,
    upstream_task_id: str,
    required: bool,
    launch_pin_store: Mapping[str, Any] | LaunchPinStoreLocator | None,
    runtime_summary: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, LaunchPinResolution | None, dict[str, Any] | None]:
    """Load a pod-ref claim from the attempt-pinned backend.

    XCom is never live-Pod/envelope authority. ``required=false`` never opens
    Kubernetes — only an injected hermetic store may be consulted, otherwise
    legacy/absent. Gate couples ``summary.launch_pin_ref`` ↔ locator XCom ↔
    backend claim, then the caller verifies the exact live Pod occurrence.
    """

    from dpone_airflow_pack.launch_pin import authoritative_launch_pin_store
    from dpone_airflow_pack.launch_pin_resolve import LaunchPinResolution
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    attempt = attempt_coordinates(ti)
    summary, summary_status, summary_detail = pull_locator_summary(
        ti=ti,
        upstream_task_id=upstream_task_id,
        runtime_summary=runtime_summary,
        required=required,
    )
    if summary_status is not None:
        return None, LaunchPinResolution(status=summary_status, detail=summary_detail), None  # type: ignore[arg-type]
    if summary is None:
        if required:
            return (
                None,
                LaunchPinResolution(
                    status="MISSING_REQUIRED",
                    detail=(
                        "launch pin locator summary missing from upstream XCom/summary.launch_pin_ref; "
                        "gate requires try_number/pod_uid/pin_sha256/pointer_resource_version"
                    ),
                ),
                None,
            )
        return None, None, None
    try_number = int(summary.get("try_number", 0) or 0)
    if try_number < 1:
        return (
            None,
            LaunchPinResolution(
                status="INVALID",
                detail=f"{PIN_INVALID}: launch pin locator summary.try_number must be >= 1",
            ),
            None,
        )

    try:
        locator = resolve_gate_store_locator(
            summary=summary,
            launch_pin_store=launch_pin_store,
        )
    except RuntimeError as exc:
        detail = str(exc)
        if detail.startswith(PIN_INVALID):
            return None, LaunchPinResolution(status="INVALID", detail=detail), summary
        raise
    if locator.backend == AIRFLOW_TASK_STATE_BACKEND:
        # The coupled XCom is a pointer claim, never envelope authority. The
        # caller re-fetches the exact namespace/name/UID and rebuilds the pin
        # from the immutable live Pod before returning PRESENT.
        return dict(summary), None, summary

    injected = get_launch_pin_store()
    if injected is not None:
        store = injected
    elif not required:
        # Legacy / optional path: never perform Kubernetes I/O from the gate.
        return None, None, None
    else:
        if not remote_store_allowed(locator):
            return (
                None,
                LaunchPinResolution(
                    status="MISSING_REQUIRED",
                    detail=(
                        f"{PIN_MISSING}: launch pin store locator missing kubernetes_conn_id "
                        "(pack launch_pin_store / DPONE_LAUNCH_PIN_STORE_KUBERNETES_CONN_ID) "
                        "and process is not in-cluster"
                    ),
                ),
                summary,
            )
        store = authoritative_launch_pin_store(
            kubernetes_conn_id=locator.kubernetes_conn_id,
            namespace=locator.namespace,
            allow_remote=True,
        )

    try:
        stored = store.get(
            dag_id=str(summary.get("dag_id") or attempt["dag_id"] or getattr(ti, "dag_id", "") or ""),
            run_id=str(
                summary.get("run_id")
                or attempt["run_id"]
                or getattr(ti, "run_id", "")
                or getattr(ti, "dag_run_id", "")
                or ""
            ),
            task_id=str(summary.get("task_id") or upstream_task_id),
            map_index=int(summary.get("map_index", -1)),
            try_number=try_number,
        )
    except RuntimeError as exc:
        detail = str(exc)
        if detail.startswith(PIN_UNAVAILABLE):
            return None, LaunchPinResolution(status="UNAVAILABLE", detail=detail), summary
        return None, LaunchPinResolution(status="INVALID", detail=detail), summary
    if stored is None:
        if required:
            return (
                None,
                LaunchPinResolution(
                    status="MISSING_REQUIRED",
                    detail="launch pin store miss for exact try; XCom locator cannot satisfy required=true alone",
                ),
                summary,
            )
        return None, None, summary
    mismatch = summary_pointer_mismatch(summary=summary, pointer=stored)
    if mismatch:
        return None, LaunchPinResolution(status="INVALID", detail=mismatch, pin=dict(stored)), summary
    return dict(stored), None, summary


def required_pointer_store_authority_error(pointer: Mapping[str, Any]) -> str:
    """Require attempt-pinned backend and store coordinates on the pointer."""

    from dpone_airflow_pack.launch_pin_locator import LAUNCH_PIN_STORE_BACKENDS
    from dpone_airflow_pack.outcome_identity import is_sha256

    backend = str(pointer.get("store_backend") or "kubernetes_configmap")
    if backend not in LAUNCH_PIN_STORE_BACKENDS:
        return f"{PIN_INVALID}: launch pin.store_backend is invalid"
    store_namespace = pointer.get("store_namespace")
    if not isinstance(store_namespace, str) or not store_namespace.strip():
        return f"{PIN_INVALID}: launch pin.store_namespace must be a non-empty string when required=true"
    pointer_digest = pointer.get("store_authority_digest")
    if not isinstance(pointer_digest, str) or not is_sha256(pointer_digest.strip()):
        return f"{PIN_INVALID}: launch pin.store_authority_digest must be a canonical sha256 digest when required=true"
    return ""


def strict_pointer_error(pointer: Mapping[str, Any]) -> str:
    """Validate pointer structure before any remote pod access."""

    from dpone_airflow_pack.launch_pin import launch_pin_error

    # Store metadata is not part of the pin digest body.
    candidate = {
        key: value
        for key, value in pointer.items()
        if key
        not in {
            "pointer_resource_version",
            "state",
            "store_backend",
            "store_namespace",
            "store_authority_digest",
            "launch_pin_ref",
            "launch_pin_store",
        }
    }
    error = launch_pin_error(candidate, require_digest=True, require_identities=False)
    if error:
        return f"{PIN_INVALID}: {error}" if not error.startswith(PIN_INVALID) else error
    for field in ("pod_namespace", "pod_name", "pod_uid", "pin_sha256"):
        if not isinstance(pointer.get(field), str) or not str(pointer.get(field)).strip():
            return f"{PIN_INVALID}: launch pin.{field} must be a non-empty string"
    return ""


def subject_coordinate_error(
    *,
    pointer: Mapping[str, Any],
    ti: Any,
    upstream_task_id: str,
) -> str:
    """Require pointer subject coordinates to match the upstream runtime TI."""

    expected_dag = str(getattr(ti, "dag_id", "") or "")
    expected_run = str(getattr(ti, "run_id", "") or getattr(ti, "dag_run_id", "") or "")
    if expected_dag and str(pointer.get("dag_id") or "") not in {"", expected_dag}:
        return f"launch pin.dag_id {pointer.get('dag_id')!r} does not match gate task dag_id {expected_dag!r}"
    if expected_run and str(pointer.get("run_id") or "") not in {"", expected_run}:
        return f"launch pin.run_id {pointer.get('run_id')!r} does not match gate task run_id {expected_run!r}"
    if str(pointer.get("task_id") or "") not in {"", upstream_task_id}:
        return f"launch pin.task_id {pointer.get('task_id')!r} does not match upstream_task_id {upstream_task_id!r}"
    return ""


def locator_pointer_authority_error(
    *,
    pointer: Mapping[str, Any],
    locator: LaunchPinStoreLocator,
    summary: Mapping[str, Any] | None = None,
) -> str:
    """Fail closed when pointer cluster authority disagrees with attempt-pinned locator."""

    pointer_backend = str(pointer.get("store_backend") or "kubernetes_configmap")
    if pointer_backend != locator.backend:
        return (
            f"{PIN_INVALID}: launch pin.store_backend {pointer_backend!r} "
            f"does not match attempt-pinned locator backend {locator.backend!r}"
        )
    pointer_conn = pointer.get("kubernetes_conn_id")
    pointer_conn_id = str(pointer_conn).strip() if isinstance(pointer_conn, str) and str(pointer_conn).strip() else None
    if locator.kubernetes_conn_id and pointer_conn_id and locator.kubernetes_conn_id != pointer_conn_id:
        return (
            f"{PIN_INVALID}: launch pin.kubernetes_conn_id {pointer_conn_id!r} "
            f"does not match attempt-pinned locator {locator.kubernetes_conn_id!r}"
        )
    store_namespace = pointer.get("store_namespace")
    pointer_ns = (
        str(store_namespace).strip() if isinstance(store_namespace, str) and str(store_namespace).strip() else None
    )
    if locator.namespace and pointer_ns and locator.namespace != pointer_ns:
        return (
            f"{PIN_INVALID}: launch pin.store_namespace {pointer_ns!r} "
            f"does not match attempt-pinned locator namespace {locator.namespace!r}"
        )
    pointer_digest = pointer.get("store_authority_digest")
    pointer_store_digest = (
        str(pointer_digest).strip() if isinstance(pointer_digest, str) and str(pointer_digest).strip() else None
    )
    expected_digest = store_authority_digest(
        backend=locator.backend,
        kubernetes_conn_id=locator.kubernetes_conn_id,
        namespace=locator.namespace,
    )
    if pointer_store_digest and pointer_store_digest != expected_digest:
        return f"{PIN_INVALID}: launch pin.store_authority_digest does not match attempt-pinned store coordinates"
    ref_digest = None
    if isinstance(summary, Mapping):
        ref = summary.get("launch_pin_ref")
        if isinstance(ref, Mapping):
            raw = ref.get("store_authority_digest")
            ref_digest = str(raw).strip() if isinstance(raw, str) and str(raw).strip() else None
    if ref_digest and ref_digest != expected_digest:
        return (
            f"{PIN_INVALID}: launch pin locator summary.launch_pin_ref.store_authority_digest "
            "does not match attempt-pinned store coordinates"
        )
    if ref_digest and pointer_store_digest and ref_digest != pointer_store_digest:
        return f"{PIN_INVALID}: launch pin.store_authority_digest does not match summary.launch_pin_ref.store_authority_digest"
    return ""


def resolve_gate_store_locator(
    *,
    summary: Mapping[str, Any] | None,
    launch_pin_store: Mapping[str, Any] | LaunchPinStoreLocator | None,
) -> LaunchPinStoreLocator:
    """Prefer attempt-pinned store coords from launch evidence over parse-time gate tip."""

    attempt_pinned = attempt_pinned_store_locator_from_evidence(summary)
    if attempt_pinned is not None:
        return attempt_pinned
    return frozen_launch_pin_store_locator(launch_pin_store)


def attempt_coordinates(ti: Any) -> dict[str, Any]:
    return {
        "dag_id": str(getattr(ti, "dag_id", "") or ""),
        "run_id": str(getattr(ti, "run_id", "") or getattr(ti, "dag_run_id", "") or ""),
        "task_id": str(getattr(ti, "task_id", "") or ""),
        "map_index": int(getattr(ti, "map_index", -1)),
        "try_number": int(getattr(ti, "try_number", 1) or 1),
    }


__all__ = [
    "attempt_coordinates",
    "load_pin_pointer",
    "locator_pointer_authority_error",
    "required_pointer_store_authority_error",
    "resolve_gate_store_locator",
    "strict_pointer_error",
    "subject_coordinate_error",
]
