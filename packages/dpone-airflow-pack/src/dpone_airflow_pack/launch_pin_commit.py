"""Stage-aware launch-pin commit after get_or_create_pod (Authority C)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.launch_pin_barrier import abandon_pod_after_cas_failure
from dpone_airflow_pack.launch_pin_codes import (
    HEAD_PROVEN_CLOSED_STATUSES,
    PIN_RECOVERY_REQUIRED,
)
from dpone_airflow_pack.launch_pin_locator import store_authority_digest
from dpone_airflow_pack.launch_pin_outcomes import (
    classify_create_once_failure,
    is_active_confirmed_pin,
    raise_for_pre_active_abandon,
)
from dpone_airflow_pack.launch_pin_resolve import AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY
from dpone_airflow_pack.launch_pin_selected_pod import (
    build_selected_pod_launch_pin,
    publish_launch_pin_locator,
)
from dpone_airflow_pack.launch_pin_summary import launch_pin_ref_error

_LAUNCH_PIN_REF_FIELDS = (
    "try_number",
    "pod_uid",
    "pin_sha256",
    "envelope_sha256",
    "pointer_resource_version",
    "store_authority_digest",
)


def maybe_commit_launch_pin_for_selected_pod(
    *,
    enabled: bool,
    context: Any,
    pod: Any,
    kubernetes_conn_id: str | None = None,
    launch_pin_store: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Commit a launch pin when the runtime task owns a separate outcome_gate."""

    if not enabled:
        return None
    return commit_launch_pin_for_selected_pod(
        context=context,
        pod=pod,
        kubernetes_conn_id=kubernetes_conn_id,
        launch_pin_store=launch_pin_store,
    )


def commit_launch_pin_for_selected_pod(
    *,
    context: Any,
    pod: Any,
    kubernetes_conn_id: str | None = None,
    launch_pin_store: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create-only pod-ref pin after get_or_create_pod (+ UID hydration)."""

    from dpone_airflow_pack.launch_pin import authoritative_launch_pin_store

    selected = build_selected_pod_launch_pin(
        context=context,
        pod=pod,
        kubernetes_conn_id=kubernetes_conn_id,
        launch_pin_store=launch_pin_store,
    )
    pin = selected.pin
    closed = selected.locator
    store = authoritative_launch_pin_store(
        kubernetes_conn_id=closed.kubernetes_conn_id,
        namespace=closed.namespace,
        allow_remote=True,
    )
    try:
        retained = store.create_once(pin)
    except Exception as exc:  # noqa: BLE001 - classify before any abandon
        outcome = classify_create_once_failure(exc, candidate=pin, store=store)
        if outcome.kind == "IDEMPOTENT_SAME_POD_WINNER" and outcome.pin is not None:
            retained = outcome.pin
        elif outcome.kind == "ACTIVE_ACK_UNKNOWN":
            raise RuntimeError(outcome.detail) from exc
        elif outcome.kind == "PRE_ACTIVE_FAILURE":
            # Only release when we may own a CANDIDATE reservation; foreign ACTIVE
            # heads must stay put (CONFLICT_OTHER_POD / STALE paths).
            _release_head_after_pre_active_failure(store=store, pin=pin)
            abandoned = abandon_pod_after_cas_failure(
                pod=pod,
                kubernetes_conn_id=selected.kubernetes_conn_id,
                detail=outcome.detail,
            )
            raise_for_pre_active_abandon(abandoned=abandoned, detail=outcome.detail)
            raise  # pragma: no cover
        elif outcome.kind == "CONFLICT_OTHER_POD":
            abandoned = abandon_pod_after_cas_failure(
                pod=pod,
                kubernetes_conn_id=selected.kubernetes_conn_id,
                detail=outcome.detail,
            )
            raise_for_pre_active_abandon(abandoned=abandoned, detail=outcome.detail)
            raise  # pragma: no cover
        else:
            raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: {outcome.detail}") from exc
    if not is_active_confirmed_pin(retained=retained, candidate=pin):
        raise RuntimeError(
            f"{PIN_RECOVERY_REQUIRED}: launch pin create_once did not confirm ACTIVE "
            f"for uid={pin['pod_uid']!r} digest={pin['pin_sha256']!r}"
        )
    ref = build_launch_pin_ref(retained)
    publish_launch_pin_locator(
        task_instance=selected.task_instance,
        pin=retained,
        launch_pin_ref=ref,
    )
    retained = dict(retained)
    retained["launch_pin_ref"] = ref
    return retained


def build_launch_pin_ref(pin: Mapping[str, Any]) -> dict[str, Any]:
    """Compact closed occurrence ref embedded in runtime summary + locator XCom."""

    store_digest = pin.get("store_authority_digest")
    if not isinstance(store_digest, str) or not store_digest.strip():
        store_digest = store_authority_digest(
            backend=str(pin.get("store_backend") or "kubernetes_configmap"),
            kubernetes_conn_id=(
                str(pin["kubernetes_conn_id"]).strip()
                if isinstance(pin.get("kubernetes_conn_id"), str) and str(pin["kubernetes_conn_id"]).strip()
                else None
            ),
            namespace=(
                str(pin["store_namespace"]).strip()
                if isinstance(pin.get("store_namespace"), str) and str(pin["store_namespace"]).strip()
                else None
            ),
        )
    ref = {
        "try_number": int(pin["try_number"]),
        "pod_uid": str(pin["pod_uid"]),
        "pin_sha256": str(pin["pin_sha256"]),
        "envelope_sha256": str(pin["envelope_sha256"]),
        "pointer_resource_version": pin.get("pointer_resource_version"),
        "store_authority_digest": str(store_digest),
    }
    error = launch_pin_ref_error(ref)
    if error:
        raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: ACTIVE pin missing closed launch_pin_ref fields: {error}")
    try_number = ref["try_number"]
    assert isinstance(try_number, int) and not isinstance(try_number, bool)
    return {
        "try_number": try_number,
        "pod_uid": str(ref["pod_uid"]),
        "pin_sha256": str(ref["pin_sha256"]),
        "envelope_sha256": str(ref["envelope_sha256"]),
        "pointer_resource_version": str(ref["pointer_resource_version"]),
        "store_authority_digest": str(ref["store_authority_digest"]),
    }


def embed_launch_pin_ref_in_summary(operator: Any, result: Any, *, context: Any | None = None) -> Any:
    """Embed committed ``launch_pin_ref`` into the runtime JSON XCom summary."""

    ref = getattr(operator, "_committed_launch_pin_ref", None)
    if not isinstance(ref, Mapping):
        ref = rehydrate_committed_launch_pin_ref(operator, context=context)
    if not isinstance(ref, Mapping):
        return result
    error = launch_pin_ref_error(ref)
    if error:
        raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: {error}")
    closed = {field: ref[field] if field != "try_number" else int(ref[field]) for field in _LAUNCH_PIN_REF_FIELDS}
    closed["pointer_resource_version"] = str(ref["pointer_resource_version"])
    closed["pod_uid"] = str(ref["pod_uid"])
    closed["pin_sha256"] = str(ref["pin_sha256"])
    closed["envelope_sha256"] = str(ref["envelope_sha256"])
    if isinstance(result, Mapping):
        merged = dict(result)
        merged["launch_pin_ref"] = closed
        _rewrite_return_value_xcom(context=context, summary=merged)
        return merged
    return result


def rehydrate_committed_launch_pin_ref(
    operator: Any,
    *,
    context: Any | None = None,
    defer_kwargs: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Restore ``launch_pin_ref`` after deferral (new operator instance).

    Prefer defer kwargs, then durable locator XCom. Returns a closed ref or None.
    """

    for source in (
        (defer_kwargs or {}).get("launch_pin_ref"),
        getattr(operator, "_committed_launch_pin_ref", None),
    ):
        if isinstance(source, Mapping) and not launch_pin_ref_error(source):
            closed = dict(source)
            operator._committed_launch_pin_ref = closed
            return closed
    ti = context.get("ti") if isinstance(context, Mapping) else None
    if ti is None or not hasattr(ti, "xcom_pull"):
        return None
    task_id = str(getattr(operator, "task_id", "") or "")
    try:
        raw = ti.xcom_pull(task_ids=task_id or None, key=AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY)
    except Exception:  # noqa: BLE001
        raw = None
    if not isinstance(raw, Mapping):
        return None
    nested = raw.get("launch_pin_ref")
    candidate = nested if isinstance(nested, Mapping) else raw
    if launch_pin_ref_error(candidate):
        return None
    closed = {
        "try_number": int(candidate["try_number"]),
        "pod_uid": str(candidate["pod_uid"]),
        "pin_sha256": str(candidate["pin_sha256"]),
        "envelope_sha256": str(candidate["envelope_sha256"]),
        "pointer_resource_version": str(candidate["pointer_resource_version"]),
        "store_authority_digest": str(candidate["store_authority_digest"]),
    }
    operator._committed_launch_pin_ref = closed
    return closed


def remember_committed_launch_pin_ref(operator: Any, retained: Mapping[str, Any] | None) -> None:
    """Persist a closed ref on the operator for execute / defer handoff."""

    if not isinstance(retained, Mapping):
        return
    raw = retained.get("launch_pin_ref")
    if not isinstance(raw, Mapping):
        return
    error = launch_pin_ref_error(raw)
    if error:
        raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: {error}")
    operator._committed_launch_pin_ref = {
        "try_number": int(raw["try_number"]),
        "pod_uid": str(raw["pod_uid"]),
        "pin_sha256": str(raw["pin_sha256"]),
        "envelope_sha256": str(raw["envelope_sha256"]),
        "pointer_resource_version": str(raw["pointer_resource_version"]),
        "store_authority_digest": str(raw["store_authority_digest"]),
    }


def inject_launch_pin_ref_into_defer_kwargs(
    operator: Any,
    *,
    kwargs: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Merge closed ``launch_pin_ref`` into ``BaseOperator.defer`` kwargs."""

    merged = dict(kwargs or {})
    ref = getattr(operator, "_committed_launch_pin_ref", None)
    if isinstance(ref, Mapping) and not launch_pin_ref_error(ref):
        merged["launch_pin_ref"] = dict(ref)
    return merged


def _release_head_after_pre_active_failure(*, store: Any, pin: Mapping[str, Any]) -> None:
    release = getattr(store, "release_own_head_candidate", None)
    if not callable(release):
        return
    try:
        status = release(pin)
    except Exception as exc:  # noqa: BLE001
        detail = str(exc)
        # Foreign/stale head was never ours to release (CONFLICT / STALE_WRITER).
        if "non-own/ambiguous head" in detail:
            return
        raise RuntimeError(
            f"{PIN_RECOVERY_REQUIRED}: failed to release own CANDIDATE head after pre-ACTIVE failure: {exc}"
        ) from exc
    if str(status) not in HEAD_PROVEN_CLOSED_STATUSES:
        raise RuntimeError(
            f"{PIN_RECOVERY_REQUIRED}: head release did not prove closure after pre-ACTIVE failure: {status!r}"
        )


def _rewrite_return_value_xcom(*, context: Any | None, summary: Mapping[str, Any]) -> None:
    """Best-effort rewrite of return_value when sidecar already published without ref."""

    ti = context.get("ti") if isinstance(context, Mapping) else None
    if ti is None or not hasattr(ti, "xcom_push"):
        return
    try:
        ti.xcom_push(key="return_value", value=dict(summary))
    except Exception:  # noqa: BLE001 - return embedding still applies to method result
        pass


__all__ = [
    "build_launch_pin_ref",
    "commit_launch_pin_for_selected_pod",
    "embed_launch_pin_ref_in_summary",
    "inject_launch_pin_ref_into_defer_kwargs",
    "maybe_commit_launch_pin_for_selected_pod",
    "rehydrate_committed_launch_pin_ref",
    "remember_committed_launch_pin_ref",
]
