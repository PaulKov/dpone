"""Subject-head ConfigMap read/match helpers for launch-pin admission."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone_airflow_pack.launch_pin_codes import (
    LAUNCH_PIN_STATE_ACTIVE,
    LAUNCH_PIN_STATE_CANDIDATE,
    LAUNCH_PIN_STATE_CONSUMED,
    PIN_INVALID,
    PIN_RECOVERY_REQUIRED,
    PIN_UNAVAILABLE,
)
from dpone_airflow_pack.launch_pin_k8s_meta import (
    configmap_data,
    configmap_metadata,
    object_meta_labels,
    object_meta_namespace,
)
from dpone_airflow_pack.launch_pin_k8s_validate import (
    LAUNCH_PIN_CONFIGMAP_LABEL,
    LAUNCH_PIN_HEAD_DATA_KEY,
    LAUNCH_PIN_HEAD_LABEL_VALUE,
    configmap_name_for_subject,
    head_configmap_body,
)
from dpone_airflow_pack.strict_json import loads_strict_json_object

ReadFn = Callable[[str], tuple[Any, str]]
ReplaceFn = Callable[..., Any]


def head_payload_from_pin(
    pin: Mapping[str, Any],
    *,
    state: str,
    namespace: str,
) -> dict[str, Any]:
    """Build the small mutable subject-head payload for a pin reservation."""

    return {
        "dag_id": pin["dag_id"],
        "run_id": pin["run_id"],
        "task_id": pin["task_id"],
        "map_index": int(pin["map_index"]),
        "try_number": int(pin["try_number"]),
        "pod_uid": str(pin["pod_uid"]),
        "pin_sha256": str(pin["pin_sha256"]),
        "state": state,
        "store_namespace": namespace,
    }


def head_matches_pin(
    head: Mapping[str, Any] | None,
    pin: Mapping[str, Any],
    *,
    states: frozenset[str] | None = None,
) -> bool:
    """Return True when head try/uid/digest (+ optional state) match the pin."""

    if head is None:
        return False
    if (
        int(head.get("try_number", -1) or -1) != int(pin["try_number"])
        or str(head.get("pod_uid") or "") != str(pin["pod_uid"])
        or str(head.get("pin_sha256") or "") != str(pin["pin_sha256"])
    ):
        return False
    if states is not None and str(head.get("state") or "") not in states:
        return False
    return True


def read_launch_pin_head(
    *,
    api: Any,
    namespace: str,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int,
    read_fn: ReadFn,
    not_found_exc: type[BaseException],
) -> dict[str, Any] | None:
    del api  # reserved for future direct clients; store passes read_fn
    name = configmap_name_for_subject(
        dag_id=dag_id, run_id=run_id, task_id=task_id, map_index=map_index, try_number=None
    )
    try:
        body, resource_version = read_fn(name)
    except not_found_exc:
        return None
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin head read failed: {exc}") from exc
    metadata = configmap_metadata(body)
    labels = object_meta_labels(metadata)
    observed_ns = object_meta_namespace(metadata)
    if observed_ns and observed_ns != namespace:
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap namespace {observed_ns!r} does not match store {namespace!r}")
    if labels.get(LAUNCH_PIN_CONFIGMAP_LABEL) != LAUNCH_PIN_HEAD_LABEL_VALUE:
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap {namespace}/{name} missing launch-pin head label")
    data = configmap_data(body)
    if data is None or set(data) != {LAUNCH_PIN_HEAD_DATA_KEY}:
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap {namespace}/{name} must carry only {LAUNCH_PIN_HEAD_DATA_KEY}")
    raw = data.get(LAUNCH_PIN_HEAD_DATA_KEY)
    if not isinstance(raw, str) or not raw.strip():
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap {namespace}/{name} head payload missing")
    try:
        payload = loads_strict_json_object(raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap {namespace}/{name} head payload is malformed") from exc
    for field in ("try_number", "pod_uid", "pin_sha256", "state"):
        if field not in payload:
            raise RuntimeError(f"{PIN_INVALID}: launch pin head missing {field}")
    result = dict(payload)
    result["pointer_resource_version"] = resource_version
    return result


def mark_head_consumed_or_delete(
    *,
    api: Any,
    namespace: str,
    pin: Mapping[str, Any],
    read_fn: ReadFn,
    replace_fn: ReplaceFn,
    delete_fn: Callable[..., Any],
    not_found_exc: type[BaseException],
) -> str:
    """CAS head ACTIVE(self)→CONSUMED with readback (or exact RV delete)."""

    return _transition_own_head(
        api=api,
        namespace=namespace,
        pin=pin,
        read_fn=read_fn,
        replace_fn=replace_fn,
        delete_fn=delete_fn,
        not_found_exc=not_found_exc,
        accept_states=frozenset({LAUNCH_PIN_STATE_ACTIVE, LAUNCH_PIN_STATE_CONSUMED}),
    )


def release_own_head_candidate(
    *,
    api: Any,
    namespace: str,
    pin: Mapping[str, Any],
    read_fn: ReadFn,
    replace_fn: ReplaceFn,
    delete_fn: Callable[..., Any],
    not_found_exc: type[BaseException],
) -> str:
    """Release own pre-ACTIVE head so a successor try can admit.

    Proved own ``CANDIDATE`` → CAS ``CONSUMED`` or exact RV delete.
    Ambiguous / foreign head → ``PIN_RECOVERY_REQUIRED``.
    """

    existing = read_launch_pin_head(
        api=api,
        namespace=namespace,
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        read_fn=read_fn,
        not_found_exc=not_found_exc,
    )
    if existing is None:
        return "absent"
    if head_matches_pin(existing, pin, states=frozenset({LAUNCH_PIN_STATE_CONSUMED})):
        return "already_consumed"
    if not head_matches_pin(existing, pin, states=frozenset({LAUNCH_PIN_STATE_CANDIDATE})):
        raise RuntimeError(
            f"{PIN_RECOVERY_REQUIRED}: cannot release non-own/ambiguous head after pre-ACTIVE "
            f"failure (observed={existing!r})"
        )
    return _transition_own_head(
        api=api,
        namespace=namespace,
        pin=pin,
        read_fn=read_fn,
        replace_fn=replace_fn,
        delete_fn=delete_fn,
        not_found_exc=not_found_exc,
        accept_states=frozenset({LAUNCH_PIN_STATE_CANDIDATE, LAUNCH_PIN_STATE_CONSUMED}),
    )


def _transition_own_head(
    *,
    api: Any,
    namespace: str,
    pin: Mapping[str, Any],
    read_fn: ReadFn,
    replace_fn: ReplaceFn,
    delete_fn: Callable[..., Any],
    not_found_exc: type[BaseException],
    accept_states: frozenset[str],
) -> str:
    head_name = configmap_name_for_subject(
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        try_number=None,
    )
    existing = read_launch_pin_head(
        api=api,
        namespace=namespace,
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        read_fn=read_fn,
        not_found_exc=not_found_exc,
    )
    if existing is None:
        return "absent"
    if not head_matches_pin(existing, pin, states=accept_states):
        return "skipped_mismatch"
    if str(existing.get("state") or "") == LAUNCH_PIN_STATE_CONSUMED:
        return "already_consumed"
    desired = head_payload_from_pin(pin, state=LAUNCH_PIN_STATE_CONSUMED, namespace=namespace)
    body = head_configmap_body(name=head_name, namespace=namespace, head=desired)
    body["metadata"]["resourceVersion"] = str(existing["pointer_resource_version"])
    retryable_states = accept_states - {LAUNCH_PIN_STATE_CONSUMED}

    def _readback_after_conflict() -> dict[str, Any] | None:
        return read_launch_pin_head(
            api=api,
            namespace=namespace,
            dag_id=str(pin["dag_id"]),
            run_id=str(pin["run_id"]),
            task_id=str(pin["task_id"]),
            map_index=int(pin["map_index"]),
            read_fn=read_fn,
            not_found_exc=not_found_exc,
        )

    def _finalize_after_retry(*, observed_after: dict[str, Any] | None, retry_succeeded: bool) -> str:
        if head_matches_pin(observed_after, pin, states=frozenset({LAUNCH_PIN_STATE_CONSUMED})):
            if retry_succeeded:
                prior = str(existing.get("state") or "")
                if prior == LAUNCH_PIN_STATE_CONSUMED:
                    return "already_consumed"
                return "consumed"
            return "already_consumed"
        return "skipped_conflict"

    try:
        replace_fn(name=head_name, namespace=namespace, body=body)
    except Exception as exc:  # noqa: BLE001
        status = getattr(exc, "status", None)
        if status == 409:
            observed = _readback_after_conflict()
            if head_matches_pin(observed, pin, states=frozenset({LAUNCH_PIN_STATE_CONSUMED})):
                return "already_consumed"
            if observed is not None and head_matches_pin(observed, pin, states=retryable_states):
                retry_body = head_configmap_body(name=head_name, namespace=namespace, head=desired)
                retry_body["metadata"]["resourceVersion"] = str(observed["pointer_resource_version"])
                try:
                    replace_fn(name=head_name, namespace=namespace, body=retry_body)
                except Exception:
                    return _finalize_after_retry(
                        observed_after=_readback_after_conflict(),
                        retry_succeeded=False,
                    )
                return _finalize_after_retry(
                    observed_after=_readback_after_conflict(),
                    retry_succeeded=True,
                )
            return "skipped_conflict"
        if status == 404:
            return "absent"
        try:
            delete_fn(
                name=head_name,
                namespace=namespace,
                body={"preconditions": {"resourceVersion": str(existing["pointer_resource_version"])}},
            )
            return "deleted"
        except Exception:
            return "cleanup_failed"
    observed = read_launch_pin_head(
        api=api,
        namespace=namespace,
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        read_fn=read_fn,
        not_found_exc=not_found_exc,
    )
    if head_matches_pin(observed, pin, states=frozenset({LAUNCH_PIN_STATE_CONSUMED})):
        return "consumed"
    return "cleanup_failed"


__all__ = [
    "head_matches_pin",
    "head_payload_from_pin",
    "mark_head_consumed_or_delete",
    "read_launch_pin_head",
    "release_own_head_candidate",
]
