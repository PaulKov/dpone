"""ConfigMap pin payload validation and active-owner successor fencing."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.launch_pin_codes import (
    LAUNCH_PIN_STATE_ACTIVE,
    LAUNCH_PIN_STATE_CANDIDATE,
    PIN_INVALID,
    PIN_RECOVERY_REQUIRED,
    PIN_STALE_WRITER,
    PIN_UNAVAILABLE,
)
from dpone_airflow_pack.launch_pin_k8s_meta import (
    configmap_data,
    configmap_metadata,
    object_meta_fields,
    object_meta_labels,
    object_meta_namespace,
)
from dpone_airflow_pack.strict_json import loads_strict_json_object

LAUNCH_PIN_CONFIGMAP_LABEL = "dpone.dev/launch-pin"
LAUNCH_PIN_CONFIGMAP_LABEL_VALUE = "v1"
LAUNCH_PIN_HEAD_LABEL_VALUE = "head-v1"
LAUNCH_PIN_DATA_KEY = "pin.json"
LAUNCH_PIN_HEAD_DATA_KEY = "head.json"
_POINTER_META_KEYS = frozenset({"pointer_resource_version", "state", "store_namespace", "store_authority_digest"})


def configmap_name_for_subject(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int,
    try_number: int | None = None,
) -> str:
    """Return immutable per-try ConfigMap name, or subject head when try_number is None."""

    if try_number is None:
        digest = hashlib.sha256(f"{dag_id}\0{run_id}\0{task_id}\0{int(map_index)}".encode()).hexdigest()
        return f"dpone-lph-{digest[:40]}"
    digest = hashlib.sha256(f"{dag_id}\0{run_id}\0{task_id}\0{int(map_index)}\0{int(try_number)}".encode()).hexdigest()
    return f"dpone-lp-{digest[:40]}"


def configmap_body(*, name: str, namespace: str, pin: Mapping[str, Any]) -> dict[str, Any]:
    payload = {key: value for key, value in pin.items() if key != "pointer_resource_version"}
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {LAUNCH_PIN_CONFIGMAP_LABEL: LAUNCH_PIN_CONFIGMAP_LABEL_VALUE},
        },
        "data": {
            LAUNCH_PIN_DATA_KEY: json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        },
    }


def head_configmap_body(*, name: str, namespace: str, head: Mapping[str, Any]) -> dict[str, Any]:
    payload = {key: value for key, value in head.items() if key != "pointer_resource_version"}
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {LAUNCH_PIN_CONFIGMAP_LABEL: LAUNCH_PIN_HEAD_LABEL_VALUE},
        },
        "data": {
            LAUNCH_PIN_HEAD_DATA_KEY: json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        },
    }


def require_valid_pin_from_configmap(
    body: Any,
    *,
    expected_name: str,
    expected_namespace: str,
) -> dict[str, Any]:
    metadata = configmap_metadata(body)
    resource_version, _uid, name = object_meta_fields(metadata)
    namespace = object_meta_namespace(metadata)
    labels = object_meta_labels(metadata)
    if name and name != expected_name:
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap name {name!r} does not match subject {expected_name!r}")
    if namespace and namespace != expected_namespace:
        raise RuntimeError(
            f"{PIN_INVALID}: ConfigMap namespace {namespace!r} does not match store {expected_namespace!r}"
        )
    if labels.get(LAUNCH_PIN_CONFIGMAP_LABEL) != LAUNCH_PIN_CONFIGMAP_LABEL_VALUE:
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap {expected_namespace}/{expected_name} missing launch-pin label")
    data = configmap_data(body)
    if data is None or set(data) != {LAUNCH_PIN_DATA_KEY}:
        raise RuntimeError(
            f"{PIN_INVALID}: ConfigMap {expected_namespace}/{expected_name} must carry only {LAUNCH_PIN_DATA_KEY}"
        )
    raw = data.get(LAUNCH_PIN_DATA_KEY)
    if not isinstance(raw, str) or not raw.strip():
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap {expected_namespace}/{expected_name} pin payload missing")
    try:
        payload = loads_strict_json_object(raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"{PIN_INVALID}: ConfigMap {expected_namespace}/{expected_name} pin payload is malformed"
        ) from exc
    from dpone_airflow_pack.launch_pin import launch_pin_error

    error = launch_pin_error(digest_body(payload), require_digest=True, require_identities=False)
    if error:
        raise RuntimeError(f"{PIN_INVALID}: {error}")
    expected = configmap_name_for_subject(
        dag_id=str(payload["dag_id"]),
        run_id=str(payload["run_id"]),
        task_id=str(payload["task_id"]),
        map_index=int(payload["map_index"]),
        try_number=int(payload["try_number"]),
    )
    if expected != expected_name:
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap subject hash does not match pin coordinates for {expected_name}")
    state = str(payload.get("state") or "").strip()
    if state and state not in {LAUNCH_PIN_STATE_CANDIDATE, LAUNCH_PIN_STATE_ACTIVE}:
        raise RuntimeError(f"{PIN_INVALID}: ConfigMap pin state {state!r} is unsupported")
    if not resource_version:
        raise RuntimeError(f"{PIN_UNAVAILABLE}: ConfigMap {expected_namespace}/{expected_name} missing resourceVersion")
    result = dict(payload)
    result["pointer_resource_version"] = resource_version
    result["store_namespace"] = expected_namespace
    if state:
        result["state"] = state
    return result


def with_state(pin: Mapping[str, Any], state: str, *, store_namespace: str) -> dict[str, Any]:
    payload = {key: value for key, value in pin.items() if key != "pointer_resource_version"}
    payload["state"] = state
    payload["store_namespace"] = store_namespace
    return payload


def with_pointer_meta(pin: Mapping[str, Any], source: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(pin)
    for key in _POINTER_META_KEYS:
        if key in source:
            payload[key] = source[key]
    return payload


def digest_body(pin: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in pin.items() if key not in _POINTER_META_KEYS}


def active_owner_blocks_successor(existing: Mapping[str, Any]) -> str:
    """Return an error string when a higher try must not replace the prior owner.

    Rules:
    - prior ``ACTIVE`` → always ``PIN_RECOVERY_REQUIRED`` (terminal/404 insufficient)
    - prior ``CANDIDATE`` + pod terminal/absent → successor may replace
    - prior ``CANDIDATE`` + pod still running → ``PIN_STALE_WRITER``
    """

    state = str(existing.get("state") or "").strip() or LAUNCH_PIN_STATE_ACTIVE
    if state not in {LAUNCH_PIN_STATE_ACTIVE, LAUNCH_PIN_STATE_CANDIDATE}:
        return f"{PIN_INVALID}: existing launch pin state is unsupported"
    namespace = str(existing.get("pod_namespace") or "").strip()
    name = str(existing.get("pod_name") or "").strip()
    uid = str(existing.get("pod_uid") or "").strip()
    if not namespace or not name or not uid:
        return f"{PIN_RECOVERY_REQUIRED}: prior launch pin owner coordinates are incomplete"
    if state == LAUNCH_PIN_STATE_ACTIVE:
        return (
            f"{PIN_RECOVERY_REQUIRED}: prior ACTIVE launch pin try_number={existing.get('try_number')} "
            f"pod_uid={uid!r} blocks successor CAS; terminal/404 is insufficient without "
            "explicit reconciliation receipt"
        )
    from dpone_airflow_pack.launch_pin_pod import get_launch_pin_pod_reader, pod_is_terminal_or_absent

    conn = existing.get("kubernetes_conn_id")
    kubernetes_conn_id = str(conn).strip() if isinstance(conn, str) and conn.strip() else None
    try:
        if pod_is_terminal_or_absent(
            namespace=namespace,
            name=name,
            expected_uid=uid,
            kubernetes_conn_id=kubernetes_conn_id,
            reader=get_launch_pin_pod_reader(),
        ):
            return ""
    except RuntimeError as exc:
        detail = str(exc)
        if detail.startswith(PIN_UNAVAILABLE):
            return (
                f"{PIN_RECOVERY_REQUIRED}: prior launch pin owner pod status is unreconciled "
                f"(try_number={existing.get('try_number')})"
            )
        raise
    return (
        f"{PIN_STALE_WRITER}: CANDIDATE owner try_number={existing.get('try_number')} "
        f"pod_uid={uid!r} still exists; successor CAS requires prior terminal"
    )


__all__ = [
    "LAUNCH_PIN_CONFIGMAP_LABEL",
    "LAUNCH_PIN_CONFIGMAP_LABEL_VALUE",
    "LAUNCH_PIN_DATA_KEY",
    "LAUNCH_PIN_HEAD_DATA_KEY",
    "LAUNCH_PIN_HEAD_LABEL_VALUE",
    "active_owner_blocks_successor",
    "configmap_body",
    "configmap_name_for_subject",
    "digest_body",
    "head_configmap_body",
    "require_valid_pin_from_configmap",
    "with_pointer_meta",
    "with_state",
]
