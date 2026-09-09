"""Pod-bound launch pin for separate outcome_gate expectations.

Authority choice **C**: the Kubernetes pod annotation/env is the immutable
launch-envelope authority. After ``get_or_create_pod`` (+ ``read_pod`` when the
local request lacks ``metadata.uid``), the runtime task records a pod-ref pin
(namespace/name/uid) through one explicitly selected backend:

- injectable ``LaunchPinStore`` (hermetic tests)
- compatibility default: Kubernetes ConfigMap ``resourceVersion`` CAS
  (``KubernetesConfigMapLaunchPinStore``)
- Airflow 3.3 / CNCF provider 10.20 durable KPO task state, with the live Pod
  UID/envelope retained as authority

XCom key ``dpone_runtime_launch_pin`` is never envelope authority. It is a
diagnostic mirror for ConfigMap CAS and a bounded locator claim for the task
state backend; the gate accepts it only after exact live-Pod UID/envelope
verification. Direct Airflow metadata DB access is not used.

Pod lifetime for separate outcome_gate: forced ``on_finish_action=keep_pod``,
gate consumes the live pod, then ``launch_pin_cleanup`` deletes the exact
namespace/name/UID. Bounded retention if the gate never starts is owned by the
platform runtime-pod-retention sweeper (creation-age floor).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.deployment_identity import deployment_identity_error
from dpone_airflow_pack.launch_pin_codes import (
    PIN_CONFLICT,
    PIN_INVALID,
    PIN_MISSING,
    PIN_RECOVERY_REQUIRED,
    PIN_STALE_WRITER,
    PIN_UNAVAILABLE,
)
from dpone_airflow_pack.launch_pin_commit import (
    commit_launch_pin_for_selected_pod,
    maybe_commit_launch_pin_for_selected_pod,
)
from dpone_airflow_pack.launch_pin_envelope import (
    AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV,
    AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION,
    ensure_launch_envelope_on_pod,
    envelope_sha256,
    launch_envelope_from_pod,
)
from dpone_airflow_pack.launch_pin_locator import (
    frozen_launch_pin_store_locator,
    in_cluster_service_account_present,
    remote_store_allowed,
    resolve_launch_pin_store_locator,
)
from dpone_airflow_pack.launch_pin_resolve import (
    AIRFLOW_RUN_PINNED_DEPLOYMENT_IDENTITY_XCOM_KEY,
    AIRFLOW_RUNTIME_LAUNCH_PIN_SCHEMA,
    AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY,
    LaunchPinResolution,
    raise_for_launch_pin_resolution,
    resolve_launch_pin,
)
from dpone_airflow_pack.launch_pin_store import get_launch_pin_store, retain_or_conflict_pin
from dpone_airflow_pack.outcome_identity import is_sha256, run_identity_error

_PIN_BODY_FIELDS = (
    "schema",
    "dag_id",
    "run_id",
    "task_id",
    "map_index",
    "try_number",
    "pod_namespace",
    "pod_name",
    "pod_uid",
    "kubernetes_conn_id",
    "run_identity",
    "deployment_identity",
    "expected_runtime_evidence_sha256",
    "envelope_sha256",
)


def launch_pin_required(pack: Mapping[str, Any]) -> bool:
    """Return whether the pack contract demands a launch pin for outcome_gate."""

    direct = pack.get("deployment_identity_pin")
    if isinstance(direct, Mapping) and "required" in direct:
        return bool(direct.get("required"))
    outcome = pack.get("outcome_gate")
    if isinstance(outcome, Mapping):
        nested = outcome.get("deployment_identity_pin")
        if isinstance(nested, Mapping) and "required" in nested:
            return bool(nested.get("required"))
    return isinstance(pack.get("_dpone_deployment_identity"), Mapping)


def authoritative_launch_pin_store(
    *,
    kubernetes_conn_id: str | None = None,
    namespace: str | None = None,
    allow_remote: bool = True,
) -> Any:
    """Return injectable store or production ConfigMap CAS store.

    When the Kubernetes client is not importable, or remote I/O is disallowed /
    lacks a protected locator (no conn_id and not in-cluster), return a
    miss-only store so ``required=false`` gates never open kube-config.
    ``create_once`` remains fail-closed.
    """

    injected = get_launch_pin_store()
    if injected is not None:
        return injected
    if not allow_remote or not _kubernetes_client_available():
        return _MissOnlyLaunchPinStore()
    # Prefer pack-frozen locator coordinates (no per-worker env re-resolution).
    if kubernetes_conn_id is not None or namespace is not None:
        locator = frozen_launch_pin_store_locator({"kubernetes_conn_id": kubernetes_conn_id, "namespace": namespace})
    else:
        locator = resolve_launch_pin_store_locator()
    if not remote_store_allowed(locator) and not in_cluster_service_account_present():
        return _MissOnlyLaunchPinStore()
    from dpone_airflow_pack.launch_pin_k8s_store import KubernetesConfigMapLaunchPinStore

    return KubernetesConfigMapLaunchPinStore(
        kubernetes_conn_id=locator.kubernetes_conn_id,
        namespace=locator.namespace,
    )


def _kubernetes_client_available() -> bool:
    try:
        from importlib.util import find_spec

        return find_spec("kubernetes") is not None and find_spec("kubernetes.client") is not None
    except (ImportError, ValueError):
        return False


class _MissOnlyLaunchPinStore:
    """Hermetic fallback when production ConfigMap CAS cannot be constructed."""

    def get(self, **_: Any) -> None:
        return None

    def create_once(self, pin: Mapping[str, Any]) -> dict[str, Any]:
        del pin
        raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin ConfigMap CAS requires the kubernetes Python client")


def build_launch_pin(
    *,
    attempt: Mapping[str, Any],
    pod_name: str,
    pod_uid: str,
    run_identity: Mapping[str, Any] | None,
    deployment_identity: Mapping[str, Any] | None,
    expected_runtime_evidence_sha256: str | None,
    pod_namespace: str = "default",
    kubernetes_conn_id: str | None = None,
) -> dict[str, Any]:
    """Build a validated launch pin payload including integrity digest."""

    payload: dict[str, Any] = {
        "schema": AIRFLOW_RUNTIME_LAUNCH_PIN_SCHEMA,
        "dag_id": str(attempt["dag_id"]),
        "run_id": str(attempt["run_id"]),
        "task_id": str(attempt["task_id"]),
        "map_index": int(attempt["map_index"]),
        "try_number": int(attempt["try_number"]),
        "pod_namespace": str(pod_namespace),
        "pod_name": pod_name,
        "pod_uid": pod_uid,
        "kubernetes_conn_id": kubernetes_conn_id,
        "run_identity": dict(run_identity) if isinstance(run_identity, Mapping) else None,
        "deployment_identity": (
            {key: str(deployment_identity[key]) for key in ("schema", "release_id", "deployment_id", "activation_id")}
            if isinstance(deployment_identity, Mapping)
            else None
        ),
        "expected_runtime_evidence_sha256": expected_runtime_evidence_sha256,
        "envelope_sha256": envelope_sha256(
            run_identity=run_identity,
            deployment_identity=deployment_identity,
            expected_runtime_evidence_sha256=expected_runtime_evidence_sha256,
        ),
    }
    error = launch_pin_error(payload, require_digest=False)
    if error:
        raise RuntimeError(f"{PIN_INVALID}: {error}")
    payload["pin_sha256"] = _pin_digest(payload)
    return payload


def launch_pin_error(
    value: object,
    *,
    require_digest: bool = True,
    require_identities: bool = False,
) -> str:
    """Return a diagnostic when a launch pin payload is structurally invalid."""

    if not isinstance(value, Mapping):
        return "launch pin must be an object"
    required_fields = set(_PIN_BODY_FIELDS)
    if require_digest:
        required_fields.add("pin_sha256")
    missing = sorted(field for field in required_fields if field not in value)
    if missing:
        return f"launch pin is missing fields: {', '.join(missing)}"
    if value.get("schema") != AIRFLOW_RUNTIME_LAUNCH_PIN_SCHEMA:
        return "launch pin schema is invalid"
    for field in ("dag_id", "run_id", "task_id", "pod_namespace", "pod_name", "pod_uid"):
        if not isinstance(value.get(field), str) or not str(value.get(field)).strip():
            return f"launch pin.{field} must be a non-empty string"
    conn_id = value.get("kubernetes_conn_id")
    if conn_id is not None and (not isinstance(conn_id, str) or not conn_id.strip()):
        return "launch pin.kubernetes_conn_id must be null or a non-empty string"
    if not isinstance(value.get("map_index"), int) or isinstance(value.get("map_index"), bool):
        return "launch pin.map_index must be an integer"
    if not isinstance(value.get("try_number"), int) or isinstance(value.get("try_number"), bool):
        return "launch pin.try_number must be an integer"
    if int(value["try_number"]) < 1:
        return "launch pin.try_number must be >= 1"
    run_identity = value.get("run_identity")
    if require_identities and run_identity is None:
        return "launch pin.run_identity must be non-null when required=true"
    if run_identity is not None:
        error = run_identity_error(run_identity)
        if error:
            return f"launch pin.run_identity {error}"
    deployment_identity = value.get("deployment_identity")
    if require_identities and deployment_identity is None:
        return "launch pin.deployment_identity must be non-null when required=true"
    if deployment_identity is not None:
        error = deployment_identity_error(deployment_identity)
        if error:
            return f"launch pin.deployment_identity {error}"
    digest = value.get("expected_runtime_evidence_sha256")
    if digest is not None and not is_sha256(digest):
        return "launch pin.expected_runtime_evidence_sha256 must be null or sha256 digest"
    env_digest = value.get("envelope_sha256")
    if not is_sha256(env_digest):
        return "launch pin.envelope_sha256 must be a canonical sha256 digest"
    expected_env = envelope_sha256(
        run_identity=value.get("run_identity") if isinstance(value.get("run_identity"), Mapping) else None,
        deployment_identity=(
            value.get("deployment_identity") if isinstance(value.get("deployment_identity"), Mapping) else None
        ),
        expected_runtime_evidence_sha256=digest if isinstance(digest, str) else None,
    )
    if env_digest != expected_env:
        return "launch pin.envelope_sha256 does not match envelope fields"
    if require_digest:
        observed = value.get("pin_sha256")
        if not is_sha256(observed):
            return "launch pin.pin_sha256 must be a canonical sha256 digest"
        body = {key: value.get(key) for key in _PIN_BODY_FIELDS}
        if observed != _pin_digest(body):
            return "launch pin.pin_sha256 does not match payload"
    return ""


def _pin_digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        {key: payload.get(key) for key in _PIN_BODY_FIELDS},
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


# Re-export retain helper used by store modules / tests.
_retain_or_conflict_pointer = retain_or_conflict_pin


__all__ = [
    "AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV",
    "AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION",
    "AIRFLOW_RUNTIME_LAUNCH_PIN_SCHEMA",
    "AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY",
    "AIRFLOW_RUN_PINNED_DEPLOYMENT_IDENTITY_XCOM_KEY",
    "LaunchPinResolution",
    "PIN_CONFLICT",
    "PIN_INVALID",
    "PIN_MISSING",
    "PIN_RECOVERY_REQUIRED",
    "PIN_STALE_WRITER",
    "PIN_UNAVAILABLE",
    "authoritative_launch_pin_store",
    "build_launch_pin",
    "commit_launch_pin_for_selected_pod",
    "ensure_launch_envelope_on_pod",
    "launch_envelope_from_pod",
    "launch_pin_error",
    "launch_pin_required",
    "maybe_commit_launch_pin_for_selected_pod",
    "raise_for_launch_pin_resolution",
    "resolve_launch_pin",
]
