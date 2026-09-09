"""Launch-pin CAS barrier: base must not mutate until dual ACTIVE CAS succeeds.

Flow for separate ``outcome_gate`` runtimes when ``launch_pin_required``:

1. Pod request includes init ``dpone-launch-pin-barrier`` that waits until
   **both** subject head and immutable per-try ConfigMaps are ``ACTIVE`` for
   this try/pod_uid/pin_sha256 (+ per-try ``envelope_sha256``).
2. ``get_or_create_pod`` creates/reattaches the pod (init starts waiting).
3. Runtime: head CANDIDATE CAS → per-try CANDIDATE → ACTIVE → head ACTIVE.
4. On pre-ACTIVE CAS failure: delete the exact pod and verify termination so the
   init never releases base for unauthorized mutation.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.launch_pin_codes import PIN_UNAVAILABLE
from dpone_airflow_pack.launch_pin_envelope import pod_metadata_fields
from dpone_airflow_pack.launch_pin_k8s_store import (
    LAUNCH_PIN_DATA_KEY,
    configmap_name_for_subject,
    resolve_launch_pin_store_namespace,
)
from dpone_airflow_pack.launch_pin_k8s_validate import LAUNCH_PIN_HEAD_DATA_KEY
from dpone_airflow_pack.launch_pin_pod import delete_pod_for_launch_pin, pod_is_terminal_or_absent
from dpone_airflow_pack.xcom_sidecar import XCOM_SIDECAR_CONTAINER_NAME

LAUNCH_PIN_BARRIER_INIT_NAME = "dpone-launch-pin-barrier"

# Portable wait loop: SA token + API server. python3 is required (no weak wget).
_BARRIER_SCRIPT = """\
set -eu
UID_VALUE="${POD_UID:-}"
CM_NS="${PIN_CM_NAMESPACE:-}"
CM_NAME="${PIN_CM_NAME:-}"
HEAD_NAME="${PIN_HEAD_CM_NAME:-}"
DATA_KEY="${PIN_DATA_KEY:-pin.json}"
HEAD_KEY="${PIN_HEAD_DATA_KEY:-head.json}"
DAG_ID="${PIN_DAG_ID:-}"
RUN_ID="${PIN_RUN_ID:-}"
TASK_ID="${PIN_TASK_ID:-}"
MAP_INDEX="${PIN_MAP_INDEX:--1}"
TRY_NUMBER="${PIN_TRY_NUMBER:-}"
ENVELOPE_SHA="${PIN_ENVELOPE_SHA256:-}"
if [ -z "$UID_VALUE" ] || [ -z "$CM_NS" ] || [ -z "$CM_NAME" ] || [ -z "$HEAD_NAME" ] || [ -z "$DAG_ID" ] || [ -z "$RUN_ID" ] || [ -z "$TASK_ID" ] || [ -z "$TRY_NUMBER" ] || [ -z "$ENVELOPE_SHA" ]; then
  echo "dpone-launch-pin-barrier: missing POD_UID / PIN_CM_* / head / subject / try / envelope digest" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "dpone-launch-pin-barrier: python3 is required (wget fallback removed)" >&2
  exit 1
fi
HOST="${KUBERNETES_SERVICE_HOST:-}"
PORT="${KUBERNETES_SERVICE_PORT:-443}"
BASE="https://${HOST}:${PORT}/api/v1/namespaces/${CM_NS}/configmaps"
export BARRIER_TRY_URL="${BASE}/${CM_NAME}" BARRIER_HEAD_URL="${BASE}/${HEAD_NAME}"
export POD_UID="$UID_VALUE" PIN_DATA_KEY="$DATA_KEY" PIN_HEAD_DATA_KEY="$HEAD_KEY"
export PIN_DAG_ID="$DAG_ID" PIN_RUN_ID="$RUN_ID" PIN_TASK_ID="$TASK_ID" PIN_MAP_INDEX="$MAP_INDEX"
export PIN_TRY_NUMBER="$TRY_NUMBER" PIN_ENVELOPE_SHA256="$ENVELOPE_SHA"
i=0
while [ "$i" -lt 600 ]; do
  i=$((i + 1))
  if python3 - <<'PY'
import json, os, ssl, sys, urllib.request
uid = os.environ["POD_UID"]
try_url = os.environ["BARRIER_TRY_URL"]
head_url = os.environ["BARRIER_HEAD_URL"]
key = os.environ.get("PIN_DATA_KEY") or "pin.json"
head_key = os.environ.get("PIN_HEAD_DATA_KEY") or "head.json"
want = {
    "dag_id": os.environ["PIN_DAG_ID"],
    "run_id": os.environ["PIN_RUN_ID"],
    "task_id": os.environ["PIN_TASK_ID"],
    "map_index": int(os.environ.get("PIN_MAP_INDEX") or "-1"),
    "try_number": int(os.environ["PIN_TRY_NUMBER"]),
    "envelope_sha256": os.environ["PIN_ENVELOPE_SHA256"],
}
token = open("/var/run/secrets/kubernetes.io/serviceaccount/token", encoding="utf-8").read().strip()
ctx = ssl.create_default_context(cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
headers = {"Authorization": f"Bearer {token}"}
def _load(url, data_key):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, context=ctx, timeout=5) as resp:
        cm = json.loads(resp.read().decode("utf-8"))
    return json.loads((cm.get("data") or {}).get(data_key) or "{}")
try:
    pin = _load(try_url, key)
    head = _load(head_url, head_key)
except Exception:
    sys.exit(1)
digest = str(pin.get("pin_sha256") or "")
env_digest = str(pin.get("envelope_sha256") or "")
try_ok = (
    str(pin.get("state") or "") == "ACTIVE"
    and str(pin.get("pod_uid") or "") == uid
    and digest.startswith("sha256:")
    and len(digest) == 71
    and env_digest == want["envelope_sha256"]
    and str(pin.get("dag_id") or "") == want["dag_id"]
    and str(pin.get("run_id") or "") == want["run_id"]
    and str(pin.get("task_id") or "") == want["task_id"]
    and int(pin.get("map_index", -1)) == want["map_index"]
    and int(pin.get("try_number", -1)) == want["try_number"]
)
head_ok = (
    str(head.get("state") or "") == "ACTIVE"
    and str(head.get("pod_uid") or "") == uid
    and str(head.get("pin_sha256") or "") == digest
    and int(head.get("try_number", -1)) == want["try_number"]
)
sys.exit(0 if (try_ok and head_ok) else 1)
PY
  then
    echo "dpone-launch-pin-barrier: ACTIVE head+per-try observed for uid=${UID_VALUE}"
    exit 0
  fi
  sleep 1
done
echo "dpone-launch-pin-barrier: timed out waiting for ACTIVE head+per-try uid=${UID_VALUE}" >&2
exit 1
"""


def attach_launch_pin_cas_barrier(*, pod: Any, operator: Any, context: Any | None) -> Any:
    """Resolve TI coordinates from operator/context and inject the CAS barrier."""

    from dpone_airflow_pack.launch_pin_envelope import envelope_sha256
    from dpone_airflow_pack.launch_pin_locator import frozen_launch_pin_store_locator

    ti = context.get("ti") if isinstance(context, Mapping) else None
    dag_id = str(getattr(ti, "dag_id", "") or getattr(operator, "dag_id", "") or "")
    run_id = str(
        getattr(ti, "run_id", "")
        or getattr(ti, "dag_run_id", "")
        or (context.get("run_id") if isinstance(context, Mapping) else "")
        or ""
    )
    task_id = str(getattr(ti, "task_id", "") or getattr(operator, "task_id", "") or "")
    map_index = int(getattr(ti, "map_index", -1) if ti is not None else -1)
    try_number = int(getattr(ti, "try_number", 1) if ti is not None else 1) or 1
    if not dag_id or not run_id or not task_id:
        # Parse-time / dependency-light paths may lack TI coordinates; barrier is
        # attached once execute-time context is available on a later rebuild.
        return pod
    locator = frozen_launch_pin_store_locator(getattr(operator, "launch_pin_store", None))
    store_namespace = locator.namespace or resolve_launch_pin_store_namespace()
    env_digest = envelope_sha256(
        run_identity=getattr(operator, "expected_run_identity", None),
        deployment_identity=getattr(operator, "expected_deployment_identity", None),
        expected_runtime_evidence_sha256=getattr(operator, "expected_runtime_evidence_sha256", None),
    )
    return ensure_launch_pin_cas_barrier_on_pod(
        pod,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        try_number=try_number,
        store_namespace=store_namespace,
        envelope_sha256_digest=env_digest,
    )


def ensure_launch_pin_cas_barrier_on_pod(
    pod: Any,
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int = -1,
    try_number: int = 1,
    store_namespace: str | None = None,
    envelope_sha256_digest: str | None = None,
) -> Any:
    """Inject the CAS wait init container + downward API POD_UID env."""

    cm_name = configmap_name_for_subject(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        try_number=int(try_number),
    )
    head_name = configmap_name_for_subject(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        try_number=None,
    )
    cm_namespace = store_namespace or resolve_launch_pin_store_namespace()
    if not envelope_sha256_digest:
        raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin barrier requires envelope_sha256")
    _set_annotation(pod, "dpone.airflow/launch-pin-cas-barrier", "v1")
    _set_annotation(pod, "dpone.airflow/launch-pin-pointer-configmap", f"{cm_namespace}/{cm_name}")
    _set_annotation(pod, "dpone.airflow/launch-pin-head-configmap", f"{cm_namespace}/{head_name}")
    init = {
        "name": LAUNCH_PIN_BARRIER_INIT_NAME,
        "image": _barrier_image(pod),
        "imagePullPolicy": "IfNotPresent",
        "command": ["/bin/sh", "-ec", _BARRIER_SCRIPT],
        "env": [
            {
                "name": "POD_UID",
                "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}},
            },
            {"name": "PIN_CM_NAME", "value": cm_name},
            {"name": "PIN_HEAD_CM_NAME", "value": head_name},
            {"name": "PIN_CM_NAMESPACE", "value": cm_namespace},
            {"name": "PIN_DATA_KEY", "value": LAUNCH_PIN_DATA_KEY},
            {"name": "PIN_HEAD_DATA_KEY", "value": LAUNCH_PIN_HEAD_DATA_KEY},
            {"name": "PIN_DAG_ID", "value": dag_id},
            {"name": "PIN_RUN_ID", "value": run_id},
            {"name": "PIN_TASK_ID", "value": task_id},
            {"name": "PIN_MAP_INDEX", "value": str(int(map_index))},
            {"name": "PIN_TRY_NUMBER", "value": str(int(try_number))},
            {"name": "PIN_ENVELOPE_SHA256", "value": str(envelope_sha256_digest)},
        ],
    }
    _upsert_init_container(pod, init)
    return pod


def abandon_pod_after_cas_failure(
    *,
    pod: Any,
    kubernetes_conn_id: str | None,
    detail: str,
) -> bool:
    """Exact delete + termination verify. Returns True only when prior is gone/terminal."""

    del detail  # retained for call-site diagnostics / logging by the caller
    try:
        namespace, name, uid = pod_metadata_fields(pod, require_uid=True)
    except RuntimeError:
        return False
    if not namespace:
        return False
    try:
        delete_pod_for_launch_pin(
            namespace=namespace,
            name=name,
            expected_uid=uid,
            kubernetes_conn_id=kubernetes_conn_id,
        )
    except Exception:  # noqa: BLE001 - still require termination proof below
        pass
    try:
        return pod_is_terminal_or_absent(
            namespace=namespace,
            name=name,
            expected_uid=uid,
            kubernetes_conn_id=kubernetes_conn_id,
        )
    except Exception:  # noqa: BLE001
        return False


def _barrier_image(pod: Any) -> str:
    for container in _containers(pod, init=False):
        if _name(container) == "base":
            image = _image(container)
            if image:
                return image
    for container in _containers(pod, init=False):
        if _name(container) == XCOM_SIDECAR_CONTAINER_NAME:
            continue
        image = _image(container)
        if image:
            return image
    return "public.ecr.aws/docker/library/busybox:1.36"


def _upsert_init_container(pod: Any, init: Mapping[str, Any]) -> None:
    if isinstance(pod, dict):
        spec = pod.setdefault("spec", {})
        if not isinstance(spec, dict):
            raise RuntimeError(f"{PIN_UNAVAILABLE}: pod.spec must be a mutable mapping")
        existing = list(spec.get("initContainers") or [])
        existing = [item for item in existing if _name(item) != LAUNCH_PIN_BARRIER_INIT_NAME]
        existing.insert(0, dict(init))
        spec["initContainers"] = existing
        return
    spec = getattr(pod, "spec", None)
    if spec is None:
        raise RuntimeError(f"{PIN_UNAVAILABLE}: selected pod is missing spec")
    existing = list(getattr(spec, "init_containers", None) or [])
    existing = [item for item in existing if _name(item) != LAUNCH_PIN_BARRIER_INIT_NAME]
    existing.insert(0, dict(init))
    spec.init_containers = existing


def _containers(pod: Any, *, init: bool) -> list[Any]:
    if isinstance(pod, dict):
        raw_spec = pod.get("spec")
        if not isinstance(raw_spec, Mapping):
            return []
        key = "initContainers" if init else "containers"
        return list(raw_spec.get(key) or [])
    spec = getattr(pod, "spec", None)
    attr = "init_containers" if init else "containers"
    return list(getattr(spec, attr, None) or [])


def _name(container: Any) -> str:
    if isinstance(container, Mapping):
        return str(container.get("name") or "")
    return str(getattr(container, "name", "") or "")


def _image(container: Any) -> str:
    if isinstance(container, Mapping):
        return str(container.get("image") or "").strip()
    return str(getattr(container, "image", "") or "").strip()


def _set_annotation(pod: Any, key: str, value: str) -> None:
    if isinstance(pod, dict):
        metadata = pod.setdefault("metadata", {})
        if not isinstance(metadata, dict):
            raise RuntimeError(f"{PIN_UNAVAILABLE}: pod metadata must be a mutable mapping")
        annotations = metadata.setdefault("annotations", {})
        if not isinstance(annotations, dict):
            raise RuntimeError(f"{PIN_UNAVAILABLE}: pod metadata.annotations must be a mutable mapping")
        annotations[key] = value
        return
    metadata = getattr(pod, "metadata", None)
    if metadata is None:
        raise RuntimeError(f"{PIN_UNAVAILABLE}: selected pod is missing metadata")
    annotations = getattr(metadata, "annotations", None)
    if annotations is None:
        metadata.annotations = {key: value}
        return
    if isinstance(annotations, dict):
        annotations[key] = value
        return
    if isinstance(annotations, Mapping):
        metadata.annotations = {**dict(annotations), key: value}
        return
    raise RuntimeError(f"{PIN_UNAVAILABLE}: pod metadata.annotations must be a mutable mapping")


__all__ = [
    "LAUNCH_PIN_BARRIER_INIT_NAME",
    "abandon_pod_after_cas_failure",
    "attach_launch_pin_cas_barrier",
    "ensure_launch_pin_cas_barrier_on_pod",
]
