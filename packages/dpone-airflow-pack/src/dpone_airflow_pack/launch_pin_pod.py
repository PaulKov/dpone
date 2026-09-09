"""Kubernetes pod access for launch-pin UID hydration, gate re-fetch, cleanup."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any, Protocol

from dpone_airflow_pack.launch_pin_codes import PIN_INVALID, PIN_UNAVAILABLE
from dpone_airflow_pack.launch_pin_envelope import pod_metadata_fields

_READER_LOCK = threading.Lock()
_ACTIVE_READER: LaunchPinPodReader | None = None


class LaunchPinPodReader(Protocol):
    """Read/delete a namespaced pod by exact name (AF3-safe; no Airflow metadb)."""

    def read_pod(self, *, namespace: str, name: str, kubernetes_conn_id: str | None = None) -> Any:
        """Return the remote pod object or raise ``PIN_UNAVAILABLE``."""

    def delete_pod(
        self,
        *,
        namespace: str,
        name: str,
        expected_uid: str,
        kubernetes_conn_id: str | None = None,
    ) -> None:
        """Delete the exact pod by namespace/name/UID or raise ``PIN_UNAVAILABLE``."""


class InMemoryLaunchPinPodReader:
    """Hermetic pod registry for unit tests (inject via ``set_launch_pin_pod_reader``)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pods: dict[tuple[str, str], Any] = {}
        self.deleted: list[tuple[str, str, str]] = []

    def put(self, pod: Any) -> None:
        namespace, name, _uid = pod_metadata_fields(pod, require_uid=False)
        if not namespace or not name:
            raise RuntimeError(f"{PIN_INVALID}: cannot register pod without metadata.namespace/name")
        with self._lock:
            self._pods[(namespace, name)] = pod

    def read_pod(self, *, namespace: str, name: str, kubernetes_conn_id: str | None = None) -> Any:
        del kubernetes_conn_id
        with self._lock:
            pod = self._pods.get((namespace, name))
        if pod is None:
            raise RuntimeError(
                f"{PIN_UNAVAILABLE}: launch pin pod {namespace}/{name} is not present in the test registry"
            )
        return pod

    def delete_pod(
        self,
        *,
        namespace: str,
        name: str,
        expected_uid: str,
        kubernetes_conn_id: str | None = None,
    ) -> None:
        del kubernetes_conn_id
        with self._lock:
            pod = self._pods.get((namespace, name))
            if pod is None:
                raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin pod {namespace}/{name} was deleted or not found")
            _ns, _name, uid = pod_metadata_fields(pod, require_uid=False)
            if uid != expected_uid:
                raise RuntimeError(
                    f"{PIN_UNAVAILABLE}: launch pin pod {namespace}/{name} uid mismatch "
                    f"(pinned={expected_uid!r}, observed={uid!r})"
                )
            del self._pods[(namespace, name)]
            self.deleted.append((namespace, name, expected_uid))


class KubernetesLaunchPinPodReader:
    """Production reader/deleter via CoreV1 namespaced pod APIs."""

    def read_pod(self, *, namespace: str, name: str, kubernetes_conn_id: str | None = None) -> Any:
        try:
            api = _core_v1_api(kubernetes_conn_id)
            return api.read_namespaced_pod(name=name, namespace=namespace)
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001 - transport/API failures are UNAVAILABLE
            status = getattr(exc, "status", None)
            if status == 404:
                raise RuntimeError(
                    f"{PIN_UNAVAILABLE}: launch pin pod {namespace}/{name} was deleted or not found"
                ) from exc
            raise RuntimeError(f"{PIN_UNAVAILABLE}: failed to read launch pin pod {namespace}/{name}: {exc}") from exc

    def delete_pod(
        self,
        *,
        namespace: str,
        name: str,
        expected_uid: str,
        kubernetes_conn_id: str | None = None,
    ) -> None:
        # Read-verify UID first so we never wildcard-delete a recreated name.
        remote = self.read_pod(namespace=namespace, name=name, kubernetes_conn_id=kubernetes_conn_id)
        _ns, _name, remote_uid = pod_metadata_fields(remote, require_uid=False)
        if remote_uid != expected_uid:
            raise RuntimeError(
                f"{PIN_UNAVAILABLE}: launch pin pod {namespace}/{name} uid mismatch "
                f"(pinned={expected_uid!r}, observed={remote_uid!r})"
            )
        try:
            api = _core_v1_api(kubernetes_conn_id)
            body = {"preconditions": {"uid": expected_uid}}
            api.delete_namespaced_pod(name=name, namespace=namespace, body=body)
        except Exception as exc:  # noqa: BLE001
            status = getattr(exc, "status", None)
            if status == 404:
                return
            raise RuntimeError(f"{PIN_UNAVAILABLE}: failed to delete launch pin pod {namespace}/{name}: {exc}") from exc


def get_launch_pin_pod_reader() -> LaunchPinPodReader:
    """Return the injectable pod reader (Kubernetes default when unset)."""

    with _READER_LOCK:
        if _ACTIVE_READER is not None:
            return _ACTIVE_READER
    return KubernetesLaunchPinPodReader()


def set_launch_pin_pod_reader(reader: LaunchPinPodReader | None) -> None:
    """Install or clear the process-wide pod reader (tests only)."""

    global _ACTIVE_READER
    with _READER_LOCK:
        _ACTIVE_READER = reader


def remember_launch_pin_pod(pod: Any) -> None:
    """Register a pod with the injectable in-memory reader when present (tests)."""

    reader = get_launch_pin_pod_reader()
    put = getattr(reader, "put", None)
    if callable(put):
        put(pod)


def ensure_selected_pod_with_uid(*, pod: Any, operator: Any | None = None) -> Any:
    """Return the selected pod with a server-assigned UID.

    KPO ``get_or_create_pod`` may return the local request object for a newly
    created pod (``create_pod`` discards the API response). When ``metadata.uid``
    is missing, re-read the exact namespace+name from the API before pinning.
    """

    namespace, name, uid = pod_metadata_fields(pod, require_uid=False)
    if uid:
        remember_launch_pin_pod(pod)
        return pod
    if not name:
        raise RuntimeError(f"{PIN_INVALID}: selected pod is missing metadata.name")
    if not namespace:
        namespace = _operator_namespace(operator)
    if not namespace:
        raise RuntimeError(f"{PIN_INVALID}: selected pod is missing metadata.namespace")
    remote = _read_pod_via_operator_or_reader(
        operator=operator,
        namespace=namespace,
        name=name,
        kubernetes_conn_id=_operator_conn_id(operator),
    )
    _namespace, _name, remote_uid = pod_metadata_fields(remote, require_uid=False)
    if not remote_uid:
        raise RuntimeError(f"{PIN_UNAVAILABLE}: remote pod {namespace}/{name} has no metadata.uid after read_pod")
    remember_launch_pin_pod(remote)
    return remote


def fetch_pod_for_launch_pin(
    *,
    namespace: str,
    name: str,
    expected_uid: str,
    kubernetes_conn_id: str | None = None,
) -> Any:
    """Re-fetch the pinned pod and verify the recorded UID still matches."""

    if not namespace or not name or not expected_uid:
        raise RuntimeError(f"{PIN_INVALID}: launch pin pod ref requires namespace, name, and uid")
    remote = get_launch_pin_pod_reader().read_pod(
        namespace=namespace,
        name=name,
        kubernetes_conn_id=kubernetes_conn_id,
    )
    _ns, _name, remote_uid = pod_metadata_fields(remote, require_uid=False)
    if remote_uid != expected_uid:
        raise RuntimeError(
            f"{PIN_UNAVAILABLE}: launch pin pod {namespace}/{name} uid mismatch "
            f"(pinned={expected_uid!r}, observed={remote_uid!r})"
        )
    return remote


def delete_pod_for_launch_pin(
    *,
    namespace: str,
    name: str,
    expected_uid: str,
    kubernetes_conn_id: str | None = None,
) -> None:
    """Delete the exact pinned pod (UID precondition); no wildcard deletes."""

    result = delete_pod_for_launch_pin_typed(
        namespace=namespace,
        name=name,
        expected_uid=expected_uid,
        kubernetes_conn_id=kubernetes_conn_id,
    )
    status = str(result.get("status") or "")
    if status in {"deleted", "already_absent"}:
        return
    detail = str(result.get("detail") or status)
    raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin pod delete failed ({status}): {detail}")


def delete_pod_for_launch_pin_typed(
    *,
    namespace: str,
    name: str,
    expected_uid: str,
    kubernetes_conn_id: str | None = None,
) -> dict[str, str]:
    """Delete pinned pod; return typed outcome (deleted|already_absent|uid_mismatch|unavailable)."""

    if not namespace or not name or not expected_uid:
        raise RuntimeError(f"{PIN_INVALID}: launch pin pod delete requires namespace, name, and uid")
    reader = get_launch_pin_pod_reader()
    try:
        remote = reader.read_pod(namespace=namespace, name=name, kubernetes_conn_id=kubernetes_conn_id)
    except RuntimeError as exc:
        detail = str(exc)
        if "was deleted or not found" in detail or "not present" in detail:
            return {"status": "already_absent", "detail": detail}
        return {"status": "unavailable", "detail": detail}
    _ns, _name, remote_uid = pod_metadata_fields(remote, require_uid=False)
    if remote_uid != expected_uid:
        detail = f"launch pin pod {namespace}/{name} uid mismatch (pinned={expected_uid!r}, observed={remote_uid!r})"
        return {"status": "uid_mismatch", "detail": detail}
    try:
        reader.delete_pod(
            namespace=namespace,
            name=name,
            expected_uid=expected_uid,
            kubernetes_conn_id=kubernetes_conn_id,
        )
    except RuntimeError as exc:
        detail = str(exc)
        if "was deleted or not found" in detail or "not present" in detail:
            return {"status": "already_absent", "detail": detail}
        return {"status": "unavailable", "detail": detail}
    return {"status": "deleted"}


def pod_is_terminal_or_absent(
    *,
    namespace: str,
    name: str,
    expected_uid: str,
    kubernetes_conn_id: str | None = None,
    reader: LaunchPinPodReader | None = None,
) -> bool:
    """Return True when the exact UID is gone or in a terminal phase."""

    handle = reader or get_launch_pin_pod_reader()
    try:
        remote = handle.read_pod(namespace=namespace, name=name, kubernetes_conn_id=kubernetes_conn_id)
    except RuntimeError as exc:
        detail = str(exc)
        if "was deleted or not found" in detail or "not present" in detail:
            return True
        raise
    _ns, _name, remote_uid = pod_metadata_fields(remote, require_uid=False)
    if remote_uid and remote_uid != expected_uid:
        # Recreated under the same name — prior occurrence is gone.
        return True
    return _pod_phase(remote) in {"Succeeded", "Failed"}


def _pod_phase(pod: Any) -> str:
    if isinstance(pod, Mapping):
        status = pod.get("status")
        if isinstance(status, Mapping):
            return str(status.get("phase") or "").strip()
        return ""
    status = getattr(pod, "status", None)
    return str(getattr(status, "phase", "") or "").strip()


def _read_pod_via_operator_or_reader(
    *,
    operator: Any | None,
    namespace: str,
    name: str,
    kubernetes_conn_id: str | None,
) -> Any:
    if operator is not None:
        for attr in ("hook", "client"):
            handle = getattr(operator, attr, None)
            if handle is None:
                continue
            for method_name in ("get_pod", "read_pod", "read_namespaced_pod"):
                method = getattr(handle, method_name, None)
                if not callable(method):
                    continue
                try:
                    if method_name == "read_namespaced_pod":
                        return method(name=name, namespace=namespace)
                    return method(name, namespace)
                except TypeError:
                    try:
                        return method(name=name, namespace=namespace)
                    except Exception as exc:  # noqa: BLE001
                        raise RuntimeError(
                            f"{PIN_UNAVAILABLE}: operator.{attr}.{method_name} failed for {namespace}/{name}: {exc}"
                        ) from exc
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError(
                        f"{PIN_UNAVAILABLE}: operator.{attr}.{method_name} failed for {namespace}/{name}: {exc}"
                    ) from exc
    return get_launch_pin_pod_reader().read_pod(
        namespace=namespace,
        name=name,
        kubernetes_conn_id=kubernetes_conn_id,
    )


def _operator_namespace(operator: Any | None) -> str:
    if operator is None:
        return ""
    for attr in ("namespace", "kubernetes_namespace"):
        value = getattr(operator, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _operator_conn_id(operator: Any | None) -> str | None:
    if operator is None:
        return None
    value = getattr(operator, "kubernetes_conn_id", None)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _core_v1_api(kubernetes_conn_id: str | None = None) -> Any:
    from dpone_airflow_pack.kubernetes_runtime_client import build_core_v1_api

    return build_core_v1_api(kubernetes_conn_id=kubernetes_conn_id)


__all__ = [
    "InMemoryLaunchPinPodReader",
    "KubernetesLaunchPinPodReader",
    "LaunchPinPodReader",
    "delete_pod_for_launch_pin",
    "delete_pod_for_launch_pin_typed",
    "ensure_selected_pod_with_uid",
    "fetch_pod_for_launch_pin",
    "get_launch_pin_pod_reader",
    "pod_is_terminal_or_absent",
    "remember_launch_pin_pod",
    "set_launch_pin_pod_reader",
]
