"""Production create-once launch-pin pointer store via Kubernetes ConfigMap CAS.

XCom remains a diagnostics / gate-locator mirror only. Subject authority is:

- small mutable head ConfigMap ``dpone-lph-*`` for **pre-activation** admission
- immutable per-try ConfigMap ``dpone-lp-*`` (CANDIDATE → ACTIVE)

Order: head CANDIDATE CAS → per-try CANDIDATE → verify → per-try ACTIVE →
head CANDIDATE→ACTIVE CAS. ``create_once`` succeeds only when both match.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.launch_pin_codes import (
    LAUNCH_PIN_STATE_ACTIVE,
    LAUNCH_PIN_STATE_CANDIDATE,
    PIN_CONFLICT,
    PIN_RECOVERY_REQUIRED,
    PIN_UNAVAILABLE,
)
from dpone_airflow_pack.launch_pin_k8s_head import read_launch_pin_head, release_own_head_candidate
from dpone_airflow_pack.launch_pin_k8s_head_cas import cas_activate_head, cas_admit_head_candidate
from dpone_airflow_pack.launch_pin_k8s_meta import configmap_metadata, object_meta_fields
from dpone_airflow_pack.launch_pin_k8s_occurrence import after_per_try_create_failure, delete_launch_pin_occurrence
from dpone_airflow_pack.launch_pin_k8s_validate import (
    LAUNCH_PIN_CONFIGMAP_LABEL,
    LAUNCH_PIN_CONFIGMAP_LABEL_VALUE,
    LAUNCH_PIN_DATA_KEY,
    active_owner_blocks_successor,
    configmap_body,
    configmap_name_for_subject,
    digest_body,
    require_valid_pin_from_configmap,
    with_pointer_meta,
    with_state,
)
from dpone_airflow_pack.launch_pin_store import retain_or_conflict_pin

_STORE_NAMESPACE_ENV = "DPONE_LAUNCH_PIN_STORE_NAMESPACE"
_SA_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


class KubernetesConfigMapLaunchPinStore:
    """Create-once subject authority backed by head CAS + per-try ConfigMap."""

    def __init__(
        self,
        *,
        namespace: str | None = None,
        kubernetes_conn_id: str | None = None,
        api: Any | None = None,
    ) -> None:
        self._namespace = namespace
        self._kubernetes_conn_id = kubernetes_conn_id
        self._api = api

    @property
    def kubernetes_conn_id(self) -> str | None:
        return self._kubernetes_conn_id

    def get(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        try_number: int | None = None,
    ) -> dict[str, Any] | None:
        if try_number is None:
            head = self._read_head(dag_id=dag_id, run_id=run_id, task_id=task_id, map_index=map_index)
            if head is None:
                return None
            try_number = int(head["try_number"])
        name = configmap_name_for_subject(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            try_number=int(try_number),
        )
        try:
            body, _rv = self._read(name)
        except _NotFound:
            return None
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin ConfigMap read failed: {exc}") from exc
        return require_valid_pin_from_configmap(body, expected_name=name, expected_namespace=self.namespace)

    def create_once(self, pin: Mapping[str, Any]) -> dict[str, Any]:
        try_name = configmap_name_for_subject(
            dag_id=str(pin["dag_id"]),
            run_id=str(pin["run_id"]),
            task_id=str(pin["task_id"]),
            map_index=int(pin["map_index"]),
            try_number=int(pin["try_number"]),
        )
        # 1) Pre-activation subject-head CAS → CANDIDATE(self)
        cas_admit_head_candidate(
            api=self._api_client(),
            namespace=self.namespace,
            pin=pin,
            read_fn=self._read,
            create_fn=self._create,
            replace_fn=self._replace_raw,
            not_found_exc=_NotFound,
            already_exists_exc=_AlreadyExists,
            load_prior=lambda head: self.get(
                dag_id=str(pin["dag_id"]),
                run_id=str(pin["run_id"]),
                task_id=str(pin["task_id"]),
                map_index=int(pin["map_index"]),
                try_number=int(head["try_number"]),
            ),
            successor_blocker=active_owner_blocks_successor,
        )
        # 2–3) Winner creates immutable per-try CANDIDATE and verifies reservation
        candidate = with_state(pin, LAUNCH_PIN_STATE_CANDIDATE, store_namespace=self.namespace)
        try:
            created = self._create(configmap_body(name=try_name, namespace=self.namespace, pin=candidate))
        except _AlreadyExists:
            return self._reconcile_existing_try(name=try_name, candidate=candidate)
        except Exception as exc:  # noqa: BLE001
            return self._after_per_try_create_failure(name=try_name, pin=pin, candidate=candidate, error=exc)
        created_pin = require_valid_pin_from_configmap(
            created, expected_name=try_name, expected_namespace=self.namespace
        )
        self._assert_matches_candidate(observed=created_pin, candidate=candidate)
        self._assert_head_reservation(pin)
        return self._activate_dual(name=try_name, pin=created_pin, candidate=candidate)

    def release_own_head_candidate(self, pin: Mapping[str, Any]) -> str:
        """Release own CANDIDATE head after proved-absent per-try / abandon."""

        return release_own_head_candidate(
            api=self._api_client(),
            namespace=self.namespace,
            pin=pin,
            read_fn=self._read,
            replace_fn=self._replace_raw,
            delete_fn=self._api_client().delete_namespaced_config_map,
            not_found_exc=_NotFound,
        )

    def delete_occurrence(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        pin_sha256: str,
        pointer_resource_version: str,
        pod_uid: str,
        try_number: int,
    ) -> dict[str, Any]:
        """Head ACTIVE→CONSUMED (+ readback) first, then exact per-try delete."""

        return delete_launch_pin_occurrence(
            namespace=self.namespace,
            api=self._api_client(),
            read_fn=self._read,
            replace_fn=self._replace_raw,
            not_found_exc=_NotFound,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            pin_sha256=pin_sha256,
            pointer_resource_version=pointer_resource_version,
            pod_uid=pod_uid,
            try_number=try_number,
        )

    def _after_per_try_create_failure(
        self,
        *,
        name: str,
        pin: Mapping[str, Any],
        candidate: Mapping[str, Any],
        error: Exception,
    ) -> dict[str, Any]:
        return after_per_try_create_failure(
            name=name,
            namespace=self.namespace,
            pin=pin,
            candidate=candidate,
            error=error,
            read_fn=self._read,
            not_found_exc=_NotFound,
            release_own_head=self.release_own_head_candidate,
            assert_matches_candidate=self._assert_matches_candidate,
            activate_dual=self._activate_dual,
        )

    @property
    def namespace(self) -> str:
        if self._namespace:
            return self._namespace
        return resolve_launch_pin_store_namespace()

    def _activate_dual(self, *, name: str, pin: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
        # 4) per-try → ACTIVE
        activated = self._activate_verified(name=name, pin=pin)
        self._assert_matches_candidate(observed=activated, candidate=candidate)
        # 5) head CANDIDATE → ACTIVE (exact self)
        try:
            cas_activate_head(
                api=self._api_client(),
                namespace=self.namespace,
                pin=activated,
                read_fn=self._read,
                replace_fn=self._replace_raw,
                not_found_exc=_NotFound,
            )
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: launch pin head ACTIVE publish failed: {exc}") from exc
        # Success only when both head and per-try match ACTIVE(self)
        self._assert_dual_active(activated)
        return activated

    def _assert_head_reservation(self, pin: Mapping[str, Any]) -> None:
        from dpone_airflow_pack.launch_pin_k8s_head import head_matches_pin

        head = self._read_head(
            dag_id=str(pin["dag_id"]),
            run_id=str(pin["run_id"]),
            task_id=str(pin["task_id"]),
            map_index=int(pin["map_index"]),
        )
        if not head_matches_pin(
            head,
            pin,
            states=frozenset({LAUNCH_PIN_STATE_CANDIDATE, LAUNCH_PIN_STATE_ACTIVE}),
        ):
            raise RuntimeError(
                f"{PIN_CONFLICT}: launch pin head reservation mismatch after per-try create (observed={head!r})"
            )

    def _assert_dual_active(self, pin: Mapping[str, Any]) -> None:
        from dpone_airflow_pack.launch_pin_k8s_head import head_matches_pin

        if str(pin.get("state") or "") != LAUNCH_PIN_STATE_ACTIVE:
            raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: per-try pin is not ACTIVE after create_once")
        head = self._read_head(
            dag_id=str(pin["dag_id"]),
            run_id=str(pin["run_id"]),
            task_id=str(pin["task_id"]),
            map_index=int(pin["map_index"]),
        )
        if not head_matches_pin(head, pin, states=frozenset({LAUNCH_PIN_STATE_ACTIVE})):
            raise RuntimeError(
                f"{PIN_RECOVERY_REQUIRED}: subject head is not ACTIVE(self) after create_once (observed={head!r})"
            )

    def _read_head(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
    ) -> dict[str, Any] | None:
        return read_launch_pin_head(
            api=self._api_client(),
            namespace=self.namespace,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            read_fn=self._read,
            not_found_exc=_NotFound,
        )

    def _activate_verified(self, *, name: str, pin: Mapping[str, Any]) -> dict[str, Any]:
        if str(pin.get("state") or "") == LAUNCH_PIN_STATE_ACTIVE:
            return dict(pin)
        resource_version = str(pin.get("pointer_resource_version") or "").strip()
        if not resource_version:
            raise RuntimeError(f"{PIN_UNAVAILABLE}: ConfigMap {self.namespace}/{name} missing resourceVersion")
        active = with_state(pin, LAUNCH_PIN_STATE_ACTIVE, store_namespace=self.namespace)
        try:
            return self._replace(name=name, pin=active, resource_version=resource_version)
        except Exception as exc:  # noqa: BLE001
            reconciled = self._reconcile_after_ambiguous_write(name=name, candidate=active, error=exc)
            if reconciled is not None and str(reconciled.get("state") or "") == LAUNCH_PIN_STATE_ACTIVE:
                self._assert_matches_candidate(observed=reconciled, candidate=active)
                return reconciled
            if isinstance(exc, RuntimeError):
                raise
            raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin ConfigMap activate failed: {exc}") from exc

    def _reconcile_existing_try(self, *, name: str, candidate: Mapping[str, Any]) -> dict[str, Any]:
        try:
            existing_body, _resource_version = self._read(name)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin ConfigMap re-read failed: {exc}") from exc
        existing = require_valid_pin_from_configmap(
            existing_body, expected_name=name, expected_namespace=self.namespace
        )
        retained = retain_or_conflict_pin(existing=digest_body(existing), candidate=digest_body(candidate))
        if str(existing.get("state") or "") != LAUNCH_PIN_STATE_ACTIVE:
            return self._activate_dual(name=name, pin=existing, candidate=candidate)
        result = with_pointer_meta(retained, existing)
        self._assert_dual_active(result)
        return result

    def _reconcile_after_ambiguous_write(
        self,
        *,
        name: str,
        candidate: Mapping[str, Any],
        error: Exception,
    ) -> dict[str, Any] | None:
        del error
        try:
            body, _rv = self._read(name)
        except _NotFound:
            return None
        except Exception:
            return None
        try:
            observed = require_valid_pin_from_configmap(body, expected_name=name, expected_namespace=self.namespace)
        except RuntimeError:
            return None
        try:
            self._assert_matches_candidate(observed=observed, candidate=candidate)
        except RuntimeError:
            return None
        return observed

    def _assert_matches_candidate(self, *, observed: Mapping[str, Any], candidate: Mapping[str, Any]) -> None:
        if str(observed.get("pod_uid") or "") != str(candidate.get("pod_uid") or "") or str(
            observed.get("pin_sha256") or ""
        ) != str(candidate.get("pin_sha256") or ""):
            raise RuntimeError(
                f"{PIN_CONFLICT}: launch pin ConfigMap write reconcile mismatch "
                f"(observed uid={observed.get('pod_uid')!r} digest={observed.get('pin_sha256')!r})"
            )
        for field in ("dag_id", "run_id", "task_id", "map_index", "try_number"):
            if observed.get(field) != candidate.get(field):
                raise RuntimeError(f"{PIN_CONFLICT}: launch pin ConfigMap subject field {field} mismatch after write")

    def _replace(self, *, name: str, pin: Mapping[str, Any], resource_version: str) -> dict[str, Any]:
        body = configmap_body(name=name, namespace=self.namespace, pin=pin)
        metadata = body.setdefault("metadata", {})
        metadata["resourceVersion"] = resource_version
        try:
            replaced = self._replace_raw(name=name, namespace=self.namespace, body=body)
            return require_valid_pin_from_configmap(replaced, expected_name=name, expected_namespace=self.namespace)
        except Exception as exc:  # noqa: BLE001
            status = getattr(exc, "status", None)
            if status == 409:
                raise RuntimeError(f"{PIN_CONFLICT}: launch pin ConfigMap resourceVersion conflict for {name}") from exc
            reconciled = self._reconcile_after_ambiguous_write(name=name, candidate=pin, error=exc)
            if reconciled is not None:
                return reconciled
            raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin ConfigMap replace failed: {exc}") from exc

    def _replace_raw(self, *, name: str, namespace: str, body: Mapping[str, Any]) -> Any:
        return self._api_client().replace_namespaced_config_map(name=name, namespace=namespace, body=body)

    def _create(self, body: Mapping[str, Any]) -> Any:
        try:
            return self._api_client().create_namespaced_config_map(namespace=self.namespace, body=body)
        except Exception as exc:  # noqa: BLE001
            status = getattr(exc, "status", None)
            reason = str(getattr(exc, "reason", "") or "")
            if status == 409 or "AlreadyExists" in reason or "already exists" in str(exc).lower():
                raise _AlreadyExists(str(exc)) from exc
            raise

    def _read(self, name: str) -> tuple[Any, str]:
        try:
            body = self._api_client().read_namespaced_config_map(name=name, namespace=self.namespace)
        except Exception as exc:  # noqa: BLE001
            status = getattr(exc, "status", None)
            if status == 404 or isinstance(exc, KeyError):
                raise _NotFound(str(exc)) from exc
            raise
        resource_version, _uid, _name = object_meta_fields(configmap_metadata(body))
        if not resource_version:
            raise RuntimeError(f"{PIN_UNAVAILABLE}: ConfigMap {self.namespace}/{name} missing resourceVersion")
        return body, resource_version

    def _api_client(self) -> Any:
        if self._api is not None:
            return self._api
        from dpone_airflow_pack.kubernetes_runtime_client import build_core_v1_api

        return build_core_v1_api(kubernetes_conn_id=self._kubernetes_conn_id)


def resolve_launch_pin_store_namespace() -> str:
    """Resolve the control namespace for launch-pin ConfigMaps."""

    env = str(os.environ.get(_STORE_NAMESPACE_ENV) or "").strip()
    if env:
        return env
    try:
        with open(_SA_NAMESPACE_PATH, encoding="utf-8") as handle:
            return handle.read().strip() or "default"
    except OSError:
        return "default"


class _AlreadyExists(RuntimeError):
    """ConfigMap create raced with an existing subject row."""


class _NotFound(RuntimeError):
    """ConfigMap subject row is absent."""


__all__ = [
    "LAUNCH_PIN_CONFIGMAP_LABEL",
    "LAUNCH_PIN_CONFIGMAP_LABEL_VALUE",
    "LAUNCH_PIN_DATA_KEY",
    "KubernetesConfigMapLaunchPinStore",
    "configmap_name_for_subject",
    "resolve_launch_pin_store_namespace",
]
