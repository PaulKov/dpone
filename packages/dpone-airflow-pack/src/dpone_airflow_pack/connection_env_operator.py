"""Native base-container-only Airflow Connection environment transport."""

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from copy import deepcopy
from functools import cached_property
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

from dpone_airflow_pack.connection_env_projection import require_connection_env_projection
from dpone_airflow_pack.connection_projection import (
    AirflowBaseHookConnectionReader,
    ConnectionUriReader,
    ProjectedAirflowConnectionUriReader,
    airflow_conn_env_name,
)
from dpone_airflow_pack.operator_runtime import patch_pod_spec_env_vars
from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator


class AirflowConnectionEnvKubernetesPodOperator(PinnedXComSidecarKubernetesPodOperator):
    """Resolve only during execution; inject after ordinary pod composition.

    Values never enter templated operator fields or pack metadata. Kubernetes
    Pod readers can still inspect environment values: this is an explicit
    platform transport choice, not a confidentiality equivalent to a volume.
    """

    def __init__(
        self,
        *,
        airflow_connection_projection: Mapping[str, Any],
        airflow_connection_reader: ConnectionUriReader | None = None,
        connection_secret_masker: Callable[[str], None] | None = None,
        **kwargs: Any,
    ) -> None:
        self.airflow_connection_projection = require_connection_env_projection(airflow_connection_projection)
        self._connection_reader = airflow_connection_reader
        self._connection_secret_masker = connection_secret_masker
        self._runtime_connection_env: dict[str, str] = {}
        self._active_connection_masker: Callable[[str], None] | None = None
        kwargs["log_pod_spec_on_failure"] = False
        super().__init__(**kwargs)

    def execute(self, context: Any) -> Any:
        with self._connection_security_lifecycle():
            self._resolve_runtime_connections()
            return super().execute(context)

    def trigger_reentry(self, context: Any, event: Any, **kwargs: Any) -> Any:
        """Mask the original Pod credentials before resumed logs, even after rotation."""
        with self._connection_security_lifecycle():
            return super().trigger_reentry(context, event, **kwargs)

    def get_or_create_pod(self, pod_request_obj: Any, context: Any) -> Any:
        """Mask the selected existing Pod when reattachment bypasses hook reads."""
        pod = super().get_or_create_pod(pod_request_obj, context)
        self._mask_pod_connections(pod)
        return pod

    @cached_property
    def hook(self) -> Any:
        """Adapt the native hook's Pod read without replacing provider lifecycle."""
        return _ConnectionMaskingPodHook(super().hook, self._mask_pod_connections)

    @contextmanager
    def _connection_security_lifecycle(self) -> Iterator[None]:
        self.log_pod_spec_on_failure = False
        try:
            self._active_connection_masker = self._connection_secret_masker or _airflow_mask_secret()
            for field in ("pod_request_obj", "pod"):
                self._mask_pod_connections(getattr(self, field, None))
            yield
        finally:
            self._runtime_connection_env.clear()
            self._active_connection_masker = None
            # Clear only local objects; never update the remote Pod or XCom.
            for field in ("pod_request_obj", "pod"):
                _clear_connection_env(getattr(self, field, None), self.airflow_connection_projection)

    def _mask_pod_connections(self, pod: Any) -> None:
        masker = self._active_connection_masker
        if masker is None:
            return
        names = {
            airflow_conn_env_name(item["connection_id"]) for item in self.airflow_connection_projection["connections"]
        }
        for container in _pod_containers(pod):
            if _container_name(container) != "base":
                continue
            env = container.get("env", []) if isinstance(container, Mapping) else getattr(container, "env", [])
            for item in env or []:
                if _container_name(item) in names:
                    value = item.get("value") if isinstance(item, Mapping) else getattr(item, "value", None)
                    if isinstance(value, str) and value:
                        _mask_uri(value, masker)

    def _resolve_runtime_connections(self) -> None:
        projection = self.airflow_connection_projection
        reader = ProjectedAirflowConnectionUriReader(
            reader=self._connection_reader or AirflowBaseHookConnectionReader(),
            scheme_overrides=projection.get("scheme_overrides"),
            database_overrides=projection.get("database_overrides"),
            query_overrides=projection.get("query_overrides"),
        )
        masker = self._active_connection_masker
        if masker is None:
            raise RuntimeError("Connection environment masking lifecycle is unavailable")
        for entry in projection["connections"]:
            connection_id = entry["connection_id"]
            env_name = airflow_conn_env_name(connection_id)
            if env_name in self._runtime_connection_env:
                continue
            try:
                uri = reader.read_uri(connection_id)
                _mask_uri(uri, masker)
            except Exception:
                raise RuntimeError("Airflow Connection environment resolution failed") from None
            self._runtime_connection_env[env_name] = uri

    def build_pod_request_obj(self, context: Any | None = None) -> Any:
        pod = super().build_pod_request_obj(context=context)
        if not self._runtime_connection_env:
            return pod
        pod = deepcopy(pod)
        containers = pod.get("spec", {}).get("containers", []) if isinstance(pod, Mapping) else pod.spec.containers
        base = next((item for item in containers if _container_name(item) == "base"), None)
        if base is None:
            raise RuntimeError("Connection environment projection requires the base runtime container")
        # Reuse the env merger on an isolated one-container envelope. Init,
        # XCom and platform sidecars never see execution-resolved credentials.
        if isinstance(base, Mapping):
            envelope = {"spec": {"containers": [base]}}
            updated = patch_pod_spec_env_vars(envelope, self._runtime_connection_env)["spec"]["containers"][0]
            containers[containers.index(base)] = updated
        else:
            from types import SimpleNamespace

            patch_pod_spec_env_vars(
                SimpleNamespace(spec=SimpleNamespace(containers=[base])), self._runtime_connection_env
            )
        return pod


def _container_name(container: Any) -> str | None:
    return container.get("name") if isinstance(container, Mapping) else getattr(container, "name", None)


class _ConnectionMaskingPodHook:
    """Preserve native hook capabilities and mask Pod reads before consumption."""

    def __init__(self, hook: Any, mask_pod: Callable[[Any], None]) -> None:
        self._hook = hook
        self._mask_pod = mask_pod

    def __getattr__(self, name: str) -> Any:
        return getattr(self._hook, name)

    def get_pod(self, *args: Any, **kwargs: Any) -> Any:
        pod = self._hook.get_pod(*args, **kwargs)
        self._mask_pod(pod)
        return pod


def _pod_containers(pod: Any) -> list[Any]:
    if pod is None:
        return []
    return (
        pod.get("spec", {}).get("containers", [])
        if isinstance(pod, Mapping)
        else getattr(getattr(pod, "spec", None), "containers", [])
    ) or []


def _clear_connection_env(pod: Any, projection: Mapping[str, Any]) -> None:
    if pod is None:
        return
    names = {airflow_conn_env_name(item["connection_id"]) for item in projection["connections"]}
    for container in _pod_containers(pod):
        if _container_name(container) != "base":
            continue
        env = container.get("env", []) if isinstance(container, Mapping) else getattr(container, "env", [])
        clean = [item for item in env or [] if _container_name(item) not in names]
        if isinstance(container, dict):
            container["env"] = clean
        else:
            container.env = clean


def _airflow_mask_secret() -> Callable[[str], None]:
    try:
        from airflow.sdk.log import mask_secret
    except ImportError:
        try:
            from airflow.sdk.execution_time.secrets_masker import mask_secret
        except ImportError:
            from airflow.utils.log.secrets_masker import mask_secret
    return mask_secret


def _mask_uri(uri: str, masker: Callable[[str], None]) -> None:
    masker(uri)
    parts = urlsplit(uri)
    for value in (parts.username, parts.password, *(value for _, value in parse_qsl(parts.query))):
        if value:
            masker(value)
            masker(unquote(value))
