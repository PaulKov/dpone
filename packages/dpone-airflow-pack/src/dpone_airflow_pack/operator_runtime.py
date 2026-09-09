"""Runtime-focused helpers for Airflow pod operators."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

import dpone_airflow_pack.connection_secret_lifecycle_runtime as lifecycle_runtime
from dpone_airflow_pack.connection_names import (
    require_airflow_conn_env_name,
    require_airflow_connection_id,
    require_kubernetes_dns_label,
)
from dpone_airflow_pack.connection_secret_identity import ATTEMPT_VOLUME_NAME_KEY
from dpone_airflow_pack.connection_secret_lifecycle import AirflowConnectionSecretLifecycle

UNSAFE_AIRFLOW_CONNECTION_ENV_ANNOTATION = "dpone.dev/unsafe-airflow-connection-env"
UNSAFE_AIRFLOW_CONNECTION_ENV_VALUE = "non-prod-only"


class AirflowConnectionUriReader(Protocol):
    """Small port for retrieving Airflow connection URIs."""

    def read_uri(self, connection_id: str) -> str: ...


def stringify_env_vars(existing: object) -> object:
    """Coerce env var values to strings for Kubernetes Pod env contract."""

    if not existing:
        return existing
    if isinstance(existing, Mapping):
        return {str(key): "" if value is None else str(value) for key, value in existing.items()}
    if isinstance(existing, list):
        normalized = []
        for env_var in existing:
            name = getattr(env_var, "name", None)
            value = getattr(env_var, "value", None)
            if name is None and isinstance(env_var, Mapping):
                name = env_var.get("name")
                value = env_var.get("value")
            if name is None:
                normalized.append(env_var)
                continue
            normalized.append(_new_env_var(str(name), "" if value is None else str(value)))
        return normalized
    return existing


def build_airflow_connection_projected_secret(
    projection: Mapping[str, object],
    *,
    reader: AirflowConnectionUriReader,
    lifecycle: AirflowConnectionSecretLifecycle | None = None,
) -> dict[str, object]:
    """Build an in-memory Kubernetes Secret payload for projected Airflow URIs."""

    secret_name = _projection_secret_name(projection)
    metadata: dict[str, object] = {"name": secret_name}
    if lifecycle is not None:
        metadata.update(lifecycle.secret_metadata())
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": metadata,
        "type": "Opaque",
        "immutable": True,
        "stringData": {
            _projection_secret_key(item): reader.read_uri(_projection_connection_id(item))
            for item in _projection_connections(projection)
        },
    }


def materialize_airflow_connection_secret_volume(
    operator: Any,
    projection: Mapping[str, object],
    *,
    lifecycle: AirflowConnectionSecretLifecycle | None = None,
) -> None:
    """Patch compact-pack pod specs with a Secret volume reference only."""

    kwargs = getattr(operator, "kwargs", None)
    if lifecycle is not None:
        lifecycle_runtime.materialize_operator_lifecycle_metadata(operator, lifecycle, kwargs=kwargs)
    full_pod_spec = getattr(operator, "full_pod_spec", None)
    if full_pod_spec is not None:
        operator.full_pod_spec = patch_pod_spec_secret_volume(full_pod_spec, projection, lifecycle=lifecycle)
    pod_template_dict = getattr(operator, "pod_template_dict", None)
    if isinstance(pod_template_dict, Mapping):
        operator.pod_template_dict = patch_pod_spec_secret_volume(
            dict(pod_template_dict), projection, lifecycle=lifecycle
        )
    if isinstance(kwargs, dict) and kwargs.get("full_pod_spec") is not None:
        kwargs["full_pod_spec"] = patch_pod_spec_secret_volume(kwargs["full_pod_spec"], projection, lifecycle=lifecycle)


def patch_pod_spec_secret_volume(
    pod: object,
    projection: Mapping[str, object],
    *,
    lifecycle: AirflowConnectionSecretLifecycle | None = None,
) -> object:
    """Attach a projected Airflow Connection Secret to the base container."""

    if pod is None:
        return pod
    if isinstance(pod, Mapping):
        return _patch_pod_spec_mapping_secret_volume(dict(pod), projection, lifecycle=lifecycle)
    spec = getattr(pod, "spec", None)
    containers = getattr(spec, "containers", None) if spec is not None else None
    if spec is None or not containers:
        return pod
    _patch_object_pod_secret_volume(spec, containers[0], projection)
    if lifecycle is not None:
        lifecycle_runtime.patch_object_pod_lifecycle_metadata(pod, lifecycle)
    return pod


def merge_env_vars(existing: object, injected: Mapping[str, str]) -> dict[str, str]:
    merged = _env_vars_to_mapping(existing)
    merged.update(injected)
    return merged


def materialize_runtime_connection_env(operator: Any) -> None:
    """Persist injected ``AIRFLOW_CONN_*`` values on compact-pack pod specs."""

    env_vars = getattr(operator, "env_vars", None)
    if not env_vars:
        return
    full_pod_spec = getattr(operator, "full_pod_spec", None)
    if full_pod_spec is not None:
        operator.full_pod_spec = patch_pod_spec_env_vars(full_pod_spec, env_vars)
    pod_template_dict = getattr(operator, "pod_template_dict", None)
    if isinstance(pod_template_dict, Mapping):
        operator.pod_template_dict = patch_pod_spec_env_vars(dict(pod_template_dict), env_vars)
    kwargs = getattr(operator, "kwargs", None)
    if isinstance(kwargs, dict):
        if kwargs.get("full_pod_spec") is not None:
            kwargs["full_pod_spec"] = patch_pod_spec_env_vars(kwargs["full_pod_spec"], env_vars)
        kwargs["env_vars"] = env_vars


def patch_pod_spec_env_vars(pod: Any, env_vars: object) -> Any:
    """Merge rendered operator env vars into base and init containers.

    Strict ``init_fetch`` pods carry the same allowlisted templates on the
    ``dpone-runtime-init-fetch`` init container (for example AWS_* for
    non-production object-storage registries). Airflow templates
    ``KubernetesPodOperator.env_vars`` at execute time; without patching init
    containers those templates stay literal and S3 fetch fails closed.
    """

    if pod is None or not env_vars:
        return pod
    normalized = _normalized_env_var_map(env_vars)
    if not normalized:
        return pod
    if isinstance(pod, Mapping):
        return _patch_pod_spec_mapping_env_vars(dict(pod), normalized)
    spec = getattr(pod, "spec", None)
    if spec is None:
        return pod
    for container in list(getattr(spec, "containers", None) or []):
        _patch_object_container_env(container, normalized)
    for container in list(getattr(spec, "init_containers", None) or []):
        _patch_object_container_env(container, normalized)
    return pod


def _patch_object_container_env(container: Any, env_vars: Mapping[str, str]) -> None:
    existing = getattr(container, "env", None) or []
    merged = merge_env_vars(_env_vars_to_mapping(existing), env_vars)
    container.env = [_new_env_var(name, value) for name, value in merged.items()]


def _normalized_env_var_map(env_vars: object) -> dict[str, str]:
    mapping = _env_vars_to_mapping(env_vars)
    return {str(key): "" if value is None else str(value) for key, value in mapping.items()}


def _env_vars_to_mapping(env_vars: object) -> dict[str, str]:
    if not env_vars:
        return {}
    if isinstance(env_vars, Mapping):
        return {str(key): "" if value is None else str(value) for key, value in env_vars.items()}
    if isinstance(env_vars, list):
        mapping: dict[str, str] = {}
        for env_var in env_vars:
            name = getattr(env_var, "name", None)
            value = getattr(env_var, "value", None)
            if name is None and isinstance(env_var, Mapping):
                name = env_var.get("name")
                value = env_var.get("value")
            if name is None:
                continue
            mapping[str(name)] = "" if value is None else str(value)
        return mapping
    raise TypeError(f"Unsupported env_vars type for pod env patch: {type(env_vars)!r}")


def _new_env_var(name: str, value: str) -> object:
    try:
        from kubernetes.client import models as k8s

        return k8s.V1EnvVar(name=name, value=value)
    except Exception:  # pragma: no cover
        return {"name": name, "value": value}


def _patch_pod_spec_mapping_env_vars(
    pod: Mapping[str, object],
    env_vars: Mapping[str, str],
) -> dict[str, object]:
    payload = dict(pod)
    spec = payload.get("spec")
    if not isinstance(spec, Mapping):
        return payload
    spec_payload = dict(spec)
    containers = spec_payload.get("containers")
    if isinstance(containers, list) and containers:
        spec_payload["containers"] = [_patch_mapping_container_env(container, env_vars) for container in containers]
    init_containers = spec_payload.get("initContainers")
    if isinstance(init_containers, list) and init_containers:
        spec_payload["initContainers"] = [
            _patch_mapping_container_env(container, env_vars) for container in init_containers
        ]
    payload["spec"] = spec_payload
    return payload


def _patch_mapping_container_env(
    container: object,
    env_vars: Mapping[str, str],
) -> object:
    if not isinstance(container, Mapping):
        return container
    patched = dict(container)
    merged = merge_env_vars(_env_vars_to_mapping(patched.get("env")), env_vars)
    patched["env"] = [{"name": name, "value": value} for name, value in merged.items()]
    return patched


def _patch_pod_spec_mapping_secret_volume(
    pod: Mapping[str, object],
    projection: Mapping[str, object],
    *,
    lifecycle: AirflowConnectionSecretLifecycle | None,
) -> dict[str, object]:
    payload = dict(pod)
    spec = payload.get("spec")
    if not isinstance(spec, Mapping):
        return payload
    spec_payload = dict(spec)
    containers = spec_payload.get("containers")
    if not isinstance(containers, list) or not containers:
        return payload
    volume = _secret_volume(projection)
    spec_payload["volumes"] = _replace_named_mapping(list(spec_payload.get("volumes") or []), volume)
    base = dict(containers[0]) if isinstance(containers[0], Mapping) else {}
    base["volumeMounts"] = _replace_named_mapping(
        list(base.get("volumeMounts") or []),
        _secret_volume_mount(projection),
    )
    spec_payload["containers"] = [base, *containers[1:]]
    payload["spec"] = spec_payload
    if lifecycle is not None:
        payload["metadata"] = lifecycle_runtime.merged_lifecycle_metadata(payload.get("metadata"), lifecycle)
    return payload


def _patch_object_pod_secret_volume(spec: object, base: object, projection: Mapping[str, object]) -> None:
    volumes = list(getattr(spec, "volumes", None) or [])
    setattr(spec, "volumes", _replace_named_object(volumes, _secret_volume(projection)))
    mounts = list(getattr(base, "volume_mounts", None) or getattr(base, "volumeMounts", None) or [])
    patched_mounts = _replace_named_object(mounts, _secret_volume_mount(projection))
    if hasattr(base, "volume_mounts"):
        base.volume_mounts = patched_mounts
    else:
        setattr(base, "volumeMounts", patched_mounts)


def _secret_volume(projection: Mapping[str, object]) -> dict[str, object]:
    return {
        "name": _projection_volume_name(projection),
        "secret": {
            "secretName": _projection_secret_name(projection),
            "items": [
                {"key": _projection_secret_key(item), "path": _projection_item_path(projection, item)}
                for item in _projection_connections(projection)
            ],
        },
    }


def _secret_volume_mount(projection: Mapping[str, object]) -> dict[str, object]:
    return {
        "name": _projection_volume_name(projection),
        "mountPath": _projection_mount_path(projection),
        "readOnly": True,
    }


def _replace_named_mapping(existing: list[object], item: Mapping[str, object]) -> list[object]:
    name = item.get("name")
    return [entry for entry in existing if _named_item_name(entry) != name] + [dict(item)]


def _replace_named_object(existing: list[object], item: Mapping[str, object]) -> list[object]:
    name = item.get("name")
    preserved = [entry for entry in existing if _named_item_name(entry) != name]
    try:
        from kubernetes.client import ApiClient
        from kubernetes.client import models as k8s
    except Exception:  # pragma: no cover
        return [*preserved, dict(item)]
    klass = k8s.V1Volume if "secret" in item else k8s.V1VolumeMount
    return [*preserved, ApiClient()._ApiClient__deserialize_model(dict(item), klass)]


def _named_item_name(item: object) -> object:
    if isinstance(item, Mapping):
        return item.get("name")
    return getattr(item, "name", None)


def _projection_connections(projection: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    raw = projection.get("connections")
    if not isinstance(raw, list):
        return ()
    return tuple(item for item in raw if isinstance(item, Mapping))


def _projection_secret_name(projection: Mapping[str, object]) -> str:
    name = str(projection.get("secret_name") or "").strip()
    if not name:
        raise ValueError("airflow_connection projection requires secret_name")
    return require_kubernetes_dns_label(name, context="airflow_connection projection secret_name")


def _projection_volume_name(projection: Mapping[str, object]) -> str:
    name = projection.get(ATTEMPT_VOLUME_NAME_KEY)
    if name is None:
        return _projection_secret_name(projection)
    return require_kubernetes_dns_label(name, context="airflow_connection projection volume name")


def _secret_manifest_name(secret: Mapping[str, object]) -> str:
    metadata = secret.get("metadata")
    name = str(metadata.get("name") if isinstance(metadata, Mapping) else "").strip()
    if not name:
        raise ValueError("airflow_connection projected Secret requires metadata.name")
    return require_kubernetes_dns_label(name, context="airflow_connection projected Secret metadata.name")


def _projection_connection_id(item: Mapping[str, object]) -> str:
    connection_id = str(item.get("connection_id") or "").strip()
    if not connection_id:
        raise ValueError("airflow_connection projection entry requires connection_id")
    return require_airflow_connection_id(connection_id, context="airflow_connection projection entry connection_id")


def _projection_secret_key(item: Mapping[str, object]) -> str:
    secret_key = str(item.get("secret_key") or "").strip()
    return require_airflow_conn_env_name(secret_key, context="airflow_connection projection secret_key")


def _projection_mount_path(projection: Mapping[str, object]) -> str:
    mount_path = str(projection.get("mount_path") or "").strip()
    if not mount_path.startswith("/run/secrets/dpone/"):
        raise ValueError("airflow_connection projection mount_path must be under /run/secrets/dpone/")
    return mount_path.rstrip("/")


def _projection_item_path(projection: Mapping[str, object], item: Mapping[str, object]) -> str:
    from pathlib import PurePosixPath

    root = PurePosixPath(_projection_mount_path(projection))
    entry_mount = PurePosixPath(str(item.get("mount_path") or ""))
    fields = item.get("fields")
    file_name = str(fields.get("uri") if isinstance(fields, Mapping) else "").strip()
    if not file_name:
        raise ValueError("airflow_connection projection entry requires fields.uri")
    relative_file = PurePosixPath(file_name)
    if relative_file.is_absolute() or ".." in relative_file.parts:
        raise ValueError("airflow_connection projection fields.uri must be relative")
    if entry_mount.is_absolute():
        try:
            relative_mount = entry_mount.relative_to(root)
        except ValueError as exc:
            raise ValueError(
                "airflow_connection projection entry mount_path must stay under projection mount_path"
            ) from exc
    else:
        relative_mount = entry_mount
    candidate = relative_mount / relative_file
    if ".." in candidate.parts or str(candidate) in {"", "."}:
        raise ValueError("airflow_connection projection item path is invalid")
    return candidate.as_posix()


def _projection_cleanup_policy(projection: Mapping[str, object]) -> str:
    policy = str(projection.get("cleanup_policy") or "after_execute").strip()
    if policy not in {"after_execute", "retain"}:
        raise ValueError("airflow_connection projection cleanup_policy must be after_execute or retain")
    return policy


def _operator_namespace(operator: object) -> str:
    namespace = getattr(operator, "namespace", None)
    if not namespace:
        kwargs = getattr(operator, "kwargs", None)
        namespace = kwargs.get("namespace") if isinstance(kwargs, Mapping) else None
    normalized = str(namespace or "").strip()
    if not normalized:
        raise ValueError("airflow_connection Secret projection requires a Kubernetes namespace")
    return normalized


def _operator_deferrable(operator: object) -> bool:
    value = getattr(operator, "deferrable", None)
    if value is None:
        kwargs = getattr(operator, "kwargs", None)
        value = kwargs.get("deferrable") if isinstance(kwargs, Mapping) else None
    return bool(value)


__all__ = [
    "UNSAFE_AIRFLOW_CONNECTION_ENV_ANNOTATION",
    "UNSAFE_AIRFLOW_CONNECTION_ENV_VALUE",
    "build_airflow_connection_projected_secret",
    "materialize_airflow_connection_secret_volume",
    "materialize_runtime_connection_env",
    "merge_env_vars",
    "patch_pod_spec_env_vars",
    "patch_pod_spec_secret_volume",
    "stringify_env_vars",
    "_projection_cleanup_policy",
    "_operator_namespace",
    "_operator_deferrable",
    "_projection_connection_id",
    "_projection_item_path",
    "_projection_secret_key",
    "_projection_secret_name",
    "_secret_manifest_name",
]
