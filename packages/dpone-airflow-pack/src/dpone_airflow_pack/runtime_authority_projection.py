"""Provider-owned, read-only projection of external runtime-authority input."""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

from dpone_airflow_pack.init_fetch_contract import RuntimeAuthoritySource
from dpone_airflow_pack.init_fetch_pod_contract import INIT_CONTAINER_NAME
from dpone_airflow_pack.init_fetch_pod_guard import reserved_collision

RUNTIME_AUTHORITY_VOLUME = "dpone-runtime-authority"
RUNTIME_AUTHORITY_DIRECTORY = "/run/secrets/dpone/runtime-authority"
RUNTIME_AUTHORITY_PATH = f"{RUNTIME_AUTHORITY_DIRECTORY}/authority"
RUNTIME_AUTHORITY_PATH_ENV = "DPONE_RUNTIME_AUTHORITY_PATH"
_BASE_CONTAINER_NAME = "base"


def patch_pod_spec_runtime_authority(
    pod: object,
    source: RuntimeAuthoritySource | None,
) -> object:
    """Mount one referenced Secret key read-only in every runtime container."""

    if pod is None or source is None:
        return pod
    if isinstance(pod, Mapping):
        return _patch_mapping_pod(dict(pod), source)
    spec = getattr(pod, "spec", None)
    if spec is None:
        raise reserved_collision("runtime-authority projection requires a pod spec")
    init = _require_named_object_container(
        list(getattr(spec, "init_containers", None) or []),
        INIT_CONTAINER_NAME,
    )
    base = _require_named_object_container(
        list(getattr(spec, "containers", None) or []),
        _BASE_CONTAINER_NAME,
    )
    volumes = list(getattr(spec, "volumes", None) or [])
    _reject_named_collision(volumes, RUNTIME_AUTHORITY_VOLUME, "volume")
    container_fields: list[tuple[object, list[object], bool, list[object]]] = []
    for container in (init, base):
        mounts = list(getattr(container, "volume_mounts", None) or getattr(container, "volumeMounts", None) or [])
        _reject_named_collision(mounts, RUNTIME_AUTHORITY_VOLUME, "volume mount")
        env = list(getattr(container, "env", None) or [])
        _reject_named_collision(env, RUNTIME_AUTHORITY_PATH_ENV, "environment variable")
        container_fields.append((container, mounts, hasattr(container, "volume_mounts"), env))
    setattr(spec, "volumes", [*volumes, _object_resource(_volume(source), kind="volume")])
    for container, mounts, uses_snake_case, env in container_fields:
        patched_mounts = [*mounts, _object_resource(_mount(), kind="mount")]
        setattr(container, "volume_mounts" if uses_snake_case else "volumeMounts", patched_mounts)
        setattr(
            container,
            "env",
            [*env, _object_env_var(RUNTIME_AUTHORITY_PATH_ENV, RUNTIME_AUTHORITY_PATH)],
        )
    return pod


def _patch_mapping_pod(
    pod: dict[str, object],
    source: RuntimeAuthoritySource,
) -> dict[str, object]:
    spec = pod.get("spec")
    if not isinstance(spec, Mapping):
        raise reserved_collision("runtime-authority projection requires a pod spec")
    projected = dict(spec)
    init_containers = projected.get("initContainers")
    base_containers = projected.get("containers")
    init_index = _require_named_mapping_container(init_containers, INIT_CONTAINER_NAME)
    base_index = _require_named_mapping_container(base_containers, _BASE_CONTAINER_NAME)
    volumes = _mapping_list(projected.get("volumes"), "volumes")
    _reject_named_collision(volumes, RUNTIME_AUTHORITY_VOLUME, "volume")
    projected["volumes"] = [*volumes, _volume(source)]
    projected["initContainers"] = _patch_mapping_container_at(init_containers, init_index)
    projected["containers"] = _patch_mapping_container_at(base_containers, base_index)
    pod["spec"] = projected
    return pod


def _patch_mapping_container_at(containers: object, index: int) -> list[object]:
    assert isinstance(containers, list)
    result = list(containers)
    container = containers[index]
    assert isinstance(container, Mapping)
    result[index] = _patch_mapping_container(container)
    return result


def _patch_mapping_container(container: Mapping[str, object]) -> dict[str, object]:
    result = dict(container)
    mounts = _mapping_list(result.get("volumeMounts"), "volumeMounts")
    _reject_named_collision(mounts, RUNTIME_AUTHORITY_VOLUME, "volume mount")
    env = _mapping_list(result.get("env"), "env")
    _reject_named_collision(env, RUNTIME_AUTHORITY_PATH_ENV, "environment variable")
    result["volumeMounts"] = [*mounts, _mount()]
    result["env"] = [*env, {"name": RUNTIME_AUTHORITY_PATH_ENV, "value": RUNTIME_AUTHORITY_PATH}]
    return result


def _volume(source: RuntimeAuthoritySource) -> dict[str, object]:
    return {
        "name": RUNTIME_AUTHORITY_VOLUME,
        "secret": {
            "secretName": source.secret_name,
            "items": [{"key": source.secret_key, "path": "authority"}],
            "optional": False,
        },
    }


def _mount() -> dict[str, object]:
    return {
        "name": RUNTIME_AUTHORITY_VOLUME,
        "mountPath": RUNTIME_AUTHORITY_DIRECTORY,
        "readOnly": True,
    }


def _item_name(value: Any) -> object:
    if isinstance(value, Mapping):
        return value.get("name")
    return getattr(value, "name", None)


def _mapping_list(value: object, field: str) -> list[object]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise reserved_collision(f"runtime-authority pod {field} must be a list")
    return list(value)


def _require_named_mapping_container(containers: object, name: str) -> int:
    values = _mapping_list(containers, "containers")
    matches = [index for index, item in enumerate(values) if _item_name(item) == name]
    if len(matches) != 1:
        raise reserved_collision(f"runtime-authority projection requires exactly one {name} container")
    if not isinstance(values[matches[0]], Mapping):
        raise reserved_collision(f"runtime-authority {name} container must be an object")
    return matches[0]


def _require_named_object_container(containers: list[object], name: str) -> object:
    matches = [item for item in containers if _item_name(item) == name]
    if len(matches) != 1:
        raise reserved_collision(f"runtime-authority projection requires exactly one {name} container")
    return matches[0]


def _reject_named_collision(values: list[object], name: str, kind: str) -> None:
    if any(_item_name(item) == name for item in values):
        raise reserved_collision(f"runtime-authority {kind} {name} is provider-owned")


def _object_resource(item: Mapping[str, object], *, kind: str) -> object:
    try:
        from kubernetes.client import ApiClient
        from kubernetes.client import models as k8s

        klass = k8s.V1Volume if kind == "volume" else k8s.V1VolumeMount
        return ApiClient()._ApiClient__deserialize_model(dict(item), klass)
    except Exception:  # pragma: no cover - dependency-light provider fallback
        return _namespace(item)


def _object_env_var(name: str, value: str) -> object:
    try:
        from kubernetes.client import models as k8s

        return k8s.V1EnvVar(name=name, value=value)
    except Exception:  # pragma: no cover - dependency-light provider fallback
        return SimpleNamespace(name=name, value=value)


def _namespace(value: object) -> object:
    if isinstance(value, Mapping):
        return SimpleNamespace(**{str(key): _namespace(item) for key, item in value.items()})
    if isinstance(value, list):
        return [_namespace(item) for item in value]
    return value


__all__ = [
    "RUNTIME_AUTHORITY_DIRECTORY",
    "RUNTIME_AUTHORITY_PATH",
    "RUNTIME_AUTHORITY_PATH_ENV",
    "RUNTIME_AUTHORITY_VOLUME",
    "patch_pod_spec_runtime_authority",
]
