"""Provider-owned, read-only projection of external runtime-authority input."""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

from dpone_airflow_pack.init_fetch_contract import RuntimeAuthoritySource
from dpone_airflow_pack.operator_runtime import _replace_named_mapping

RUNTIME_AUTHORITY_VOLUME = "dpone-runtime-authority"
RUNTIME_AUTHORITY_DIRECTORY = "/run/secrets/dpone/runtime-authority"
RUNTIME_AUTHORITY_PATH = f"{RUNTIME_AUTHORITY_DIRECTORY}/authority"
RUNTIME_AUTHORITY_PATH_ENV = "DPONE_RUNTIME_AUTHORITY_PATH"


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
        return pod
    containers = [
        *list(getattr(spec, "init_containers", None) or []),
        *list(getattr(spec, "containers", None) or []),
    ]
    if not containers:
        return pod
    spec.volumes = _replace_named_object_items(
        list(getattr(spec, "volumes", None) or []),
        _volume(source),
        kind="volume",
    )
    for container in containers:
        mounts = list(getattr(container, "volume_mounts", None) or getattr(container, "volumeMounts", None) or [])
        patched = _replace_named_object_items(mounts, _mount(), kind="mount")
        if hasattr(container, "volume_mounts"):
            container.volume_mounts = patched
        else:
            setattr(container, "volumeMounts", patched)
        env = [
            item
            for item in list(getattr(container, "env", None) or [])
            if _item_name(item) != RUNTIME_AUTHORITY_PATH_ENV
        ]
        env.append(_object_env_var(RUNTIME_AUTHORITY_PATH_ENV, RUNTIME_AUTHORITY_PATH))
        container.env = env
    return pod


def _patch_mapping_pod(
    pod: dict[str, object],
    source: RuntimeAuthoritySource,
) -> dict[str, object]:
    spec = pod.get("spec")
    if not isinstance(spec, Mapping):
        return pod
    projected = dict(spec)
    init_containers = projected.get("initContainers")
    base_containers = projected.get("containers")
    if not (isinstance(init_containers, list) and init_containers) and not (
        isinstance(base_containers, list) and base_containers
    ):
        return pod
    projected["volumes"] = _replace_named_mapping(list(projected.get("volumes") or []), _volume(source))
    if isinstance(init_containers, list):
        projected["initContainers"] = [_patch_mapping_container(item) for item in init_containers]
    if isinstance(base_containers, list):
        projected["containers"] = [_patch_mapping_container(item) for item in base_containers]
    pod["spec"] = projected
    return pod


def _patch_mapping_container(container: object) -> object:
    if not isinstance(container, Mapping):
        return container
    result = dict(container)
    result["volumeMounts"] = _replace_named_mapping(list(result.get("volumeMounts") or []), _mount())
    env = [item for item in list(result.get("env") or []) if _item_name(item) != RUNTIME_AUTHORITY_PATH_ENV]
    env.append({"name": RUNTIME_AUTHORITY_PATH_ENV, "value": RUNTIME_AUTHORITY_PATH})
    result["env"] = env
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


def _replace_named_object_items(
    existing: list[object],
    item: Mapping[str, object],
    *,
    kind: str,
) -> list[object]:
    preserved = [value for value in existing if _item_name(value) != item["name"]]
    return [*preserved, _object_resource(item, kind=kind)]


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
