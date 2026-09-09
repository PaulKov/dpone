from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.init_fetch_pod_contract import INIT_CONTAINER_NAME
from dpone_airflow_pack.init_fetch_validation import runtime_image

XCOM_SIDECAR_CONTAINER_NAME = "airflow-xcom-sidecar"


@dataclass(frozen=True)
class XComSidecarRuntimeConfig:
    """Explicit sidecar image binding; mutable tag defaults are forbidden."""

    image: str
    strict_runtime_image: str | None = None


def require_strict_xcom_sidecar_image(pack: Mapping[str, Any]) -> str:
    """Require the one closed XCom image contract used by strict init-fetch."""

    raw = pack.get("xcom")
    if not isinstance(raw, Mapping) or not raw.get("sidecar_image"):
        raise InitFetchProviderError(
            "DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED",
            "strict init-fetch requires a workload pack rebuilt with digest-pinned xcom.sidecar_image",
        )
    unknown = sorted(str(key) for key in raw if str(key) != "sidecar_image")
    if unknown:
        raise _collision("strict init-fetch xcom contains unsupported fields: " + ", ".join(unknown))
    image = raw["sidecar_image"]
    digest = image.rpartition("@")[2] if isinstance(image, str) else ""
    try:
        return runtime_image(image, digest, None)
    except InitFetchProviderError:
        raise _collision("strict init-fetch xcom.sidecar_image must be an exact digest-pinned OCI reference") from None


def require_xcom_sidecar_image(config: XComSidecarRuntimeConfig | None) -> str:
    """Fail closed when XCom push would otherwise inherit a mutable default image."""

    if config is None or not str(config.image or "").strip():
        raise RuntimeError(
            "DPONE_AIRFLOW_XCOM_SIDECAR_IMAGE_REQUIRED: "
            "XCom push requires an explicit sidecar image; mutable tag defaults are forbidden"
        )
    return str(config.image)


def pin_xcom_sidecar_image(pod: Any, image: str) -> Any:
    if not str(image or "").strip():
        raise RuntimeError("DPONE_AIRFLOW_XCOM_SIDECAR_IMAGE_REQUIRED: XCom sidecar image must be provided explicitly")
    containers = _pod_containers(pod, init=False, required=False)
    for container in containers:
        if _container_field(container, "name") == XCOM_SIDECAR_CONTAINER_NAME:
            _set_container_image(container, image)
            break
    return pod


def validate_strict_xcom_pod(
    pod: Any,
    *,
    sidecar_image: str,
    runtime_image_ref: str,
) -> Any:
    """Verify the final KPO pod contains only the provider-approved topology."""

    containers = _pod_containers(pod, init=False, required=True)
    base = [container for container in containers if _container_field(container, "name") == "base"]
    sidecars = [
        container for container in containers if _container_field(container, "name") == XCOM_SIDECAR_CONTAINER_NAME
    ]
    if len(containers) != 2 or len(base) != 1 or len(sidecars) != 1:
        raise _collision(
            "strict init-fetch final pod must contain exactly one base container and one Airflow XCom sidecar"
        )
    if _container_field(base[0], "image") != runtime_image_ref:
        raise _collision("strict init-fetch final pod base container image does not match the provider runtime image")
    if _container_field(sidecars[0], "image") != sidecar_image:
        raise _collision("strict init-fetch final pod XCom sidecar image does not match the provider-pinned image")

    init_containers = _pod_containers(pod, init=True, required=True)
    if (
        len(init_containers) != 1
        or _container_field(init_containers[0], "name") != INIT_CONTAINER_NAME
        or _container_field(init_containers[0], "image") != runtime_image_ref
    ):
        raise _collision(
            "strict init-fetch final pod must contain exactly one provider-owned "
            "init-fetch container with the runtime image"
        )
    return pod


def _pod_containers(
    pod: Any,
    *,
    init: bool,
    required: bool,
) -> list[Any]:
    spec = pod.get("spec") if isinstance(pod, Mapping) else getattr(pod, "spec", None)
    mapping_key = "initContainers" if init else "containers"
    attribute = "init_containers" if init else "containers"
    raw = spec.get(mapping_key) if isinstance(spec, Mapping) else getattr(spec, attribute, None)
    if raw is None and not required:
        return []
    if not isinstance(raw, Sequence) or isinstance(raw, str | bytes):
        raise _collision(f"strict init-fetch final pod {mapping_key} must be an array")
    return list(raw)


def _container_field(container: Any, field: str) -> Any:
    return container.get(field) if isinstance(container, Mapping) else getattr(container, field, None)


def _set_container_image(container: Any, image: str) -> None:
    if isinstance(container, MutableMapping):
        container["image"] = image
        return
    try:
        setattr(container, "image", image)
    except Exception:
        raise _collision("Airflow XCom sidecar image cannot be pinned on the final pod model") from None


def _collision(message: str) -> InitFetchProviderError:
    return InitFetchProviderError(
        "DPONE_INIT_FETCH_RESERVED_COLLISION",
        message,
    )


__all__ = [
    "XCOM_SIDECAR_CONTAINER_NAME",
    "XComSidecarRuntimeConfig",
    "pin_xcom_sidecar_image",
    "require_strict_xcom_sidecar_image",
    "require_xcom_sidecar_image",
    "validate_strict_xcom_pod",
]
