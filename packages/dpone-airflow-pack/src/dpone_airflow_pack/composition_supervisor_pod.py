"""Closed Kubernetes Pod policy for protected composition supervisors."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from dpone_airflow_pack.composition_supervisor_contract import (
    CompositionSupervisorProjection,
)
from dpone_airflow_pack.init_fetch_pod_contract import FORBIDDEN_POD_SPEC_FIELDS
from dpone_airflow_pack.init_fetch_pod_guard import reserved_collision
from dpone_airflow_pack.run_identity import (
    COMPOSITION_SUPERVISOR_B64_ENV,
    encode_composition_supervisor,
)

COMPOSITION_SUPERVISOR_VOLUME = "dpone-composition-supervisor"
COMPOSITION_PROFILES_VOLUME = "dpone-composition-profiles"
COMPOSITION_SUPERVISOR_ROOT = "/var/lib/dpone/composition"
COMPOSITION_PROFILES_ROOT = "/dev/shm/dpone-composition"


def composition_supervisor_security_context() -> dict[str, Any]:
    """Return the exact least-privilege root-supervisor container boundary."""

    return {
        "runAsUser": 0,
        "runAsGroup": 0,
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "seccompProfile": {"type": "RuntimeDefault"},
        "capabilities": {
            "drop": ["ALL"],
            "add": [
                "CHOWN",
                "FOWNER",
                "DAC_READ_SEARCH",
                "SETUID",
                "SETGID",
                "KILL",
            ],
        },
    }


def apply_composition_supervisor(
    pod: dict[str, Any],
    projection: CompositionSupervisorProjection,
) -> None:
    """Apply the closed v3 supervisor authority to one provider-owned Pod."""

    spec = pod.get("spec")
    if not isinstance(spec, dict):
        raise reserved_collision("supervisor pod spec must be an object")
    configured_pod_security = sorted(FORBIDDEN_POD_SPEC_FIELDS.intersection(spec))
    if configured_pod_security:
        raise reserved_collision(
            "supervisor pod cannot preconfigure pod security fields: " + ", ".join(configured_pod_security)
        )
    containers = spec.get("containers")
    init_containers = spec.get("initContainers")
    volumes = spec.get("volumes")
    if (
        not isinstance(containers, list)
        or len(containers) != 1
        or not isinstance(containers[0], dict)
        or not isinstance(init_containers, list)
        or any(not isinstance(container, dict) for container in init_containers)
    ):
        raise reserved_collision("supervisor pod topology is invalid")
    if not isinstance(volumes, list):
        raise reserved_collision("supervisor pod volumes must be a list")
    all_containers = [*containers, *init_containers]
    if any("securityContext" in container for container in all_containers):
        raise reserved_collision("supervisor pod cannot preconfigure container security")
    reserved_volumes = {
        COMPOSITION_SUPERVISOR_VOLUME,
        COMPOSITION_PROFILES_VOLUME,
    }
    if any(not isinstance(volume, Mapping) or volume.get("name") in reserved_volumes for volume in volumes):
        raise reserved_collision("supervisor pod contains a reserved volume collision")
    _reject_container_collisions(
        all_containers,
        reserved_volumes=reserved_volumes,
    )
    base = containers[0]
    base_env = base.get("env")
    base_mounts = base.get("volumeMounts")
    if not isinstance(base_env, list):
        raise reserved_collision("supervisor pod container env must be a list")
    if not isinstance(base_mounts, list):
        raise reserved_collision("supervisor pod container volumeMounts must be a list")
    supervisor_env = {
        "name": COMPOSITION_SUPERVISOR_B64_ENV,
        "value": encode_composition_supervisor(projection.to_dict()),
    }
    replacement_base = deepcopy(base)
    replacement_base["securityContext"] = composition_supervisor_security_context()
    replacement_base["env"] = [*deepcopy(base_env), supervisor_env]
    replacement_base["volumeMounts"] = [
        *deepcopy(base_mounts),
        *_supervisor_mounts(),
    ]
    replacement_spec = deepcopy(spec)
    replacement_spec["containers"] = [replacement_base]
    replacement_spec["volumes"] = [
        *deepcopy(volumes),
        *_supervisor_volumes(projection),
    ]
    pod["spec"] = replacement_spec


def _reject_container_collisions(
    containers: list[object],
    *,
    reserved_volumes: set[str],
) -> None:
    reserved_paths = {COMPOSITION_SUPERVISOR_ROOT, COMPOSITION_PROFILES_ROOT}
    for container in containers:
        if not isinstance(container, dict):
            raise reserved_collision("supervisor pod topology is invalid")
        mounts = container.get("volumeMounts")
        env = container.get("env")
        if not isinstance(mounts, list):
            raise reserved_collision("supervisor pod container volumeMounts must be a list")
        if any(
            not isinstance(mount, Mapping)
            or mount.get("name") in reserved_volumes
            or mount.get("mountPath") in reserved_paths
            for mount in mounts
        ):
            raise reserved_collision("supervisor pod contains a reserved mount collision")
        if not isinstance(env, list):
            raise reserved_collision("supervisor pod container env must be a list")
        if any(not isinstance(item, Mapping) or item.get("name") == COMPOSITION_SUPERVISOR_B64_ENV for item in env):
            raise reserved_collision("supervisor pod contains a reserved environment collision")


def _supervisor_mounts() -> list[dict[str, object]]:
    return [
        {
            "name": COMPOSITION_SUPERVISOR_VOLUME,
            "mountPath": COMPOSITION_SUPERVISOR_ROOT,
            "readOnly": False,
        },
        {
            "name": COMPOSITION_PROFILES_VOLUME,
            "mountPath": COMPOSITION_PROFILES_ROOT,
            "readOnly": False,
        },
    ]


def _supervisor_volumes(
    projection: CompositionSupervisorProjection,
) -> list[dict[str, object]]:
    return [
        {
            "name": COMPOSITION_SUPERVISOR_VOLUME,
            "persistentVolumeClaim": {
                "claimName": projection.persistent_volume_claim,
            },
        },
        {
            "name": COMPOSITION_PROFILES_VOLUME,
            "emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"},
        },
    ]


__all__ = [
    "apply_composition_supervisor",
    "composition_supervisor_security_context",
]
