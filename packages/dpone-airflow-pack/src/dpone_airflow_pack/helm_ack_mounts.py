"""Harden loader-ACK mounts in official Airflow Helm render output."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

ACK_VOLUME = "dpone-airflow-loader-ack"
ACK_MOUNT_PATH = "/opt/airflow/.dpone-ack"
PARSER_CONTAINERS = frozenset({"scheduler", "dag-processor"})
ACK_READERS = frozenset({"dpone-cache-status-projector", "dpone-cache-watch"})
_CONTAINER_COLLECTIONS = ("initContainers", "containers", "ephemeralContainers")


class LoaderAckMountPolicyError(ValueError):
    """Rendered workload does not preserve the loader-ACK authority boundary."""


def harden_loader_ack_mounts(documents: list[object]) -> list[object]:
    """Return copied manifests with one parser writer and read-only dpone readers."""

    rendered = deepcopy(documents)
    target_count = 0
    for document in rendered:
        for pod_spec in _target_pod_specs(document):
            if not _has_ack_volume(pod_spec):
                continue
            target_count += 1
            parser_name = _exact_parser_name(pod_spec)
            for collection in _CONTAINER_COLLECTIONS:
                for container in _containers(pod_spec, collection):
                    name = _required_name(container)
                    mount: dict[str, object] | None = None
                    if collection == "containers" and name == parser_name:
                        mount = _ack_mount(read_only=False)
                    elif collection == "containers" and name in ACK_READERS:
                        mount = _ack_mount(read_only=True)
                    _replace_ack_mount(container, mount=mount)
    _require_one_ack_workload(target_count)
    verify_loader_ack_mounts(rendered)
    return rendered


def verify_loader_ack_mounts(documents: list[object]) -> None:
    """Fail unless every ACK-bearing workload has one exact parser writer."""

    target_count = 0
    for document in documents:
        for pod_spec in _target_pod_specs(document):
            if not _has_ack_volume(pod_spec):
                continue
            target_count += 1
            parser_name = _exact_parser_name(pod_spec)
            for collection in _CONTAINER_COLLECTIONS:
                for container in _containers(pod_spec, collection):
                    name = _required_name(container)
                    expected: bool | None = None
                    if collection == "containers" and name == parser_name:
                        expected = False
                    elif collection == "containers" and name in ACK_READERS:
                        expected = True
                    _require_ack_access(container, expected=expected)
    _require_one_ack_workload(target_count)


def loader_ack_mount_summary(documents: list[object]) -> tuple[dict[str, object], ...]:
    """Return bounded structural evidence without copying rendered workload data."""

    verify_loader_ack_mounts(documents)
    summaries: list[dict[str, object]] = []
    for document in _resource_documents(documents):
        for pod_spec in _target_pod_specs(document):
            if not _has_ack_volume(pod_spec):
                continue
            parser_name = _exact_parser_name(pod_spec)
            readers = sorted(
                name
                for container in _containers(pod_spec, "containers")
                if (name := _required_name(container)) in ACK_READERS
                and any(mount.get("name") == ACK_VOLUME for mount in container.get("volumeMounts", []))
            )
            metadata = document.get("metadata")
            resource_name = metadata.get("name") if isinstance(metadata, dict) else None
            summaries.append(
                {
                    "kind": str(document.get("kind")),
                    "name": str(resource_name) if isinstance(resource_name, str) else "<unnamed>",
                    "parser_container": parser_name,
                    "writer_mount_path": ACK_MOUNT_PATH,
                    "writer_read_only": False,
                    "reader_containers": readers,
                    "reader_read_only": True,
                }
            )
    return tuple(summaries)


def _resource_documents(documents: list[object]) -> tuple[dict[str, Any], ...]:
    resources: list[dict[str, Any]] = []
    for document in documents:
        if not isinstance(document, dict):
            continue
        if document.get("kind") == "List":
            items = document.get("items")
            if not isinstance(items, list):
                raise LoaderAckMountPolicyError("Kubernetes List items must be a list")
            resources.extend(_resource_documents(items))
        else:
            resources.append(document)
    return tuple(resources)


def _require_one_ack_workload(target_count: int) -> None:
    if target_count != 1:
        raise LoaderAckMountPolicyError(
            f"render must contain exactly one ACK-bearing parser workload; observed={target_count}"
        )


def _target_pod_specs(document: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(document, dict):
        return ()
    kind = document.get("kind")
    if kind == "List":
        items = document.get("items")
        if not isinstance(items, list):
            raise LoaderAckMountPolicyError("Kubernetes List items must be a list")
        return tuple(pod_spec for item in items for pod_spec in _target_pod_specs(item))
    if kind == "Pod":
        return (_required_pod_spec(document.get("spec"), "Pod"),)
    if kind == "CronJob":
        spec = _required_mapping(document.get("spec"), "CronJob spec")
        job_template = _required_mapping(spec.get("jobTemplate"), "CronJob job template")
        job_spec = _required_mapping(job_template.get("spec"), "CronJob job spec")
        return (_template_pod_spec(job_spec, "CronJob"),)
    if kind in {
        "DaemonSet",
        "Deployment",
        "Job",
        "PodTemplate",
        "ReplicaSet",
        "ReplicationController",
        "StatefulSet",
    }:
        spec = _required_mapping(document.get("spec"), f"{kind} spec")
        return (_template_pod_spec(spec, str(kind)),)
    if _contains_ack_storage_reference(document):
        raise LoaderAckMountPolicyError(f"unsupported ACK-bearing Kubernetes resource kind: {kind!r}")
    return ()


def _template_pod_spec(spec: dict[str, Any], label: str) -> dict[str, Any]:
    template = _required_mapping(spec.get("template"), f"{label} pod template")
    return _required_pod_spec(template.get("spec"), label)


def _required_pod_spec(value: object, label: str) -> dict[str, Any]:
    return _required_mapping(value, f"{label} pod spec")


def _required_mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise LoaderAckMountPolicyError(f"{label} must be an object")
    return value


def _contains_ack_storage_reference(value: object) -> bool:
    if isinstance(value, dict):
        for field in ("volumes", "volumeMounts"):
            entries = value.get(field)
            if isinstance(entries, list) and any(
                isinstance(item, dict) and item.get("name") == ACK_VOLUME for item in entries
            ):
                return True
        return any(_contains_ack_storage_reference(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_ack_storage_reference(item) for item in value)
    return False


def _has_ack_volume(pod_spec: dict[str, Any]) -> bool:
    volumes = pod_spec.get("volumes", [])
    if not isinstance(volumes, list):
        raise LoaderAckMountPolicyError("pod volumes must be a list")
    return any(isinstance(volume, dict) and volume.get("name") == ACK_VOLUME for volume in volumes)


def _exact_parser_name(pod_spec: dict[str, Any]) -> str:
    parser_names = [
        name
        for container in _containers(pod_spec, "containers")
        if (name := _required_name(container)) in PARSER_CONTAINERS
    ]
    if len(parser_names) != 1:
        raise LoaderAckMountPolicyError(
            f"ACK-bearing workload must contain exactly one parser container; observed={sorted(parser_names)!r}"
        )
    return parser_names[0]


def _containers(pod_spec: dict[str, Any], field: str) -> list[dict[str, Any]]:
    value = pod_spec.get(field, [])
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise LoaderAckMountPolicyError(f"pod {field} must be a list of objects")
    return value


def _required_name(container: dict[str, Any]) -> str:
    name = container.get("name")
    if not isinstance(name, str) or not name:
        raise LoaderAckMountPolicyError("container name must be a non-empty string")
    return name


def _replace_ack_mount(container: dict[str, Any], *, mount: dict[str, object] | None) -> None:
    mounts = container.get("volumeMounts", [])
    if not isinstance(mounts, list) or not all(isinstance(item, dict) for item in mounts):
        raise LoaderAckMountPolicyError(f"container {_required_name(container)!r} volumeMounts must be a list")
    retained = [item for item in mounts if item.get("name") != ACK_VOLUME]
    if mount is not None:
        retained.append(mount)
    if retained:
        container["volumeMounts"] = retained
    else:
        container.pop("volumeMounts", None)


def _require_ack_access(container: dict[str, Any], *, expected: bool | None) -> None:
    name = _required_name(container)
    mounts = container.get("volumeMounts", [])
    if not isinstance(mounts, list) or not all(isinstance(item, dict) for item in mounts):
        raise LoaderAckMountPolicyError(f"container {name!r} volumeMounts must be a list")
    ack_mounts = [item for item in mounts if item.get("name") == ACK_VOLUME]
    if expected is None:
        if ack_mounts:
            raise LoaderAckMountPolicyError(f"container {name!r} must not mount loader ACK")
        return
    if len(ack_mounts) != 1:
        raise LoaderAckMountPolicyError(f"container {name!r} must have exactly one loader-ACK mount")
    mount = ack_mounts[0]
    if mount.get("mountPath") != ACK_MOUNT_PATH or mount.get("readOnly") is not expected:
        mode = "read-only" if expected else "read-write"
        raise LoaderAckMountPolicyError(f"container {name!r} loader ACK must be {mode} at {ACK_MOUNT_PATH}")


def _ack_mount(*, read_only: bool) -> dict[str, object]:
    return {
        "name": ACK_VOLUME,
        "mountPath": ACK_MOUNT_PATH,
        "readOnly": read_only,
    }


__all__ = [
    "LoaderAckMountPolicyError",
    "harden_loader_ack_mounts",
    "loader_ack_mount_summary",
    "verify_loader_ack_mounts",
]
