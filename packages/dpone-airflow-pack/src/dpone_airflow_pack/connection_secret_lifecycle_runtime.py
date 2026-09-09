"""Pod metadata adapters for the pure Airflow Connection Secret lifecycle contract."""

from __future__ import annotations

from collections.abc import Mapping

from dpone_airflow_pack.connection_secret_lifecycle import AirflowConnectionSecretLifecycle


def merged_lifecycle_metadata(
    existing: object,
    lifecycle: AirflowConnectionSecretLifecycle,
) -> dict[str, object]:
    """Merge validated dpone-owned keys while preserving user metadata."""

    metadata = dict(existing) if isinstance(existing, Mapping) else {}
    injected = lifecycle.pod_metadata()
    for field in ("labels", "annotations"):
        current = metadata.get(field)
        merged = dict(current) if isinstance(current, Mapping) else {}
        values = injected.get(field)
        if isinstance(values, Mapping):
            merged.update({str(key): str(value) for key, value in values.items()})
        metadata[field] = merged
    return metadata


def patch_object_pod_lifecycle_metadata(
    pod: object,
    lifecycle: AirflowConnectionSecretLifecycle,
) -> None:
    """Patch Kubernetes model-like Pod metadata without importing it on base paths."""

    metadata = getattr(pod, "metadata", None)
    if isinstance(metadata, Mapping):
        setattr(pod, "metadata", merged_lifecycle_metadata(metadata, lifecycle))
        return
    if metadata is None:
        metadata = _new_object_metadata()
        setattr(pod, "metadata", metadata)
    injected = lifecycle.pod_metadata()
    for field in ("labels", "annotations"):
        current = getattr(metadata, field, None)
        merged = dict(current) if isinstance(current, Mapping) else {}
        values = injected.get(field)
        if isinstance(values, Mapping):
            merged.update({str(key): str(value) for key, value in values.items()})
        setattr(metadata, field, merged)


def materialize_operator_lifecycle_metadata(
    operator: object,
    lifecycle: AirflowConnectionSecretLifecycle,
    *,
    kwargs: object,
) -> None:
    """Pin lifecycle fields at the highest-precedence KPO metadata surface."""

    seed: dict[str, object] = {}
    for field in ("labels", "annotations"):
        current = getattr(operator, field, None)
        if not isinstance(current, Mapping) and isinstance(kwargs, Mapping):
            current = kwargs.get(field)
        seed[field] = current
    merged = merged_lifecycle_metadata(seed, lifecycle)
    for field in ("labels", "annotations"):
        values = merged[field]
        setattr(operator, field, values)
        if isinstance(kwargs, dict):
            kwargs[field] = dict(values) if isinstance(values, Mapping) else values


def _new_object_metadata() -> object:
    try:
        from kubernetes.client import models as k8s

        return k8s.V1ObjectMeta()
    except Exception:  # pragma: no cover - lightweight provider fallback
        return type("DponePodMetadata", (), {})()


__all__ = [
    "materialize_operator_lifecycle_metadata",
    "merged_lifecycle_metadata",
    "patch_object_pod_lifecycle_metadata",
]
