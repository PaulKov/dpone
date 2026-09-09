"""Typed + Mapping accessors for Kubernetes ConfigMap / ObjectMeta fields.

Official ``kubernetes`` client models (``V1ConfigMap``, ``V1ObjectMeta``) are
not ``Mapping`` instances. Production responses expose snake_case attributes
(``resource_version``); dict/JSON forms use camelCase (``resourceVersion``).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def configmap_metadata(body: object) -> object | None:
    """Return ConfigMap metadata from a Mapping or client model."""

    if isinstance(body, Mapping):
        return body.get("metadata")
    return getattr(body, "metadata", None)


def configmap_data(body: object) -> Mapping[str, Any] | None:
    """Return ConfigMap data from a Mapping or client model."""

    if isinstance(body, Mapping):
        raw = body.get("data")
        return raw if isinstance(raw, Mapping) else None
    raw = getattr(body, "data", None)
    return raw if isinstance(raw, Mapping) else None


def object_meta_fields(value: object | None) -> tuple[str, str, str]:
    """Return ``(resource_version, uid, name)`` from Mapping or ``V1ObjectMeta``."""

    if value is None:
        return "", "", ""
    if isinstance(value, Mapping):
        resource_version = str(value.get("resourceVersion") or value.get("resource_version") or "").strip()
        uid = str(value.get("uid") or "").strip()
        name = str(value.get("name") or "").strip()
        return resource_version, uid, name
    resource_version = str(
        getattr(value, "resource_version", None) or getattr(value, "resourceVersion", None) or ""
    ).strip()
    uid = str(getattr(value, "uid", "") or "").strip()
    name = str(getattr(value, "name", "") or "").strip()
    return resource_version, uid, name


def object_meta_labels(value: object | None) -> dict[str, str]:
    """Return labels from Mapping or client ObjectMeta."""

    if value is None:
        return {}
    if isinstance(value, Mapping):
        raw = value.get("labels")
    else:
        raw = getattr(value, "labels", None)
    if not isinstance(raw, Mapping):
        return {}
    return {str(key): str(item) for key, item in raw.items()}


def object_meta_namespace(value: object | None) -> str:
    """Return namespace from Mapping or client ObjectMeta."""

    if value is None:
        return ""
    if isinstance(value, Mapping):
        return str(value.get("namespace") or "").strip()
    return str(getattr(value, "namespace", "") or "").strip()


__all__ = [
    "configmap_data",
    "configmap_metadata",
    "object_meta_fields",
    "object_meta_labels",
    "object_meta_namespace",
]
