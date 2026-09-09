"""Immutable metadata-only Kubernetes value contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class KubernetesMetadataSnapshot:
    """Safe subset of one Kubernetes ``PartialObjectMetadata`` item."""

    resource: str
    name: str
    uid: str
    resource_version: str
    creation_timestamp: datetime | None
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    deletion_timestamp: datetime | None = None


__all__ = ["KubernetesMetadataSnapshot"]
