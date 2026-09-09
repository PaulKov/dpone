"""Compatibility facade for object storage access preflight APIs."""

from __future__ import annotations

from dpone.runtime.object_storage_access_models import (
    ObjectStorageAccessEvidence,
    ObjectStorageAccessRequest,
    ObjectStorageConnectionRef,
    ObjectStorageReadContract,
    ObjectStorageRuntimeAccess,
)
from dpone.runtime.object_storage_clickhouse_probe import ClickHouseObjectStorageReadinessProbe
from dpone.runtime.object_storage_connection_resolver import ObjectStorageConnectionResolver
from dpone.runtime.object_storage_fast_path_preflight import ObjectStorageAccessPreflightService

__all__ = [
    "ClickHouseObjectStorageReadinessProbe",
    "ObjectStorageAccessEvidence",
    "ObjectStorageAccessPreflightService",
    "ObjectStorageAccessRequest",
    "ObjectStorageConnectionRef",
    "ObjectStorageConnectionResolver",
    "ObjectStorageReadContract",
    "ObjectStorageRuntimeAccess",
]
