"""Object storage adapters compatibility facade."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "AzureBlobObjectStorageClient",
    "GCSObjectStorageClient",
    "ImmutableObjectStorageClient",
    "ObjectStorageEndpointAuthority",
    "S3ObjectStorageClient",
    "LocalObjectStorageClient",
    "ObjectStorageObject",
    "ObjectStorageProvider",
    "ObjectStorageReadLimitExceeded",
    "ObjectStorageUri",
    "ObjectStorageWriteConflict",
]

_EXPORTS: dict[str, str] = {
    "AzureBlobObjectStorageClient": "dpone.storage.azure_adapter:AzureBlobObjectStorageClient",
    "GCSObjectStorageClient": "dpone.storage.adapters:GCSObjectStorageClient",
    "ImmutableObjectStorageClient": "dpone.storage.protocols:ImmutableObjectStorageClient",
    "ObjectStorageEndpointAuthority": "dpone.storage.protocols:ObjectStorageEndpointAuthority",
    "S3ObjectStorageClient": "dpone.storage.adapters:S3ObjectStorageClient",
    "LocalObjectStorageClient": "dpone.storage.local:LocalObjectStorageClient",
    "ObjectStorageObject": "dpone.storage.models:ObjectStorageObject",
    "ObjectStorageProvider": "dpone.storage.models:ObjectStorageProvider",
    "ObjectStorageReadLimitExceeded": "dpone.storage.models:ObjectStorageReadLimitExceeded",
    "ObjectStorageUri": "dpone.storage.models:ObjectStorageUri",
    "ObjectStorageWriteConflict": "dpone.storage.models:ObjectStorageWriteConflict",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":", 1)
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
