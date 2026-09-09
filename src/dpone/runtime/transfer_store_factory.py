from __future__ import annotations

import os

from dpone.runtime.transfer_store_models import TransferStorePolicy
from dpone.runtime.transfer_store_service import SliceTransferStore
from dpone.storage import (
    AzureBlobObjectStorageClient,
    GCSObjectStorageClient,
    LocalObjectStorageClient,
    S3ObjectStorageClient,
)
from dpone.storage.protocols import ObjectStorageClient


def build_slice_transfer_store(
    policy: TransferStorePolicy | None,
    *,
    run_id: str,
    dataset: str,
    table: str,
) -> SliceTransferStore | None:
    if policy is None:
        return None
    return SliceTransferStore(
        client=build_object_storage_client(policy),
        policy=policy,
        run_id=run_id,
        dataset=dataset,
        table=table,
    )


def build_object_storage_client(policy: TransferStorePolicy) -> ObjectStorageClient:
    store_type = policy.store_type.lower()
    if store_type == "local":
        root_dir = policy.local_root_dir or os.environ.get("DPONE_TRANSFER_STORE_LOCAL_ROOT")
        if not root_dir:
            raise ValueError("runtime.storage.transfer_store.local_root_dir is required for local transfer store")
        return LocalObjectStorageClient(root_dir=root_dir)
    if store_type in {"s3", "minio"}:
        return S3ObjectStorageClient(endpoint_url=policy.endpoint_url)
    if store_type == "gcs":
        return GCSObjectStorageClient()
    if store_type == "azure":
        return AzureBlobObjectStorageClient()
    raise ValueError(f"Unsupported transfer store type: {policy.store_type}")


__all__ = ["build_object_storage_client", "build_slice_transfer_store"]
