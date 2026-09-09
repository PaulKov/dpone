from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dpone.runtime.transfer_store_models import TransferObjectRef, TransferStorePolicy, normalize_sha256
from dpone.storage.checksum import file_sha256
from dpone.storage.models import ObjectStorageUri
from dpone.storage.protocols import ObjectStorageClient


@dataclass(frozen=True, slots=True)
class TransferStorePreflightResult:
    passed: bool
    blockers: tuple[str, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "blockers": list(self.blockers), "details": dict(self.details)}


class TransferStorePreflightService:
    def __init__(self, client: ObjectStorageClient) -> None:
        self._client = client

    def check(self, policy: TransferStorePolicy, *, scratch_dir: str | Path) -> TransferStorePreflightResult:
        scratch = Path(scratch_dir)
        scratch.mkdir(parents=True, exist_ok=True)
        probe = scratch / "probe.txt"
        probe.write_text("ok\n", encoding="utf-8")
        destination = ObjectStorageUri.parse(policy.uri).prefix().child(".preflight", "probe.txt")
        try:
            uploaded = self._client.put_file(probe, destination, content_type="text/plain")
            if not self._client.exists(ObjectStorageUri.parse(uploaded.uri)):
                return _failed("transfer_store_probe_missing", policy)
            downloaded = scratch / "downloaded-probe.txt"
            self._client.get_file(ObjectStorageUri.parse(uploaded.uri), downloaded)
            if normalize_sha256(file_sha256(downloaded)) != normalize_sha256(uploaded.sha256):
                return _failed("transfer_store_probe_checksum_mismatch", policy)
            self._client.delete_prefix(ObjectStorageUri.parse(policy.uri).prefix().child(".preflight").prefix())
        except Exception as exc:
            return _failed("transfer_store_preflight_failed", policy, error=str(exc))
        finally:
            probe.unlink(missing_ok=True)
            (scratch / "downloaded-probe.txt").unlink(missing_ok=True)
        return TransferStorePreflightResult(passed=True, details={"prefix": str(ObjectStorageUri.parse(policy.uri))})


class SliceTransferStore:
    def __init__(
        self,
        *,
        client: ObjectStorageClient,
        policy: TransferStorePolicy,
        run_id: str,
        dataset: str,
        table: str,
    ) -> None:
        self._client = client
        self.policy = policy
        self.run_id = run_id
        self.dataset = dataset
        self.table = table

    def stage_file(
        self,
        local_path: str | Path,
        *,
        partition_index: int,
        slice_index: int,
        content_type: str | None = None,
    ) -> TransferObjectRef:
        path = Path(local_path)
        uploaded = self._client.put_file(
            path,
            self._object_uri(partition_index=partition_index, slice_index=slice_index, suffix=path.suffix),
            content_type=content_type,
        )
        uri = ObjectStorageUri.parse(uploaded.uri)
        return TransferObjectRef(
            uri=uploaded.uri,
            provider=uri.provider.value,
            size_bytes=uploaded.size_bytes,
            sha256=normalize_sha256(uploaded.sha256),
            content_type=uploaded.content_type,
            metadata={
                "dataset": self.dataset,
                "table": self.table,
                "run_id": self.run_id,
                "partition_index": partition_index,
                "slice_index": slice_index,
            },
        )

    def hydrate(self, ref: TransferObjectRef, local_path: str | Path) -> Path:
        target = Path(local_path)
        self._client.get_file(ObjectStorageUri.parse(ref.uri), target)
        actual = normalize_sha256(file_sha256(target))
        if actual != normalize_sha256(ref.sha256):
            target.unlink(missing_ok=True)
            raise RuntimeError("native_transfer_object_checksum_mismatch")
        if target.stat().st_size != ref.size_bytes:
            target.unlink(missing_ok=True)
            raise RuntimeError("native_transfer_object_size_mismatch")
        return target

    def hydrate_temp(self, ref: TransferObjectRef) -> Path:
        suffix = Path(ObjectStorageUri.parse(ref.uri).key).suffix or ".bin"
        handle = tempfile.NamedTemporaryFile(prefix="dpone_native_object_", suffix=suffix, delete=False)
        handle.close()
        return self.hydrate(ref, handle.name)

    def cleanup_run(self) -> int:
        if self.policy.cleanup.temp_objects not in {"eager", "on_success"}:
            return 0
        return self._client.delete_prefix(ObjectStorageUri.parse(self.policy.uri).prefix().child(self.run_id).prefix())

    def _object_uri(self, *, partition_index: int, slice_index: int, suffix: str) -> ObjectStorageUri:
        file_name = f"p{partition_index:04d}_s{slice_index:04d}{suffix or '.bin'}"
        return ObjectStorageUri.parse(self.policy.uri).prefix().child(self.run_id, file_name)


def _failed(code: str, policy: TransferStorePolicy, *, error: str | None = None) -> TransferStorePreflightResult:
    details: dict[str, Any] = {"prefix": str(ObjectStorageUri.parse(policy.uri))}
    if error:
        details["error"] = error
    return TransferStorePreflightResult(passed=False, blockers=(code,), details=details)


__all__ = ["SliceTransferStore", "TransferStorePreflightResult", "TransferStorePreflightService"]
