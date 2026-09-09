from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class TransferStoreCleanupPolicy:
    temp_objects: str = "on_success"
    failed_objects: str = "keep"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> TransferStoreCleanupPolicy:
        raw = dict(value or {})
        return cls(
            temp_objects=str(raw.get("temp_objects") or "on_success"),
            failed_objects=str(raw.get("failed_objects") or "keep"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TransferStoreEncryptionPolicy:
    mode: str = "provider_default"
    kms_key_id: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> TransferStoreEncryptionPolicy:
        raw = dict(value or {})
        return cls(
            mode=str(raw.get("mode") or "provider_default"),
            kms_key_id=str(raw["kms_key_id"]) if raw.get("kms_key_id") else None,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TransferStoreMultipartPolicy:
    enabled: bool = True
    part_size: int = 64 * 1024 * 1024

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> TransferStoreMultipartPolicy:
        raw = dict(value or {})
        from dpone.runtime.storage_policy import parse_byte_size

        return cls(
            enabled=_bool(raw.get("enabled"), default=True),
            part_size=parse_byte_size(raw.get("part_size", "64MiB")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TransferStorePolicy:
    store_type: str
    uri: str
    connection_id: str | None = None
    connection_type: str | None = None
    endpoint_url: str | None = None
    local_root_dir: str | None = None
    cleanup: TransferStoreCleanupPolicy = field(default_factory=TransferStoreCleanupPolicy)
    encryption: TransferStoreEncryptionPolicy = field(default_factory=TransferStoreEncryptionPolicy)
    multipart: TransferStoreMultipartPolicy = field(default_factory=TransferStoreMultipartPolicy)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> TransferStorePolicy:
        raw = dict(value)
        uri = str(raw.get("uri") or "").strip()
        if not uri:
            raise ValueError("runtime.storage.transfer_store.uri is required")
        store_type = str(raw.get("type") or _type_from_uri(uri)).strip().lower()
        return cls(
            store_type=store_type,
            uri=uri,
            connection_id=str(raw["connection_id"]) if raw.get("connection_id") else None,
            connection_type=str(raw["connection_type"]) if raw.get("connection_type") else None,
            endpoint_url=str(raw["endpoint_url"]) if raw.get("endpoint_url") else None,
            local_root_dir=str(raw["local_root_dir"]) if raw.get("local_root_dir") else None,
            cleanup=TransferStoreCleanupPolicy.from_mapping(
                raw.get("cleanup") if isinstance(raw.get("cleanup"), Mapping) else None
            ),
            encryption=TransferStoreEncryptionPolicy.from_mapping(
                raw.get("encryption") if isinstance(raw.get("encryption"), Mapping) else None
            ),
            multipart=TransferStoreMultipartPolicy.from_mapping(
                raw.get("multipart") if isinstance(raw.get("multipart"), Mapping) else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.store_type,
            "uri": self.uri,
            "connection_id": self.connection_id,
            "connection_type": self.connection_type,
            "endpoint_url": self.endpoint_url,
            "local_root_dir": self.local_root_dir,
            "cleanup": self.cleanup.to_dict(),
            "encryption": self.encryption.to_dict(),
            "multipart": self.multipart.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class TransferObjectRef:
    uri: str
    provider: str
    size_bytes: int
    sha256: str
    content_type: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "uri": self.uri,
            "provider": self.provider,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "content_type": self.content_type,
            "metadata": dict(self.metadata),
        }


def normalize_sha256(value: str) -> str:
    text = str(value).strip()
    return text if text.startswith("sha256:") else f"sha256:{text}"


def _type_from_uri(uri: str) -> str:
    scheme = uri.split(":", 1)[0].lower()
    return {"gs": "gcs", "az": "azure", "azure": "azure"}.get(scheme, scheme)


def _bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


__all__ = [
    "TransferObjectRef",
    "TransferStoreCleanupPolicy",
    "TransferStoreEncryptionPolicy",
    "TransferStoreMultipartPolicy",
    "TransferStorePolicy",
    "normalize_sha256",
]
