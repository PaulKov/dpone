"""Object storage URI and upload result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from dpone._compat import StrEnum


class ObjectStorageWriteConflict(RuntimeError):
    """Raised when conditional creation finds an existing object."""

    def __init__(self, uri: ObjectStorageUri | str) -> None:
        self.uri = str(uri)
        super().__init__("object already exists")


class ObjectStorageReadLimitExceeded(RuntimeError):
    """Raised while streaming an object that exceeds the caller-owned limit."""

    def __init__(self, limit_bytes: int) -> None:
        self.limit_bytes = limit_bytes
        super().__init__("object body exceeds the configured read limit")


class ObjectStorageProvider(StrEnum):
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"


@dataclass(frozen=True, slots=True)
class ObjectStorageUri:
    provider: ObjectStorageProvider
    bucket: str
    key: str
    account: str | None = None

    @classmethod
    def parse(cls, value: str) -> ObjectStorageUri:
        parsed = urlparse(value)
        if parsed.query or parsed.fragment:
            raise ValueError("Object storage URI must not contain a query or fragment")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("Object storage URI must not contain embedded credentials")
        scheme = parsed.scheme.lower()
        if scheme == "s3":
            return cls(ObjectStorageProvider.S3, cls._required(parsed.netloc, value), parsed.path.removeprefix("/"))
        if scheme == "gs":
            return cls(ObjectStorageProvider.GCS, cls._required(parsed.netloc, value), parsed.path.removeprefix("/"))
        if scheme == "az":
            return cls(
                ObjectStorageProvider.AZURE_BLOB,
                cls._required(parsed.netloc, value),
                parsed.path.removeprefix("/"),
            )
        if scheme == "azure":
            account = cls._required(parsed.netloc, value)
            path = parsed.path.removeprefix("/")
            container, _, key = path.partition("/")
            if not container:
                raise ValueError(f"Azure URI must include container: {value}")
            return cls(ObjectStorageProvider.AZURE_BLOB, container, key, account=account)
        raise ValueError(f"Unsupported object storage URI scheme: {value}")

    @property
    def provider_name(self) -> str:
        return self.provider.value

    def child(self, *parts: str) -> ObjectStorageUri:
        suffix = "/".join(part.strip("/") for part in parts if part.strip("/"))
        key = "/".join(part for part in (self.key.strip("/"), suffix) if part)
        return ObjectStorageUri(self.provider, self.bucket, key, account=self.account)

    def prefix(self) -> ObjectStorageUri:
        key = self.key
        if key and not key.endswith("/"):
            key += "/"
        return ObjectStorageUri(self.provider, self.bucket, key, account=self.account)

    def __str__(self) -> str:
        if self.provider == ObjectStorageProvider.S3:
            return f"s3://{self.bucket}/{self.key}".rstrip("/")
        if self.provider == ObjectStorageProvider.GCS:
            return f"gs://{self.bucket}/{self.key}".rstrip("/")
        if self.account:
            return f"azure://{self.account}/{self.bucket}/{self.key}".rstrip("/")
        return f"az://{self.bucket}/{self.key}".rstrip("/")

    @staticmethod
    def _required(value: str, raw: str) -> str:
        if not value:
            raise ValueError(f"Object storage URI is missing bucket/container: {raw}")
        return value


@dataclass(frozen=True, slots=True)
class ObjectStorageObject:
    uri: str
    size_bytes: int
    sha256: str
    content_type: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "uri": self.uri,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "content_type": self.content_type,
            "metadata": dict(self.metadata),
        }


__all__ = [
    "ObjectStorageObject",
    "ObjectStorageProvider",
    "ObjectStorageReadLimitExceeded",
    "ObjectStorageUri",
    "ObjectStorageWriteConflict",
]
