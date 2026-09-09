from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ClickHouseClientBulkOptions:
    """Canonical ClickHouse client bulk settings."""

    command: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = None
    secure: bool | None = None
    timeout_seconds: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ClickHouseHttpBulkOptions:
    """Canonical ClickHouse HTTP bulk settings."""

    host: str | None = None
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = None
    secure: bool = False
    timeout_seconds: int = 3600
    chunk_size: int = 1024 * 1024

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ClickHouseNativeTcpBulkOptions:
    """Canonical ClickHouse native protocol ingest settings."""

    enabled: bool = False
    backend: str = "auto"
    compression: str = "auto"
    host: str | None = None
    port: int = 9000
    secure: bool = False
    timeout_seconds: int | None = None
    connection_pool_size: int = 2
    query_timeout_seconds: int = 3600

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ClickHouseBulkOptions:
    """Canonical ClickHouse bulk-load options."""

    mode: str
    client: ClickHouseClientBulkOptions
    http: ClickHouseHttpBulkOptions
    native_tcp: ClickHouseNativeTcpBulkOptions
    insert_settings: dict[str, Any]
    query_id: str | None = None
    insert_deduplication_token: str | None = None
    deprecated_aliases: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "client": self.client.to_dict(),
            "http": self.http.to_dict(),
            "native_tcp": self.native_tcp.to_dict(),
            "insert_settings": dict(self.insert_settings),
            "query_id": self.query_id,
            "insert_deduplication_token": self.insert_deduplication_token,
            "deprecated_aliases": list(self.deprecated_aliases),
        }


__all__ = [
    "ClickHouseBulkOptions",
    "ClickHouseClientBulkOptions",
    "ClickHouseHttpBulkOptions",
    "ClickHouseNativeTcpBulkOptions",
]
