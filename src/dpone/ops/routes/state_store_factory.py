"""Factory for route state-store backends."""

from __future__ import annotations

from pathlib import Path

from dpone.ops.routes.state_store import LocalRouteStateStore, RouteStateStore
from dpone.ops.routes.state_store_sqlite import SqliteRouteStateStore

LOCAL_JSON_BACKEND = "local_json"
SQLITE_BACKEND = "sqlite"
SUPPORTED_BACKENDS = (LOCAL_JSON_BACKEND, SQLITE_BACKEND)


class RouteStateStoreFactory:
    """Create route state stores from stable backend names."""

    @staticmethod
    def build(*, backend: str = LOCAL_JSON_BACKEND, store_uri: str | Path | None = None) -> RouteStateStore:
        normalized = backend.strip().lower().replace("-", "_")
        if normalized == LOCAL_JSON_BACKEND:
            return LocalRouteStateStore()
        if normalized == SQLITE_BACKEND:
            return SqliteRouteStateStore(db_path=store_uri)
        supported = ", ".join(SUPPORTED_BACKENDS)
        raise ValueError(f"Unsupported route state store backend {backend!r}; supported: {supported}")
