"""Factory for route execution ledger persistence backends."""

from __future__ import annotations

from pathlib import Path

from dpone.ops.routes.execution_store import LocalRouteExecutionLedgerStore, RouteExecutionLedgerStore
from dpone.ops.routes.execution_store_sqlite import SqliteRouteExecutionLedgerStore

LOCAL_JSON_BACKEND = "local_json"
SQLITE_BACKEND = "sqlite"
SUPPORTED_BACKENDS = (LOCAL_JSON_BACKEND, SQLITE_BACKEND)


class RouteExecutionLedgerStoreFactory:
    """Create route execution ledger stores from stable backend names."""

    @staticmethod
    def build(*, backend: str = LOCAL_JSON_BACKEND, store_uri: str | Path | None = None) -> RouteExecutionLedgerStore:
        normalized = backend.strip().lower().replace("-", "_")
        if normalized == LOCAL_JSON_BACKEND:
            return LocalRouteExecutionLedgerStore()
        if normalized == SQLITE_BACKEND:
            return SqliteRouteExecutionLedgerStore(db_path=store_uri)
        supported = ", ".join(SUPPORTED_BACKENDS)
        raise ValueError(f"Unsupported route execution ledger store backend {backend!r}; supported: {supported}")
