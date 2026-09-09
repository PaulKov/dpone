"""Backfill state-store assembly from manifest runtime options."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.backfill.sql_state import (
    ClickHouseBackfillStateStore,
    MSSQLBackfillStateStore,
    PostgresBackfillStateStore,
)
from dpone.backfill.state import FileBackfillStateStore
from dpone.contracts.connector_declarations import canonical_connector_id


class BackfillStateStoreFactory:
    """Build the configured state store while keeping orchestration DI-friendly."""

    def build(
        self,
        backfill_options: Mapping[str, Any],
        *,
        sink_type: str,
        sink_connector: Any | None = None,
    ) -> FileBackfillStateStore:
        state = backfill_options.get("state") if isinstance(backfill_options.get("state"), Mapping) else {}
        backend = str((state or {}).get("backend") or "local_file")
        if backend == "local_file":
            store = FileBackfillStateStore(backfill_options.get("state_dir"))
            self._enforce_state_requirements(store, state or {})
            return store
        if backend != "audit_schema":
            raise ValueError(f"Unsupported backfill.state.backend={backend!r}")
        if sink_connector is None:
            raise ValueError("backfill.state.backend=audit_schema requires sink_connector")
        store = self._audit_schema_store(
            sink_type=sink_type,
            sink_connector=sink_connector,
            schema=str((state or {}).get("schema") or "DWH_Tech"),
            cache_dir=backfill_options.get("state_dir"),
        )
        self._enforce_state_requirements(store, state or {})
        return store

    def build_worker(
        self,
        backfill_options: Mapping[str, Any],
        *,
        sink_type: str,
        sink_connector: Any,
        worker_id: int,
    ) -> FileBackfillStateStore:
        """Build an isolated SQL session/cache pair for one parallel lane."""

        state = backfill_options.get("state")
        backend = str(state.get("backend") or "local_file") if isinstance(state, Mapping) else "local_file"
        if backend != "audit_schema":
            raise RuntimeError("backfill.parallel_worker_state_store_factory_required")
        worker_options = dict(backfill_options)
        root = FileBackfillStateStore(backfill_options.get("state_dir")).root_dir
        worker_options["state_dir"] = str(root / ".workers" / f"worker-{worker_id}")
        return self.build(
            worker_options,
            sink_type=sink_type,
            sink_connector=sink_connector,
        )

    def _audit_schema_store(
        self,
        *,
        sink_type: str,
        sink_connector: Any,
        schema: str,
        cache_dir: Any,
    ) -> FileBackfillStateStore:
        normalized = canonical_connector_id(sink_type)
        if normalized == "clickhouse":
            return ClickHouseBackfillStateStore(sink_connector, schema=schema, cache_dir=cache_dir)
        if normalized == "postgres":
            return PostgresBackfillStateStore(sink_connector, schema=schema, cache_dir=cache_dir)
        if normalized == "mssql":
            return MSSQLBackfillStateStore(sink_connector, schema=schema, cache_dir=cache_dir)
        raise ValueError(f"Unsupported audit_schema backfill state sink_type={sink_type!r}")

    def _enforce_state_requirements(self, store: FileBackfillStateStore, state: Mapping[str, Any]) -> None:
        if not bool(state.get("require_distributed_lock")):
            return
        capabilities = store.state_capabilities()
        if capabilities.get("distributed_lock") is True:
            return
        raise ValueError(
            "distributed backfill lock is required but current state backend is not certified "
            f"(backend={capabilities.get('backend')}, lock_scope={capabilities.get('lock_scope')})"
        )


__all__ = ["BackfillStateStoreFactory"]
