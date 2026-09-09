"""Factory for child snapshot stores used by nested normalization."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dpone.runtime.normalization.snapshot_sql import ChildSnapshotSqlExecutor, SqlChildSnapshotStore
from dpone.runtime.normalization.snapshot_store import ChildSnapshotStore, JsonFileChildSnapshotStore


@dataclass(frozen=True, slots=True)
class ChildSnapshotStoreOptions:
    enabled: bool = False
    backend: str = "json"
    table: str = "etl_state.__dpone__child_snapshots"
    path: str | None = None
    connection_id: str | None = None

    @classmethod
    def from_config(cls, config: object) -> ChildSnapshotStoreOptions:
        if isinstance(config, bool):
            return cls(enabled=config)
        if not isinstance(config, Mapping):
            return cls()
        return cls(
            enabled=bool(config.get("enabled", False)),
            backend=str(config.get("backend", config.get("type", "json"))),
            table=str(config.get("table", "etl_state.__dpone__child_snapshots")),
            path=str(config["path"]) if config.get("path") else None,
            connection_id=str(config["connection_id"]) if config.get("connection_id") else None,
        )


class ChildSnapshotExecutorRegistry(Protocol):
    def get(self, backend: str) -> ChildSnapshotSqlExecutor | None: ...


class ChildSnapshotStoreFactory:
    """Build configured child snapshot stores without coupling to DB connectors."""

    def __init__(self, *, executors: Mapping[str, ChildSnapshotSqlExecutor] | None = None) -> None:
        self._executors = {str(key).lower(): value for key, value in dict(executors or {}).items()}

    @classmethod
    def from_runtime_options(cls, options: Mapping[str, object] | None) -> ChildSnapshotStoreFactory:
        raw_options = dict(options or {})
        executors = raw_options.get("child_snapshot_executors", raw_options.get("_dpone_child_snapshot_executors", {}))
        if not isinstance(executors, Mapping):
            executors = {}
        return cls(executors={str(key): value for key, value in executors.items()})

    def create(self, options: ChildSnapshotStoreOptions) -> ChildSnapshotStore | None:
        if not options.enabled:
            return None
        backend = _normalize_backend(options.backend)
        if backend == "json":
            return JsonFileChildSnapshotStore(options.path or str(Path(".dpone") / "state" / "child_snapshots.json"))
        executor = self._executors.get(backend)
        if executor is None:
            raise ValueError(f"child snapshot store backend `{backend}` requires a registered SQL executor")
        return SqlChildSnapshotStore(dialect=backend, table=options.table, executor=executor)


def _normalize_backend(backend: str) -> str:
    normalized = str(backend or "json").strip().lower()
    aliases = {"local": "json", "file": "json", "postgresql": "postgres", "sqlserver": "mssql"}
    return aliases.get(normalized, normalized)
