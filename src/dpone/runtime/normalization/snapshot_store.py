"""Stateful child snapshot storage for nested reconciliation."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class ChildSnapshotStore(Protocol):
    """Protocol for committed/staged child key snapshots."""

    def load_committed(self, *, root_table: str, child_table: str) -> list[dict[str, object]]: ...

    def stage_snapshot(
        self,
        *,
        root_table: str,
        child_table: str,
        unique_key: Sequence[str],
        keys: Sequence[Mapping[str, object]],
        load_id: str,
    ) -> None: ...

    def commit_snapshot(self, *, root_table: str, child_table: str, load_id: str) -> None: ...

    def rollback_staged(self, *, root_table: str, child_table: str, load_id: str) -> None: ...


@dataclass(frozen=True, slots=True)
class ChildSnapshotRecord:
    """Serialized child snapshot record."""

    root_table: str
    child_table: str
    unique_key: tuple[str, ...]
    keys: tuple[dict[str, object], ...]
    load_id: str
    status: str

    def to_dict(self) -> dict[str, object]:
        return {
            "root_table": self.root_table,
            "child_table": self.child_table,
            "unique_key": list(self.unique_key),
            "keys": list(self.keys),
            "load_id": self.load_id,
            "status": self.status,
        }


class JsonFileChildSnapshotStore:
    """Small durable snapshot store for local tests, CI and single-node runs."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load_committed(self, *, root_table: str, child_table: str) -> list[dict[str, object]]:
        data = self._load()
        key = _record_key(root_table, child_table)
        record = data.get(key, {}).get("committed")
        if not isinstance(record, dict):
            return []
        keys = record.get("keys", [])
        return [dict(item) for item in keys if isinstance(item, dict)]

    def stage_snapshot(
        self,
        *,
        root_table: str,
        child_table: str,
        unique_key: Sequence[str],
        keys: Sequence[Mapping[str, object]],
        load_id: str,
    ) -> None:
        data = self._load()
        key = _record_key(root_table, child_table)
        slot = data.setdefault(key, {})
        slot["staged"] = ChildSnapshotRecord(
            root_table=root_table,
            child_table=child_table,
            unique_key=tuple(unique_key),
            keys=tuple(dict(item) for item in keys),
            load_id=load_id,
            status="staged",
        ).to_dict()
        self._save(data)

    def commit_snapshot(self, *, root_table: str, child_table: str, load_id: str) -> None:
        data = self._load()
        key = _record_key(root_table, child_table)
        slot = data.setdefault(key, {})
        staged = slot.get("staged")
        if not isinstance(staged, dict) or staged.get("load_id") != load_id:
            raise ValueError(f"No staged child snapshot for {root_table}.{child_table} and load_id={load_id}")
        committed = dict(staged)
        committed["status"] = "committed"
        slot["committed"] = committed
        slot.pop("staged", None)
        self._save(data)

    def rollback_staged(self, *, root_table: str, child_table: str, load_id: str) -> None:
        data = self._load()
        key = _record_key(root_table, child_table)
        slot = data.setdefault(key, {})
        staged = slot.get("staged")
        if isinstance(staged, dict) and staged.get("load_id") == load_id:
            slot.pop("staged", None)
            self._save(data)

    def _load(self) -> dict[str, dict[str, object]]:
        if not self.path.exists():
            return {}
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}

    def _save(self, data: dict[str, dict[str, object]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def _record_key(root_table: str, child_table: str) -> str:
    return f"{root_table}::{child_table}"
