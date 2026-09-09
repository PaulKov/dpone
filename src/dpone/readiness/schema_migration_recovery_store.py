"""Recovery catalog persistence ports and local JSON backend."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

RECOVERY_CATALOG_SCHEMA = "dpone.schema_migration_recovery_catalog.v1"


class RecoveryCatalogStore(Protocol):
    def append(self, point: Mapping[str, Any]) -> None: ...

    def get(self, restore_point_id: str) -> dict[str, Any] | None: ...

    def query(
        self,
        *,
        target: str | None = None,
        environment: str | None = None,
        status: str | None = None,
    ) -> tuple[dict[str, Any], ...]: ...


@dataclass(frozen=True, slots=True)
class LocalJsonRecoveryCatalogStore:
    path: Path

    def append(self, point: Mapping[str, Any]) -> None:
        records = list(self.query())
        incoming = dict(point)
        for existing in records:
            if existing.get("restore_point_id") == incoming.get("restore_point_id"):
                if _canonical(existing) == _canonical(incoming):
                    return
                raise ValueError("recovery_catalog.destination_conflict")
            if _same_destination(existing, incoming):
                raise ValueError("recovery_catalog.destination_conflict")
        records.append(incoming)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            _canonical({"schema_version": RECOVERY_CATALOG_SCHEMA, "restore_points": records}) + "\n",
            encoding="utf-8",
        )

    def get(self, restore_point_id: str) -> dict[str, Any] | None:
        return next((point for point in self.query() if point.get("restore_point_id") == restore_point_id), None)

    def query(
        self,
        *,
        target: str | None = None,
        environment: str | None = None,
        status: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        points = _read_points(self.path)
        return tuple(
            point
            for point in points
            if (target is None or _target_key(point.get("target", {})) == target)
            and (environment is None or point.get("environment") == environment)
            and (status is None or point.get("status") == status)
        )


def _read_points(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        return ()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("recovery catalog must be a JSON object")
    points = raw.get("restore_points", [])
    if not isinstance(points, list):
        raise ValueError("recovery catalog restore_points must be a list")
    return tuple(dict(item) for item in points if isinstance(item, Mapping))


def _same_destination(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return (
        _target_key(left.get("target", {})) == _target_key(right.get("target", {}))
        and left.get("environment") == right.get("environment")
        and left.get("destination") == right.get("destination")
    )


def _target_key(raw: object) -> str:
    target = dict(raw) if isinstance(raw, Mapping) else {}
    return f"{target.get('sink_type')}.{target.get('table')}"


def _canonical(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


__all__ = ["LocalJsonRecoveryCatalogStore", "RECOVERY_CATALOG_SCHEMA", "RecoveryCatalogStore"]
