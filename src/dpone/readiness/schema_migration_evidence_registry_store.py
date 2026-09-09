"""Local JSON store for schema migration evidence registry records."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.readiness.schema_migration_evidence_registry import (
    REGISTRY_SCHEMA,
    MigrationEvidenceQuery,
    logical_key,
    record_matches,
    sort_records,
)


class LocalJsonEvidenceRegistryStore:
    """Persist registry records as deterministic JSON for CI artifacts."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @property
    def backend_name(self) -> str:
        return "local_json"

    def append(self, record: Mapping[str, Any]) -> dict[str, Any]:
        records = list(self.records())
        decision = _append_decision(records, record)
        if decision["status"] != "recorded":
            return decision
        records.append(dict(record))
        self._write(records)
        return decision

    def query(self, query: MigrationEvidenceQuery) -> tuple[dict[str, Any], ...]:
        return sort_records(tuple(record for record in self.records() if record_matches(record, query)))

    def records(self) -> tuple[dict[str, Any], ...]:
        if not self.path.exists():
            return ()
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise ValueError("evidence registry must be a JSON object")
        records = raw.get("records", [])
        if not isinstance(records, list):
            raise ValueError("evidence registry records must be a list")
        return tuple(dict(item) for item in records if isinstance(item, Mapping))

    def _write(self, records: list[dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": REGISTRY_SCHEMA,
            "record_count": len(records),
            "records": sort_records(records),
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _append_decision(existing: list[dict[str, Any]], record: Mapping[str, Any]) -> dict[str, Any]:
    record_id = str(record.get("record_id", ""))
    key = logical_key(record)
    for current in existing:
        if current.get("record_id") == record_id:
            return _decision("duplicate", record, ())
        if logical_key(current) == key:
            return _decision("blocked", record, ("evidence_registry.record_conflict",))
    return _decision("recorded", record, ())


def _decision(status: str, record: Mapping[str, Any], blockers: tuple[str, ...]) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_migration_evidence_registry_append.v1",
        "status": status,
        "record_id": record.get("record_id"),
        "pack_id": record.get("pack_id"),
        "bundle_id": record.get("bundle_id"),
        "blockers": list(blockers),
    }


__all__ = ["LocalJsonEvidenceRegistryStore"]
