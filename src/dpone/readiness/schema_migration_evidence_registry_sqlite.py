"""SQLite store for schema migration evidence registry records."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.readiness.schema_migration_evidence_registry import (
    MigrationEvidenceQuery,
    logical_key,
    record_matches,
    sort_records,
)


class SqliteEvidenceRegistryStore:
    """Persist registry records in SQLite for shared CI runners."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @property
    def backend_name(self) -> str:
        return "sqlite"

    def append(self, record: Mapping[str, Any]) -> dict[str, Any]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                decision = _append_decision(conn, record)
                if decision["status"] == "recorded":
                    _insert(conn, record)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return decision

    def query(self, query: MigrationEvidenceQuery) -> tuple[dict[str, Any], ...]:
        with self._connect() as conn:
            rows = conn.execute("SELECT payload_json FROM evidence_records").fetchall()
        records = [_record_from_row(row) for row in rows]
        return sort_records(tuple(record for record in records if record_matches(record, query)))

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        _ensure_schema(conn)
        return conn


def _append_decision(conn: sqlite3.Connection, record: Mapping[str, Any]) -> dict[str, Any]:
    record_id = str(record.get("record_id", ""))
    if conn.execute("SELECT 1 FROM evidence_records WHERE record_id = ?", (record_id,)).fetchone():
        return _decision("duplicate", record, ())
    environment, stage, pack_id, bundle_id = logical_key(record)
    row = conn.execute(
        """
        SELECT record_id FROM evidence_records
        WHERE environment = ? AND stage = ? AND pack_id = ? AND bundle_id = ?
        """,
        (environment, stage, pack_id, bundle_id),
    ).fetchone()
    if row is not None:
        return _decision("blocked", record, ("evidence_registry.record_conflict",))
    return _decision("recorded", record, ())


def _insert(conn: sqlite3.Connection, record: Mapping[str, Any]) -> None:
    target = record.get("target", {}) if isinstance(record.get("target"), Mapping) else {}
    conn.execute(
        """
        INSERT INTO evidence_records (
            record_id, target_key, environment, stage, status, pack_id, bundle_id, recorded_at, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record.get("record_id"),
            f"{target.get('sink_type')}.{target.get('table')}",
            record.get("environment"),
            record.get("stage"),
            record.get("status"),
            record.get("pack_id"),
            record.get("bundle_id"),
            record.get("recorded_at"),
            json.dumps(dict(record), ensure_ascii=False, sort_keys=True),
        ),
    )


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS evidence_records (
            record_id TEXT PRIMARY KEY,
            target_key TEXT NOT NULL,
            environment TEXT NOT NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL,
            pack_id TEXT NOT NULL,
            bundle_id TEXT NOT NULL,
            recorded_at TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_evidence_records_target_env_stage_time "
        "ON evidence_records(target_key, environment, stage, recorded_at)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_evidence_records_pack_id ON evidence_records(pack_id)")


def _record_from_row(row: sqlite3.Row) -> dict[str, Any]:
    payload = json.loads(str(row["payload_json"]))
    return dict(payload) if isinstance(payload, Mapping) else {}


def _decision(status: str, record: Mapping[str, Any], blockers: tuple[str, ...]) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_migration_evidence_registry_append.v1",
        "status": status,
        "record_id": record.get("record_id"),
        "pack_id": record.get("pack_id"),
        "bundle_id": record.get("bundle_id"),
        "blockers": list(blockers),
    }


__all__ = ["SqliteEvidenceRegistryStore"]
