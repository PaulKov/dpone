"""SQLite schema contract registry store."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class SqliteSchemaContractRegistryStore:
    path: str | Path

    def append(self, version: Mapping[str, Any]) -> dict[str, Any]:
        with _connect(self.path) as conn:
            _ensure(conn)
            existing = _get(conn, str(version.get("contract_id")), str(version.get("version")))
            if existing and existing.get("contract_fingerprint") == version.get("contract_fingerprint"):
                return {
                    "status": "duplicate",
                    "contract_version_id": existing.get("contract_version_id"),
                    "blockers": [],
                }
            if existing:
                return {
                    "status": "blocked",
                    "contract_id": version.get("contract_id"),
                    "version": version.get("version"),
                    "blockers": ["schema_contract_registry.version_conflict"],
                }
            conn.execute(
                """
                insert into contract_versions (
                    contract_version_id, contract_id, version, fingerprint, created_at, payload_json
                ) values (?, ?, ?, ?, ?, ?)
                """,
                (
                    version.get("contract_version_id"),
                    version.get("contract_id"),
                    version.get("version"),
                    version.get("contract_fingerprint"),
                    version.get("created_at"),
                    json.dumps(dict(version), ensure_ascii=False, sort_keys=True),
                ),
            )
            return {"status": "recorded", "contract_version_id": version.get("contract_version_id"), "blockers": []}

    def history(self, *, contract_id: str) -> tuple[dict[str, Any], ...]:
        with _connect(self.path) as conn:
            _ensure(conn)
            rows = conn.execute(
                "select payload_json from contract_versions where contract_id = ? order by version",
                (contract_id,),
            ).fetchall()
        return tuple(json.loads(row[0]) for row in rows)

    def latest(self, *, contract_id: str) -> dict[str, Any] | None:
        history = self.history(contract_id=contract_id)
        return history[-1] if history else None

    def get(self, *, contract_id: str, version: str) -> dict[str, Any] | None:
        with _connect(self.path) as conn:
            _ensure(conn)
            return _get(conn, contract_id, version)


def _connect(path: str | Path) -> sqlite3.Connection:
    raw = Path(path)
    raw.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(raw)


def _ensure(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists contract_versions (
            contract_version_id text primary key,
            contract_id text not null,
            version text not null,
            fingerprint text not null,
            created_at text,
            payload_json text not null,
            unique(contract_id, version)
        )
        """
    )
    conn.execute("create index if not exists idx_contract_versions_contract on contract_versions(contract_id, version)")


def _get(conn: sqlite3.Connection, contract_id: str, version: str) -> dict[str, Any] | None:
    row = conn.execute(
        "select payload_json from contract_versions where contract_id = ? and version = ?",
        (contract_id, version),
    ).fetchone()
    return json.loads(row[0]) if row else None
