"""SQLite recovery catalog backend."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class SqliteRecoveryCatalogStore:
    path: Path

    def append(self, point: Mapping[str, Any]) -> None:
        payload = dict(point)
        with _connect(self.path) as conn:
            _init(conn)
            existing = conn.execute(
                "select payload_json from recovery_points where restore_point_id = ?",
                (payload.get("restore_point_id"),),
            ).fetchone()
            if existing:
                if existing[0] == _canonical(payload):
                    return
                raise ValueError("recovery_catalog.destination_conflict")
            conflict = conn.execute(
                """
                select 1 from recovery_points
                where target_key = ? and environment = ? and destination = ?
                """,
                (_target_key(payload.get("target", {})), payload.get("environment"), payload.get("destination")),
            ).fetchone()
            if conflict:
                raise ValueError("recovery_catalog.destination_conflict")
            conn.execute(
                """
                insert into recovery_points (
                    restore_point_id, target_key, environment, status, chain_id,
                    base_restore_point_id, created_at, destination, payload_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("restore_point_id"),
                    _target_key(payload.get("target", {})),
                    payload.get("environment"),
                    payload.get("status"),
                    payload.get("chain_id"),
                    payload.get("base_restore_point_id"),
                    payload.get("created_at"),
                    payload.get("destination"),
                    _canonical(payload),
                ),
            )

    def get(self, restore_point_id: str) -> dict[str, Any] | None:
        with _connect(self.path) as conn:
            _init(conn)
            row = conn.execute(
                "select payload_json from recovery_points where restore_point_id = ?",
                (restore_point_id,),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def query(
        self,
        *,
        target: str | None = None,
        environment: str | None = None,
        status: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        where: list[str] = []
        args: list[str] = []
        if target:
            where.append("target_key = ?")
            args.append(target)
        if environment:
            where.append("environment = ?")
            args.append(environment)
        if status:
            where.append("status = ?")
            args.append(status)
        sql = "select payload_json from recovery_points"
        if where:
            sql += " where " + " and ".join(where)
        sql += " order by created_at, restore_point_id"
        with _connect(self.path) as conn:
            _init(conn)
            rows = conn.execute(sql, tuple(args)).fetchall()
        return tuple(json.loads(row[0]) for row in rows)


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(path)


def _init(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists recovery_points (
            restore_point_id text primary key,
            target_key text not null,
            environment text not null,
            status text not null,
            chain_id text,
            base_restore_point_id text,
            created_at text,
            destination text,
            payload_json text not null
        )
        """
    )
    conn.execute(
        "create index if not exists idx_recovery_points_target on recovery_points(target_key, environment, status)"
    )
    conn.execute("create index if not exists idx_recovery_points_chain on recovery_points(chain_id)")


def _target_key(raw: object) -> str:
    target = dict(raw) if isinstance(raw, Mapping) else {}
    return f"{target.get('sink_type')}.{target.get('table')}"


def _canonical(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


__all__ = ["SqliteRecoveryCatalogStore"]
