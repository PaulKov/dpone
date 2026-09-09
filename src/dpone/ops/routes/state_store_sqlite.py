"""SQLite-backed route state store with atomic compare-and-swap writes."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from dpone.ops.routes.state_promotion_models import RouteStateRecord

_UTC = timezone.utc  # noqa: UP017
_DEFAULT_DB_FILE = "route_state_store.sqlite3"


class SqliteRouteStateStore:
    """Persist promoted route state in SQLite for shared local runners."""

    def __init__(self, *, db_path: str | Path | None = None) -> None:
        self._db_path = Path(db_path) if db_path is not None else None

    @property
    def backend_name(self) -> str:
        return "sqlite"

    def read_state(
        self,
        *,
        output_dir: str | Path,
        route: _RouteIdentity,
        dataset: str,
    ) -> RouteStateRecord | None:
        path = self.state_path(output_dir=output_dir, route=route, dataset=dataset)
        with self._connect(path) as conn:
            row = conn.execute(
                """
                SELECT state_json
                FROM route_state_records
                WHERE route_case_id = ? AND dataset = ?
                """,
                (route.case_id, dataset),
            ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(str(row["state_json"]))
        except json.JSONDecodeError:
            return None
        return RouteStateRecord.from_dict(payload) if isinstance(payload, Mapping) else None

    def write_state_if_version(
        self,
        *,
        output_dir: str | Path,
        route: _RouteIdentity,
        dataset: str,
        expected_version: int,
        record: RouteStateRecord,
    ) -> tuple[Path, bool]:
        path = self.state_path(output_dir=output_dir, route=route, dataset=dataset)
        with self._connect(path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    """
                    SELECT version
                    FROM route_state_records
                    WHERE route_case_id = ? AND dataset = ?
                    """,
                    (route.case_id, dataset),
                ).fetchone()
                current_version = int(row["version"]) if row is not None else 0
                if current_version != expected_version:
                    conn.execute("COMMIT")
                    return path, False
                conn.execute(
                    """
                    INSERT INTO route_state_records (
                        route_case_id, dataset, version, state_json, state_path, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(route_case_id, dataset) DO UPDATE SET
                        version = excluded.version,
                        state_json = excluded.state_json,
                        state_path = excluded.state_path,
                        updated_at = excluded.updated_at
                    """,
                    (
                        route.case_id,
                        dataset,
                        record.version,
                        json.dumps(record.to_dict(), ensure_ascii=False, sort_keys=True),
                        str(path),
                        datetime.now(_UTC).isoformat(),
                    ),
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        return path, True

    def state_path(self, *, output_dir: str | Path, route: _RouteIdentity, dataset: str) -> Path:
        del route, dataset
        if self._db_path is not None:
            return self._db_path
        return Path(output_dir) / _DEFAULT_DB_FILE

    def _connect(self, path: Path) -> sqlite3.Connection:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS route_state_records (
                route_case_id TEXT NOT NULL,
                dataset TEXT NOT NULL,
                version INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                state_path TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (route_case_id, dataset)
            )
            """
        )
        return conn


class _RouteIdentity(Protocol):
    @property
    def case_id(self) -> str: ...
