"""SQLite-backed route execution ledger store with atomic CAS semantics."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

from dpone.ops.routes.execution_models import RouteExecutionLease, RouteExecutionStep
from dpone.ops.routes.models import RouteKey

_UTC = timezone.utc  # noqa: UP017
_DEFAULT_DB_FILE = "route_execution_ledger.sqlite3"


class SqliteRouteExecutionLedgerStore:
    """Persist route execution ledgers in SQLite for shared-runner coordination."""

    def __init__(self, *, db_path: str | Path | None = None) -> None:
        self._db_path = Path(db_path) if db_path is not None else None

    @property
    def backend_name(self) -> str:
        return "sqlite"

    def read_steps(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
    ) -> tuple[RouteExecutionStep, ...]:
        path = self.ledger_path(output_dir=output_dir, route=route, dataset=dataset, run_id=run_id)
        with self._connect(path) as conn:
            row = conn.execute(
                """
                SELECT steps_json
                FROM route_execution_runs
                WHERE route_case_id = ? AND dataset = ? AND run_id = ?
                """,
                (route.case_id, dataset, run_id),
            ).fetchone()
        if row is None:
            return tuple()
        return _steps_from_json(str(row["steps_json"]))

    def write_steps(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
        steps: tuple[RouteExecutionStep, ...],
    ) -> Path:
        path = self.ledger_path(output_dir=output_dir, route=route, dataset=dataset, run_id=run_id)
        with self._connect(path) as conn:
            _begin_immediate(conn)
            try:
                self._upsert_run(conn, path=path, route=route, dataset=dataset, run_id=run_id, steps=steps)
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        return path

    def append_steps_if_version(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
        expected_step_count: int,
        steps: tuple[RouteExecutionStep, ...],
    ) -> tuple[Path, bool]:
        path = self.ledger_path(output_dir=output_dir, route=route, dataset=dataset, run_id=run_id)
        with self._connect(path) as conn:
            _begin_immediate(conn)
            try:
                row = conn.execute(
                    """
                    SELECT step_count
                    FROM route_execution_runs
                    WHERE route_case_id = ? AND dataset = ? AND run_id = ?
                    """,
                    (route.case_id, dataset, run_id),
                ).fetchone()
                current_count = int(row["step_count"]) if row is not None else 0
                if current_count != expected_step_count:
                    conn.execute("COMMIT")
                    return path, False
                self._upsert_run(conn, path=path, route=route, dataset=dataset, run_id=run_id, steps=steps)
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        return path, True

    def acquire_lease(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        owner: str,
        now: datetime,
        ttl_seconds: int,
    ) -> tuple[RouteExecutionLease, tuple[str, ...]]:
        path = self.ledger_path(output_dir=output_dir, route=route, dataset=dataset, run_id="")
        lease_key = self.lease_key(route=route, dataset=dataset)
        with self._connect(path) as conn:
            _begin_immediate(conn)
            try:
                row = conn.execute(
                    """
                    SELECT lease_key, owner, acquired_at, expires_at, fencing_token
                    FROM route_execution_leases
                    WHERE lease_key = ?
                    """,
                    (lease_key,),
                ).fetchone()
                existing = _lease_from_row(row) if row is not None else None
                if existing and existing.owner != owner and _parse_datetime(existing.expires_at) > now:
                    conn.execute("COMMIT")
                    return existing, ("route_execution.lease_held_by_another_runner",)
                lease = _new_lease(lease_key=lease_key, owner=owner, now=now, ttl_seconds=ttl_seconds)
                conn.execute(
                    """
                    INSERT INTO route_execution_leases (
                        lease_key, route_case_id, dataset, owner, acquired_at, expires_at, fencing_token, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(lease_key) DO UPDATE SET
                        owner = excluded.owner,
                        acquired_at = excluded.acquired_at,
                        expires_at = excluded.expires_at,
                        fencing_token = excluded.fencing_token,
                        updated_at = excluded.updated_at
                    """,
                    (
                        lease.lease_key,
                        route.case_id,
                        dataset,
                        lease.owner,
                        lease.acquired_at,
                        lease.expires_at,
                        lease.fencing_token,
                        now.isoformat(),
                    ),
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        return lease, tuple()

    def ledger_path(self, *, output_dir: str | Path, route: RouteKey, dataset: str, run_id: str) -> Path:
        del route, dataset, run_id
        if self._db_path is not None:
            return self._db_path
        return Path(output_dir) / _DEFAULT_DB_FILE

    @staticmethod
    def lease_key(*, route: RouteKey, dataset: str) -> str:
        return f"{route.case_id}:{dataset}"

    def _connect(self, path: Path) -> sqlite3.Connection:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        _ensure_schema(conn)
        return conn

    @staticmethod
    def _upsert_run(
        conn: sqlite3.Connection,
        *,
        path: Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
        steps: tuple[RouteExecutionStep, ...],
    ) -> None:
        updated_at = datetime.now(_UTC).isoformat()
        conn.execute(
            """
            INSERT INTO route_execution_runs (
                route_case_id, dataset, run_id, route_json, step_count, steps_json, ledger_path, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(route_case_id, dataset, run_id) DO UPDATE SET
                route_json = excluded.route_json,
                step_count = excluded.step_count,
                steps_json = excluded.steps_json,
                ledger_path = excluded.ledger_path,
                updated_at = excluded.updated_at
            """,
            (
                route.case_id,
                dataset,
                run_id,
                json.dumps(route.to_dict(), ensure_ascii=False, sort_keys=True),
                len(steps),
                _steps_to_json(steps),
                str(path),
                updated_at,
            ),
        )


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS route_execution_runs (
            route_case_id TEXT NOT NULL,
            dataset TEXT NOT NULL,
            run_id TEXT NOT NULL,
            route_json TEXT NOT NULL,
            step_count INTEGER NOT NULL,
            steps_json TEXT NOT NULL,
            ledger_path TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (route_case_id, dataset, run_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS route_execution_leases (
            lease_key TEXT PRIMARY KEY,
            route_case_id TEXT NOT NULL,
            dataset TEXT NOT NULL,
            owner TEXT NOT NULL,
            acquired_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            fencing_token TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def _begin_immediate(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN IMMEDIATE")


def _steps_to_json(steps: tuple[RouteExecutionStep, ...]) -> str:
    return json.dumps([step.to_dict() for step in steps], ensure_ascii=False, sort_keys=True)


def _steps_from_json(value: str) -> tuple[RouteExecutionStep, ...]:
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return tuple()
    if not isinstance(payload, list):
        return tuple()
    return tuple(RouteExecutionStep.from_dict(item) for item in payload if isinstance(item, Mapping))


def _lease_from_row(row: sqlite3.Row) -> RouteExecutionLease:
    return RouteExecutionLease(
        lease_key=str(row["lease_key"]),
        owner=str(row["owner"]),
        acquired_at=str(row["acquired_at"]),
        expires_at=str(row["expires_at"]),
        fencing_token=str(row["fencing_token"]),
    )


def _new_lease(*, lease_key: str, owner: str, now: datetime, ttl_seconds: int) -> RouteExecutionLease:
    expires_at = datetime.fromtimestamp(now.timestamp() + max(ttl_seconds, 1), _UTC)
    return RouteExecutionLease(
        lease_key=lease_key,
        owner=owner,
        acquired_at=now.isoformat(),
        expires_at=expires_at.isoformat(),
        fencing_token=f"{lease_key}:{owner}:{int(now.timestamp() * 1_000_000)}",
    )


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return datetime.fromtimestamp(0, _UTC)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=_UTC)
