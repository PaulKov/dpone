"""MSSQL source-side work-table materialization provider."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.runtime.source_materialization import (
    SourceMaterializationPolicy,
    SourceMaterializedSnapshot,
)
from dpone.runtime.source_materialization_cleanup import (
    SourceMaterializationCleanupPolicy,
    SourceMaterializationCleanupResult,
    SourceMaterializationSweepResult,
)
from dpone.runtime.source_materialization_preparation import guard_source_materialization_preparation

Clock = Callable[[], float]
RunIdFactory = Callable[[], str]
IdentifierQuoter = Callable[[str], str]


@dataclass(frozen=True, slots=True)
class MssqlWorkTableLocation:
    """Validated current- or cross-database work-table namespace."""

    database: str | None
    schema: str
    quote_identifier: IdentifierQuoter

    @classmethod
    def resolve(
        cls,
        load_config: Any,
        policy: SourceMaterializationPolicy,
        *,
        quote_identifier: IdentifierQuoter,
    ) -> MssqlWorkTableLocation:
        database = _optional_coordinate(policy.work_database, "database")
        schema = str(policy.work_schema or getattr(load_config, "source_schema", "dbo") or "dbo").strip()
        if not schema:
            raise ValueError("MSSQL schema must not be empty")
        if database is not None and "." in schema:
            raise ValueError("MSSQL schema must not contain dots when work_database is explicit")
        return cls(database=database, schema=schema, quote_identifier=quote_identifier)

    def qualified_table(self, table: str) -> str:
        parts = [self.schema, str(table).strip()]
        if self.database is not None:
            parts.insert(0, self.database)
        return ".".join(self.quote_identifier(part) for part in parts)

    def system_catalog(self, view: str) -> str:
        prefix = f"{self.quote_identifier(self.database)}." if self.database else ""
        return f"{prefix}sys.{view}"

    def permissions_query(self) -> str:
        query = f"""
        SELECT
            CAST(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE') AS int) AS can_create_table,
            CAST(HAS_PERMS_BY_NAME(N'{_literal(self.schema)}', 'SCHEMA', 'ALTER') AS int) AS can_alter_schema,
            'dpone_source_materialization_permissions' AS dpone_source_materialization_permissions
        """.strip()
        if self.database is None:
            return query
        return f"EXEC {self.quote_identifier(self.database)}.sys.sp_executesql N'{_literal(query)}'"

    def evidence(self) -> dict[str, str | None]:
        return {"work_database": self.database, "work_schema": self.schema}


class MssqlWorkTableMaterializationProvider:
    """Create and clean run-scoped MSSQL work tables for source snapshots."""

    provider_id = "mssql_work_table"

    def __init__(
        self,
        connector: Any,
        *,
        run_id_factory: RunIdFactory | None = None,
        clock: Clock = time.monotonic,
    ) -> None:
        self.connector = connector
        self._run_id_factory = run_id_factory or (lambda: uuid.uuid4().hex)
        self._clock = clock

    def permissions_ok(self, load_config: Any, policy: SourceMaterializationPolicy) -> bool:
        location = self._work_table_location(load_config, policy)
        try:
            rows = self.connector.get_records(location.permissions_query(), as_dict=True)
        except Exception:
            return False
        if not rows:
            return False
        row = rows[0]
        return bool(row.get("can_create_table")) and bool(row.get("can_alter_schema"))

    def prepare(
        self,
        load_config: Any,
        *,
        query: str,
        schema: Sequence[tuple[str, str]],
        policy: SourceMaterializationPolicy,
    ) -> SourceMaterializedSnapshot:
        started = self._clock()
        columns = tuple(column for column, _ in schema)
        location = self._work_table_location(load_config, policy)
        sweep = self.sweep_stale(load_config, policy=policy)
        table = _work_table_name(policy, self._run_id_factory())
        qualified = location.qualified_table(table)
        select_columns = ", ".join(_quote(self.connector, column) for column in columns)
        create_sql = f"SELECT {select_columns} INTO {qualified} FROM ({query}) AS dpone_src"
        cleanup = MssqlWorkTableCleanup(self.connector, qualified, policy.cleanup)
        with guard_source_materialization_preparation(
            cleanup=cleanup,
            provider=self.provider_id,
            cleanup_policy=policy.cleanup_policy,
        ):
            self.connector.execute_query(create_sql)
            row_count = _row_count(self.connector, qualified)
            warnings: list[str] = []
            index_created = False
            statistics_updated = False
            if policy.index.mode in {"auto", "required"} and policy.index.boundary_columns:
                try:
                    self._create_index(qualified, policy.index.boundary_columns)
                    index_created = True
                except Exception:
                    if policy.index.mode == "required":
                        raise
                    warnings.append("source_materialization_index_auto_failed")
            if policy.update_statistics in {"auto", "required"}:
                try:
                    self.connector.execute_query(f"UPDATE STATISTICS {qualified}")
                    statistics_updated = True
                except Exception:
                    if policy.update_statistics == "required":
                        raise
                    warnings.append("source_materialization_update_statistics_auto_failed")
            duration = max(self._clock() - started, 0.0)

            def rewrite(selected_columns: Sequence[str]) -> str:
                selected = ", ".join(_quote(self.connector, column) for column in selected_columns)
                return f"SELECT {selected} FROM {qualified}"

            return SourceMaterializedSnapshot(
                qualified_name=qualified,
                rewrite_query=rewrite,
                cleanup=cleanup,
                evidence={
                    "provider": self.provider_id,
                    "work_connection_ref": policy.work_connection_ref,
                    **location.evidence(),
                    "work_table": qualified,
                    "row_count": row_count,
                    "duration_seconds": round(duration, 6),
                    "cleanup_policy": policy.cleanup_policy,
                    "cleanup": policy.cleanup.to_dict(),
                    "pre_materialization_sweep": sweep.to_dict(),
                    "ttl_hours": policy.ttl_hours,
                    "index_created": index_created,
                    "statistics_updated": statistics_updated,
                    "warnings": warnings,
                },
            )

    def sweep_stale(
        self,
        load_config: Any,
        *,
        policy: SourceMaterializationPolicy,
    ) -> SourceMaterializationSweepResult:
        """Drop expired dpone-owned work tables before creating a new snapshot."""

        location = self._work_table_location(load_config, policy)
        if not _is_safe_work_prefix(policy.table_prefix):
            return SourceMaterializationSweepResult(
                status="blocked",
                blockers=("source_materialization_unsafe_table_prefix",),
                details={**location.evidence(), "table_prefix": policy.table_prefix},
            )
        candidates = _stale_work_tables(
            self.connector,
            location=location,
            table_prefix=policy.table_prefix,
            ttl_hours=policy.ttl_hours,
        )
        deleted = 0
        deferred = 0
        warnings: list[str] = []
        for candidate in candidates:
            cleanup = MssqlWorkTableCleanup(
                self.connector,
                location.qualified_table(candidate),
                policy.cleanup,
            )()
            if cleanup.status == "deleted":
                deleted += 1
            elif cleanup.status == "deferred":
                deferred += 1
                warnings.append("source_materialization_sweep_deferred")
        status = "green" if not deferred else "warning"
        return SourceMaterializationSweepResult(
            status=status,
            scanned=len(candidates),
            deleted=deleted,
            deferred=deferred,
            warnings=tuple(dict.fromkeys(warnings)),
            details={
                "work_connection_ref": policy.work_connection_ref,
                **location.evidence(),
                "table_prefix": policy.table_prefix,
                "ttl_hours": policy.ttl_hours,
            },
        )

    def _work_table_location(
        self,
        load_config: Any,
        policy: SourceMaterializationPolicy,
    ) -> MssqlWorkTableLocation:
        return MssqlWorkTableLocation.resolve(
            load_config,
            policy,
            quote_identifier=lambda identifier: _quote(self.connector, identifier),
        )

    def _create_index(self, qualified: str, columns: Sequence[str]) -> None:
        index_name = "ix_dpone_snapshot_" + uuid.uuid4().hex[:12]
        column_sql = ", ".join(_quote(self.connector, column) for column in columns)
        self.connector.execute_query(f"CREATE INDEX {_quote(self.connector, index_name)} ON {qualified} ({column_sql})")


class MssqlWorkTableCleanup:
    """Drop MSSQL work tables without holding DWH metadata locks indefinitely."""

    def __init__(
        self,
        connector: Any,
        qualified_name: str,
        policy: SourceMaterializationCleanupPolicy,
    ) -> None:
        self._connector = connector
        self._qualified_name = qualified_name
        self._policy = policy

    def __call__(self) -> SourceMaterializationCleanupResult:
        deferred: SourceMaterializationCleanupResult | None = None
        reset_error: str | None = None
        attempts = 0
        self._connector.execute_query(f"SET LOCK_TIMEOUT {int(self._policy.lock_timeout_ms)}")
        try:
            for attempt in range(self._policy.retry_attempts + 1):
                attempts = attempt + 1
                try:
                    self._connector.execute_query(f"DROP TABLE IF EXISTS {self._qualified_name}")
                    deferred = None
                    break
                except Exception as exc:
                    if not (self._policy.defer_on_lock_timeout and _is_lock_timeout_error(exc)):
                        raise
                    deferred = SourceMaterializationCleanupResult(
                        status="deferred",
                        reason="mssql_cleanup_lock_timeout",
                        details={
                            "work_table": self._qualified_name,
                            "lock_timeout_ms": self._policy.lock_timeout_ms,
                            "attempts": attempts,
                            "message": _safe_error(exc),
                        },
                    )
                    if attempt < self._policy.retry_attempts:
                        time.sleep(self._policy.retry_backoff_ms / 1000)
        except Exception as exc:
            if not (self._policy.defer_on_lock_timeout and _is_lock_timeout_error(exc)):
                raise
        finally:
            if self._policy.reset_lock_timeout:
                try:
                    self._connector.execute_query("SET LOCK_TIMEOUT -1")
                except Exception as exc:  # pragma: no cover - defensive reset evidence
                    reset_error = _safe_error(exc)
        if deferred is not None:
            details = dict(deferred.details or {})
            if reset_error:
                details["reset_error"] = reset_error
            return SourceMaterializationCleanupResult(
                status=deferred.status,
                reason=deferred.reason,
                details=details,
            )
        return SourceMaterializationCleanupResult(
            status="deleted",
            reason=None,
            details={
                "work_table": self._qualified_name,
                "lock_timeout_ms": self._policy.lock_timeout_ms,
                "attempts": attempts,
                **({"reset_error": reset_error} if reset_error else {}),
            },
        )


def _work_table_name(policy: SourceMaterializationPolicy, run_id: str) -> str:
    suffix = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in str(run_id))
    return f"{policy.table_prefix}{suffix}"


def _optional_coordinate(value: Any, label: str) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "." in text:
        raise ValueError(f"MSSQL {label} must not contain dots")
    return text


def _is_safe_work_prefix(table_prefix: str) -> bool:
    prefix = str(table_prefix)
    return len(prefix) >= 8 and "dpone" in prefix.lower() and not any(token in prefix for token in ("%", "[", "]"))


def _stale_work_tables(
    connector: Any,
    *,
    location: MssqlWorkTableLocation,
    table_prefix: str,
    ttl_hours: int,
) -> tuple[str, ...]:
    # sys.tables.create_date uses server-local time; the worker may be in UTC.
    hours = max(int(ttl_hours), 0)
    query = f"""
    SELECT t.name AS table_name
    FROM {location.system_catalog("tables")} t
    JOIN {location.system_catalog("schemas")} s ON s.schema_id = t.schema_id
    WHERE s.name = N'{_literal(location.schema)}'
      AND t.name LIKE N'{_literal(_like_prefix(table_prefix))}'
      AND t.create_date < DATEADD(hour, -{hours}, SYSDATETIME())
    ORDER BY t.create_date ASC
    """
    rows = connector.get_records(query, as_dict=True)
    if not rows:
        return ()
    if isinstance(rows[0], dict):
        return tuple(str(row.get("table_name")) for row in rows if row.get("table_name"))
    return tuple(str(row[0]) for row in rows if row)


def _like_prefix(prefix: str) -> str:
    escaped = str(prefix).replace("[", "[[]").replace("%", "[%]").replace("_", "[_]")
    return f"{escaped}%"


def _quote(connector: Any, name: str) -> str:
    if hasattr(connector, "quote_identifier"):
        return str(connector.quote_identifier(str(name)))
    return f"[{str(name).replace(']', ']]')}]"


def _literal(value: str) -> str:
    return str(value).replace("'", "''")


def _row_count(connector: Any, qualified: str) -> int | None:
    try:
        rows = connector.get_records(f"SELECT COUNT_BIG(1) AS row_count FROM {qualified}", as_dict=True)
    except Exception:
        return None
    if not rows:
        return None
    return int(rows[0].get("row_count") or 0)


def _is_lock_timeout_error(exc: Exception) -> bool:
    text = _safe_error(exc).lower()
    return (
        "lock request time out period exceeded" in text
        or "lock_timeout" in text
        or "error 1222" in text
        or " 1222" in text
    )


def _safe_error(exc: Exception) -> str:
    return str(exc).replace("\n", " ")[:500]


__all__ = ["MssqlWorkTableCleanup", "MssqlWorkTableMaterializationProvider"]
