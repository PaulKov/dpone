"""ClickHouse temporary target adapter for safe sample runs."""

from __future__ import annotations

from collections.abc import Callable

from dpone.services.safe_sample_policy import TemporaryTargetPlan
from dpone.services.safe_sample_target_lifecycle import TemporaryTargetAdapterRegistry

_EXPIRY_COLUMN = "_dpone_sample_expires_at"


class ClickHouseTemporaryTargetAdapter:
    """Create/drop ClickHouse temporary sample targets through an injected executor."""

    def __init__(self, *, execute: Callable[[str], object]) -> None:
        self._execute = execute

    def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
        ttl_seconds = _positive_ttl_seconds(plan.ttl_seconds)
        original = _qualified_table(plan.original_table, role="original")
        temporary = _qualified_table(plan.temporary_table, role="temporary")
        temporary_schema = _required_name(plan.temporary_table, "schema", role="temporary")
        self._execute(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(temporary_schema)}")
        try:
            self._execute(f"CREATE TABLE {temporary} AS {original}")
            self._execute(
                f"ALTER TABLE {temporary} ADD COLUMN {_quote_identifier(_EXPIRY_COLUMN)} "
                f"DateTime MATERIALIZED now() + toIntervalSecond({ttl_seconds})"
            )
            self._execute(f"ALTER TABLE {temporary} MODIFY TTL {_quote_identifier(_EXPIRY_COLUMN)} DELETE")
        except Exception:
            self._drop_after_failed_create(temporary)
            raise
        return {
            "backend": "clickhouse",
            "operation": "create",
            "statements": 4,
            "server_side_expiry": True,
            "ttl_seconds": ttl_seconds,
        }

    def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
        temporary = _qualified_table(plan.temporary_table, role="temporary")
        self._execute(f"DROP TABLE IF EXISTS {temporary}")
        return {"backend": "clickhouse", "operation": "drop", "statements": 1}

    def _drop_after_failed_create(self, temporary: str) -> None:
        try:
            self._execute(f"DROP TABLE IF EXISTS {temporary}")
        except Exception:  # noqa: BLE001 - preserve the original create failure.
            pass


def _qualified_table(table: dict[str, str], *, role: str) -> str:
    schema = _required_name(table, "schema", role=role)
    name = _required_name(table, "name", role=role)
    return f"{_quote_identifier(schema)}.{_quote_identifier(name)}"


def _required_name(table: dict[str, str], key: str, *, role: str) -> str:
    value = str(table.get(key) or "")
    if not value:
        raise ValueError(f"{role} table {key} is required")
    return value


def _quote_identifier(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _positive_ttl_seconds(value: int) -> int:
    if isinstance(value, bool) or value <= 0:
        raise ValueError("temporary target ttl_seconds must be a positive integer")
    return value


def clickhouse_temporary_target_registry(*, execute: Callable[[str], object]) -> TemporaryTargetAdapterRegistry:
    registry = TemporaryTargetAdapterRegistry()
    registry.register("clickhouse", lambda _plan: ClickHouseTemporaryTargetAdapter(execute=execute))
    return registry


__all__ = ["ClickHouseTemporaryTargetAdapter", "clickhouse_temporary_target_registry"]
