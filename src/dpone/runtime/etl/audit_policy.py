"""Load governance audit policy helpers for ETL runtime."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class AuditPolicy:
    """Normalized load audit storage policy."""

    def __init__(self, *, enabled: bool, state_schema: str, loads_table: str, steps_table: str) -> None:
        self.enabled = enabled
        self.state_schema = state_schema
        self.loads_table = loads_table
        self.steps_table = steps_table


def audit_policy(load_config: LoadConfig) -> AuditPolicy:
    options = getattr(load_config, "options", {}) or {}
    governance = options.get("load_governance")
    if not isinstance(governance, Mapping):
        return AuditPolicy(
            enabled=True, state_schema="etl_state", loads_table="__dpone__loads", steps_table="__dpone__load_steps"
        )
    if governance.get("enabled") is False:
        return AuditPolicy(
            enabled=False, state_schema="etl_state", loads_table="__dpone__loads", steps_table="__dpone__load_steps"
        )
    audit = governance.get("audit")
    if not isinstance(audit, Mapping):
        return AuditPolicy(
            enabled=True, state_schema="etl_state", loads_table="__dpone__loads", steps_table="__dpone__load_steps"
        )
    if audit.get("mode") == "off":
        return AuditPolicy(
            enabled=False, state_schema="etl_state", loads_table="__dpone__loads", steps_table="__dpone__load_steps"
        )
    return AuditPolicy(
        enabled=bool(audit.get("enabled", True)),
        state_schema=str(audit.get("state_schema") or "etl_state"),
        loads_table=str(audit.get("loads_table") or "__dpone__loads"),
        steps_table=str(audit.get("steps_table") or "__dpone__load_steps"),
    )


def is_clickhouse_connector(connector: Any) -> bool:
    if connector is None:
        return False
    class_name = connector.__class__.__name__.lower()
    module_name = connector.__class__.__module__.lower()
    return "clickhouse" in class_name or "clickhouse" in module_name


def is_mssql_connector(connector: Any) -> bool:
    """Return whether a runtime connector is the SQL Server adapter."""

    if connector is None:
        return False
    class_name = connector.__class__.__name__.lower()
    module_name = connector.__class__.__module__.lower()
    return "mssql" in class_name or "mssql" in module_name


__all__ = [
    "AuditPolicy",
    "audit_policy",
    "is_clickhouse_connector",
    "is_mssql_connector",
]
