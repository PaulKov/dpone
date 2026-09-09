"""CDC setup contracts and durable offset shape.

This readiness module contains only serializable metadata and setup SQL. Live
readers live under ``dpone.runtime.cdc`` so CLI/readiness imports stay free of
optional database drivers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone._compat import StrEnum


class CDCBackend(StrEnum):
    POSTGRES_LOGICAL = "postgres_logical"
    MSSQL_CDC = "mssql_cdc"
    MSSQL_CHANGE_TRACKING = "mssql_change_tracking"


@dataclass(frozen=True, slots=True)
class CDCOffset:
    """Serializable durable CDC offset stored in a state backend."""

    backend: CDCBackend
    token: str
    snapshot_complete: bool = False

    def to_state(self) -> dict[str, Any]:
        return {
            "backend": self.backend.value,
            "token": self.token,
            "snapshot_complete": self.snapshot_complete,
        }

    @classmethod
    def from_state(cls, payload: dict[str, Any]) -> CDCOffset:
        return cls(
            backend=CDCBackend(str(payload["backend"])),
            token=str(payload["token"]),
            snapshot_complete=bool(payload.get("snapshot_complete", False)),
        )


@dataclass(frozen=True, slots=True)
class CDCConfig:
    """Source-side CDC setup configuration."""

    backend: CDCBackend
    source_schema: str
    source_table: str
    slot_name: str | None = None
    publication_name: str | None = None
    capture_instance: str | None = None
    plugin: str = "pgoutput"

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.source_schema:
            errors.append("source_schema is required")
        if not self.source_table:
            errors.append("source_table is required")
        if self.backend == CDCBackend.POSTGRES_LOGICAL and not self.slot_name:
            errors.append("slot_name is required for postgres_logical CDC")
        if self.backend == CDCBackend.MSSQL_CDC and not self.capture_instance:
            errors.append("capture_instance is required for mssql_cdc")
        return errors


def _pg_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _mssql_literal(value: str) -> str:
    return "N'" + value.replace("'", "''") + "'"


def _mssql_ident(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


def build_postgres_slot_sql(config: CDCConfig) -> str:
    """Build PostgreSQL logical decoding setup SQL.

    ``pgoutput`` is the production default and needs a publication. When no
    publication name is supplied, dpone generates a deterministic one from the
    source table identity. ``test_decoding`` remains available as an explicit
    diagnostics fallback.
    """

    errors = config.validate()
    if errors:
        raise ValueError("Invalid CDC config: " + "; ".join(errors))
    publication_name = config.publication_name or f"dpone_{config.source_schema}_{config.source_table}_pub"
    publication_sql = ""
    if config.plugin == "pgoutput":
        publication_sql = (
            "DO $$\n"
            "BEGIN\n"
            "    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = "
            f"{_pg_literal(publication_name)}) THEN\n"
            "        EXECUTE "
            + _pg_literal(
                f'CREATE PUBLICATION "{publication_name}" FOR TABLE "{config.source_schema}"."{config.source_table}"'
            )
            + ";\n"
            "    END IF;\n"
            "END $$;\n"
        )
    return (
        publication_sql
        + "SELECT * FROM pg_create_logical_replication_slot("
        + f"{_pg_literal(config.slot_name or '')}, {_pg_literal(config.plugin)}"
        + ");"
    )


def build_mssql_cdc_enable_sql(config: CDCConfig) -> str:
    """Build SQL Server CDC enablement SQL for database and source table."""

    errors = config.validate()
    if errors:
        raise ValueError("Invalid CDC config: " + "; ".join(errors))
    return (
        "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE database_id = DB_ID() AND is_cdc_enabled = 1)\n"
        "    EXEC sys.sp_cdc_enable_db;\n"
        "IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = "
        f"{_mssql_literal(config.capture_instance or '')})\n"
        "    EXEC sys.sp_cdc_enable_table "
        f"@source_schema = {_mssql_literal(config.source_schema)}, "
        f"@source_name = {_mssql_literal(config.source_table)}, "
        "@role_name = NULL, "
        f"@capture_instance = {_mssql_literal(config.capture_instance or '')}, "
        "@supports_net_changes = 1;"
    )


def build_mssql_change_tracking_enable_sql(config: CDCConfig) -> str:
    """Build SQL Server Change Tracking enablement SQL."""

    errors = config.validate()
    non_capture_errors = [error for error in errors if "capture_instance" not in error]
    if non_capture_errors:
        raise ValueError("Invalid CDC config: " + "; ".join(non_capture_errors))
    qualified = f"{_mssql_ident(config.source_schema)}.{_mssql_ident(config.source_table)}"
    literal = _mssql_literal(qualified)
    return (
        "IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())\n"
        "BEGIN\n"
        "    DECLARE @sql nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + "
        "N' SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)';\n"
        "    EXEC(@sql);\n"
        "END;\n"
        f"IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID({literal}))\n"
        f"    ALTER TABLE {qualified} ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);"
    )
