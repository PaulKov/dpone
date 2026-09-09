"""ClickHouse backup/restore SQL dialect for migration evidence contracts."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_DESTINATION = re.compile(r"^(Disk|File|S3|AzureBlobStorage)\s*\(", re.IGNORECASE)
_SETTING_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class ClickHouseBackupDialect:
    """Render safe ClickHouse BACKUP/RESTORE statements."""

    def render_backup_table(self, *, table: str, destination: str, settings: Mapping[str, Any]) -> str:
        clause = f"BACKUP TABLE {_quote_table(table)} TO {_destination(destination)}"
        return _with_settings(clause, settings)

    def render_restore_table(
        self,
        *,
        source_table: str,
        restored_table: str,
        destination: str,
        settings: Mapping[str, Any],
    ) -> str:
        clause = (
            f"RESTORE TABLE {_quote_table(source_table)} AS {_quote_table(restored_table)} "
            f"FROM {_destination(destination)}"
        )
        return _with_settings(clause, settings)


def _with_settings(sql: str, settings: Mapping[str, Any]) -> str:
    if not settings:
        return sql
    rendered = ", ".join(f"{_setting_name(key)} = {_literal(settings[key])}" for key in sorted(settings))
    return f"{sql} SETTINGS {rendered}"


def _quote_table(table: str) -> str:
    return ".".join(_quote_identifier(part.strip("`")) for part in str(table).split(".") if part)


def _quote_identifier(value: str) -> str:
    return "`" + str(value).replace("`", "``") + "`"


def _destination(value: str) -> str:
    text = str(value).strip()
    if not _DESTINATION.match(text):
        raise ValueError(f"unsafe ClickHouse backup destination: {value}")
    return text


def _setting_name(value: object) -> str:
    text = str(value)
    if not _SETTING_NAME.fullmatch(text):
        raise ValueError(f"unsafe ClickHouse backup setting name: {value}")
    return text


def _literal(value: object) -> str:
    if isinstance(value, Mapping) and "raw" in value:
        return str(value["raw"])
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


__all__ = ["ClickHouseBackupDialect"]
