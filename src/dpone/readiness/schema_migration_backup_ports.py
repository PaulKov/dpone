"""Ports and tiny operation builders for migration backup contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class TargetBackupDialect(Protocol):
    def render_backup_table(self, *, table: str, destination: str, settings: Mapping[str, Any]) -> str: ...

    def render_restore_table(
        self,
        *,
        source_table: str,
        restored_table: str,
        destination: str,
        settings: Mapping[str, Any],
    ) -> str: ...


def restore_operation(
    source_table: str,
    restored_table: str,
    backup_destination: str,
    dialect: TargetBackupDialect | None,
) -> dict[str, Any]:
    sql = (
        dialect.render_restore_table(
            source_table=source_table,
            restored_table=restored_table,
            destination=backup_destination,
            settings={"allow_non_empty_tables": 0},
        )
        if dialect
        else ""
    )
    return {"name": "restore_table", "operation_type": "sql", "sql": sql}


__all__ = ["TargetBackupDialect", "restore_operation"]
