"""ClickHouse recovery point restore SQL dialect."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.runtime.sinks.clickhouse_backup import ClickHouseBackupDialect


@dataclass(frozen=True, slots=True)
class ClickHouseRecoveryDialect:
    """Render ClickHouse RESTORE statements for cataloged recovery points."""

    def render_restore_table(
        self,
        *,
        source_table: str,
        restored_table: str,
        destination: str,
        settings: Mapping[str, Any],
    ) -> str:
        return ClickHouseBackupDialect().render_restore_table(
            source_table=source_table,
            restored_table=restored_table,
            destination=destination,
            settings=settings,
        )


__all__ = ["ClickHouseRecoveryDialect"]
