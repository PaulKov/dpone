"""ClickHouse managed operation table naming and placement."""

from __future__ import annotations

import uuid
from dataclasses import replace
from typing import Any

DEFAULT_STAGING_SCHEMA = "staging"


class ClickHouseOperationTableResolver:
    """Resolve ClickHouse sink-side managed artifact table configs.

    ClickHouse historically created runtime artifacts next to the business target.
    To keep that OSS default compatible, the generic LoadConfig default
    ``staging_schema=staging`` is treated as "same database as target". An explicit
    non-default staging schema, for example ``DWH_Tech``, becomes the managed
    artifact database for staging/projected/shadow/backup tables.
    """

    def operation_schema(self, load_config: Any) -> str:
        configured = str(getattr(load_config, "staging_schema", "") or "").strip()
        if not configured or configured == DEFAULT_STAGING_SCHEMA:
            return str(load_config.target_schema)
        return configured

    def operation_table_name(self, target_table: str, operation: str) -> str:
        return f"{target_table}__dpone_{operation}_{uuid.uuid4().hex[:8]}"

    def operation_config(self, load_config: Any, operation: str) -> Any:
        return replace(
            load_config,
            target_schema=self.operation_schema(load_config),
            target_table=self.operation_table_name(load_config.target_table, operation),
        )


__all__ = ["ClickHouseOperationTableResolver", "DEFAULT_STAGING_SCHEMA"]
