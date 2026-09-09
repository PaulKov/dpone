"""ClickHouse schema identity migration SQL dialect."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ClickHouseIdentityMigrationDialect:
    """Render ClickHouse SQL for identity rename lifecycle phases."""

    def render_expand_contract_phases(
        self, *, table: str, decisions: Sequence[dict[str, Any]]
    ) -> tuple[dict[str, Any], ...]:
        return (
            _phase("expand", [self._expand_sql(table, item) for item in decisions]),
            _phase("backfill", [self._backfill_sql(table, item) for item in decisions]),
            _phase("validate", [self._validate_sql(table, item) for item in decisions], operation_type="validation"),
            _phase("contract", [self._contract_sql(table, item) for item in decisions]),
        )

    def render_direct_rename_phases(
        self, *, table: str, decisions: Sequence[dict[str, Any]]
    ) -> tuple[dict[str, Any], ...]:
        return (_phase("direct_rename", [self._rename_sql(table, item) for item in decisions]),)

    def _expand_sql(self, table: str, decision: dict[str, Any]) -> str:
        return (
            f"ALTER TABLE {_quote_table(table)} ADD COLUMN IF NOT EXISTS "
            f"{_quote_identifier(str(decision['canonical_name']))} {str(decision.get('target_type') or decision.get('source_type') or 'String')}"
        )

    def _backfill_sql(self, table: str, decision: dict[str, Any]) -> str:
        return (
            f"ALTER TABLE {_quote_table(table)} UPDATE {_quote_identifier(str(decision['canonical_name']))} = "
            f"{_quote_identifier(str(decision['observed_name']))} WHERE 1"
        )

    def _validate_sql(self, table: str, decision: dict[str, Any]) -> str:
        canonical = _quote_identifier(str(decision["canonical_name"]))
        observed = _quote_identifier(str(decision["observed_name"]))
        return f"SELECT 1 FROM {_quote_table(table)} WHERE {canonical} != {observed} LIMIT 1"

    def _contract_sql(self, table: str, decision: dict[str, Any]) -> str:
        return f"ALTER TABLE {_quote_table(table)} DROP COLUMN {_quote_identifier(str(decision['observed_name']))}"

    def _rename_sql(self, table: str, decision: dict[str, Any]) -> str:
        return (
            f"ALTER TABLE {_quote_table(table)} RENAME COLUMN {_quote_identifier(str(decision['observed_name']))} "
            f"TO {_quote_identifier(str(decision['canonical_name']))}"
        )


def _phase(name: str, sql: Sequence[str], *, operation_type: str = "sql") -> dict[str, Any]:
    key = "validations" if operation_type == "validation" else "operations"
    return {
        "name": name,
        "operations": []
        if key == "validations"
        else [_operation(name, index, item, operation_type) for index, item in enumerate(sql, start=1)],
        "validations": [_operation(name, index, item, operation_type) for index, item in enumerate(sql, start=1)]
        if key == "validations"
        else [],
        "preconditions": [],
    }


def _operation(phase: str, index: int, sql: str, operation_type: str) -> dict[str, str]:
    return {"name": f"{phase}_{index}", "operation_type": operation_type, "sql": sql}


def _quote_table(table: str) -> str:
    return ".".join(_quote_identifier(part.strip("`")) for part in table.split("."))


def _quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


__all__ = ["ClickHouseIdentityMigrationDialect"]
