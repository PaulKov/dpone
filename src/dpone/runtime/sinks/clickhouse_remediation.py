"""ClickHouse remediation SQL dialect for controlled rollback plans."""

from __future__ import annotations

import re
from dataclasses import dataclass

_SETTING_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class ClickHouseRemediationDialect:
    """Render ClickHouse DDL/DML primitives used by remediation plans."""

    def render_shadow_exchange(self, *, actual_table: str, backup_table: str) -> str:
        return f"EXCHANGE TABLES {_quote_table(actual_table)} AND {_quote_table(backup_table)}"

    def render_drop_shadow(self, *, shadow_table: str) -> str:
        return f"DROP TABLE {_quote_table(shadow_table)}"

    def render_reset_setting(self, *, table: str, setting: str) -> str:
        return f"ALTER TABLE {_quote_table(table)} RESET SETTING {_setting_name(setting)}"

    def render_modify_setting(self, *, table: str, setting: str, value: str | int | float | bool) -> str:
        return f"ALTER TABLE {_quote_table(table)} MODIFY SETTING {_setting_name(setting)} = {_literal(value)}"


def _quote_table(table: str) -> str:
    return ".".join(_quote_identifier(part.strip("`")) for part in str(table).split(".") if part)


def _quote_identifier(value: str) -> str:
    return "`" + str(value).replace("`", "``") + "`"


def _setting_name(value: str) -> str:
    text = str(value)
    if not _SETTING_NAME.fullmatch(text):
        raise ValueError(f"unsafe ClickHouse setting name: {value}")
    return text


def _literal(value: str | int | float | bool) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


__all__ = ["ClickHouseRemediationDialect"]
