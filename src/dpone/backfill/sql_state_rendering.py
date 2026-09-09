"""Dialect-safe identifier and literal rendering for backfill state SQL."""

from __future__ import annotations


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def clickhouse_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def postgres_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


__all__ = ["clickhouse_identifier", "postgres_identifier", "sql_string"]
