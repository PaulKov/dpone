"""SQL column lineage extraction for schema contract consumers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from sqlglot import expressions as exp
from sqlglot import parse_one


@dataclass(frozen=True, slots=True)
class SqlColumnLineageExtractor:
    """Extracts best-effort column reads from local SQL text with sqlglot."""

    dialect: str | None = None

    def extract(
        self,
        *,
        sql: str,
        consumer_id: str,
        target_table: str,
        target_columns: Sequence[str],
        path: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        parsed = parse_one(sql, read=self.dialect) if self.dialect else parse_one(sql)
        if not _references_target(parsed, target_table):
            return ()
        columns = _columns(parsed, target_columns)
        if not columns:
            return (
                {
                    "consumer_id": consumer_id,
                    "dataset": target_table,
                    "column": None,
                    "confidence": "table_only",
                    "path": path,
                },
            )
        return tuple(
            {
                "consumer_id": consumer_id,
                "dataset": target_table,
                "column": column,
                "confidence": "parsed",
                "path": path,
            }
            for column in sorted(columns)
        )


def _references_target(parsed: exp.Expression, target_table: str) -> bool:
    targets = _target_names(target_table)
    return any(_table_names(table) & targets for table in parsed.find_all(exp.Table))


def _columns(parsed: exp.Expression, target_columns: Sequence[str]) -> set[str]:
    allowed = {str(column).lower() for column in target_columns if str(column)}
    if any(isinstance(item, exp.Star) for item in parsed.find_all(exp.Star)):
        return set()
    found = {str(column.name).lower() for column in parsed.find_all(exp.Column) if str(column.name).lower() in allowed}
    return {column for column in allowed if column in found}


def _target_names(target_table: str) -> set[str]:
    normalized = _normalize_identifier(target_table)
    parts = normalized.split(".")
    names = {normalized}
    if parts:
        names.add(parts[-1])
    if len(parts) >= 2:
        names.add(".".join(parts[-2:]))
    return names


def _table_names(table: exp.Table) -> set[str]:
    parts = [str(part) for part in (table.catalog, table.db, table.name) if str(part)]
    names = {_normalize_identifier(".".join(parts))}
    if table.name:
        names.add(_normalize_identifier(str(table.name)))
    if table.db and table.name:
        names.add(_normalize_identifier(f"{table.db}.{table.name}"))
    return names


def _normalize_identifier(value: str) -> str:
    return value.replace("`", "").replace('"', "").lower()


__all__ = ["SqlColumnLineageExtractor"]
