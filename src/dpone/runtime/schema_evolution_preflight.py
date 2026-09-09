"""Catalog-only schema-evolution gates executed before source row export."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any


class ProtectedColumnPreflight:
    """Prove that variant routing cannot detach immutable key authority."""

    def applies(
        self,
        source: Any,
        *,
        sink_dialect: str,
        enabled: bool,
        on_type_change: str,
        protected_columns: Sequence[str],
    ) -> bool:
        return bool(
            enabled
            and on_type_change == "new_column"
            and protected_columns
            and _is_postgres_source(source)
            and sink_dialect == "mssql"
        )

    def blocker(
        self,
        *,
        load_config: Any,
        source: Any,
        target_columns: Sequence[Any],
        target_exists: bool,
        protected_columns: Sequence[str],
        column_factory: Callable[..., Any],
        compare: Callable[[Sequence[Any], Sequence[Any]], Any],
    ) -> str | None:
        if not target_exists:
            return None
        projection = _source_projection(load_config, source)
        if projection is None:
            return "schema_evolution.protected_column_preflight_unavailable"
        columns = getattr(projection, "columns", None)
        if not columns:
            return "schema_evolution.protected_column_preflight_unavailable"
        source_columns = [
            column_factory(
                column.target_name,
                column.target_type,
                nullable=column.nullable,
                collation=column.collation,
            )
            for column in columns
        ]
        plan = compare(source_columns, target_columns)
        protected = {key.casefold() for key in protected_columns}
        invalid = tuple(
            change.column
            for change in plan.changes
            if change.column.casefold() in protected and change.change_type == "protected_column_type_change"
        )
        if invalid:
            return "schema_evolution.protected_column_type_change:" + ",".join(invalid)
        return None


def _source_projection(load_config: Any, source: Any) -> Any | None:
    fetch = getattr(source, "fetch_schema_projection", None)
    if not callable(fetch):
        return None
    try:
        fetched = fetch(load_config)
    except Exception:
        return None
    return getattr(fetched, "target_projection", fetched)


def _is_postgres_source(source: Any) -> bool:
    identity = f"{type(source).__module__}.{type(source).__name__}".casefold()
    return "postgres" in identity


__all__ = ["ProtectedColumnPreflight"]
