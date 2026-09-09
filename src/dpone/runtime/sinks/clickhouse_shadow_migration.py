"""ClickHouse shadow-table migration SQL dialect."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDdlRenderer, ClickHouseTableDesign


@dataclass(frozen=True, slots=True)
class ClickHouseShadowMigrationDialect:
    """Render ClickHouse SQL for shadow migration phases."""

    def render_create_shadow(self, *, desired: Any, shadow_table: str) -> str:
        design = ClickHouseTableDesign.from_options(
            {
                "physical_design": {
                    "storage": {
                        "clickhouse": {
                            "engine": desired.engine or "MergeTree",
                            "partition_by": desired.partition_by,
                            "order_by": list(desired.order_by),
                            "ttl": getattr(desired, "ttl", None),
                            "cluster": {
                                "name": getattr(desired, "cluster", None),
                                "on_cluster": bool(getattr(desired, "on_cluster", False)),
                            },
                            "table_settings": dict(desired.table_settings),
                        }
                    }
                }
            }
        )
        columns_sql = [f"`{column.name}` {column.target_type}" for column in desired.columns.values()]
        return ClickHouseTableDdlRenderer().render_create_table(
            table=_quote_table(shadow_table),
            columns_sql=columns_sql,
            design=design,
        )

    def render_backfill(
        self,
        *,
        actual_table: str,
        shadow_table: str,
        columns: Sequence[str],
        expressions: Sequence[str],
    ) -> str:
        return (
            f"INSERT INTO {_quote_table(shadow_table)} ({', '.join(_quote_identifier(item) for item in columns)}) "
            f"SELECT {', '.join(expressions)} FROM {_quote_table(actual_table)}"
        )

    def render_validation_checks(
        self,
        *,
        actual_table: str,
        shadow_table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
    ) -> tuple[str, ...]:
        checks = [
            f"SELECT count() FROM {_quote_table(actual_table)}",
            f"SELECT count() FROM {_quote_table(shadow_table)}",
        ]
        if columns:
            expr = ", ".join(_quote_identifier(column) for column in columns)
            checks.extend(
                [
                    f"SELECT cityHash64({expr}) FROM {_quote_table(actual_table)} ORDER BY {expr}",
                    f"SELECT cityHash64({expr}) FROM {_quote_table(shadow_table)} ORDER BY {expr}",
                ]
            )
        if key_columns:
            keys = ", ".join(_quote_identifier(column) for column in key_columns)
            checks.append(
                f"SELECT {keys}, count() FROM {_quote_table(shadow_table)} GROUP BY {keys} HAVING count() > 1"
            )
        return tuple(checks)

    def render_cutover(self, *, actual_table: str, shadow_table: str) -> str:
        return f"EXCHANGE TABLES {_quote_table(actual_table)} AND {_quote_table(shadow_table)}"

    def render_contract(self, *, retained_table: str) -> str:
        return f"DROP TABLE {_quote_table(retained_table)}"

    def render_rollback(self, *, actual_table: str, shadow_table: str) -> tuple[str, ...]:
        return (self.render_cutover(actual_table=actual_table, shadow_table=shadow_table),)


def _quote_table(table: str) -> str:
    return ".".join(_quote_identifier(part.strip("`")) for part in table.split("."))


def _quote_identifier(value: str) -> str:
    return "`" + str(value).replace("`", "``") + "`"


__all__ = ["ClickHouseShadowMigrationDialect"]
