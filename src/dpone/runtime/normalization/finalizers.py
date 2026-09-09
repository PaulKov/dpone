"""Sink-native finalization plans for nested child reconciliation."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

Dialect = Literal["mssql", "postgres", "clickhouse", "bigquery", "kafka"]


@dataclass(frozen=True, slots=True)
class ChildDeleteFinalizerPlan:
    """A staging-first child delete finalization plan."""

    dialect: str
    operation: str
    target_table: str
    deleted_keys_staging_table: str
    unique_key: tuple[str, ...]
    statements: tuple[str, ...]
    requires_staging: bool = True
    warnings: tuple[str, ...] = ()
    event_contract: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "dialect": self.dialect,
            "operation": self.operation,
            "target_table": self.target_table,
            "deleted_keys_staging_table": self.deleted_keys_staging_table,
            "unique_key": list(self.unique_key),
            "statements": list(self.statements),
            "requires_staging": self.requires_staging,
            "warnings": list(self.warnings),
            "event_contract": self.event_contract,
        }


class ChildDeleteFinalizerService:
    """Render sink-native staged delete plans for child tables."""

    def plan_delete(
        self,
        *,
        dialect: str,
        target_table: str,
        deleted_keys_staging_table: str,
        unique_key: Sequence[str],
    ) -> ChildDeleteFinalizerPlan:
        key = tuple(str(item) for item in unique_key)
        if not key:
            raise ValueError("child delete finalizer requires at least one unique_key column")
        renderer = _RENDERERS.get(str(dialect).lower())
        if renderer is None:
            raise ValueError(f"Unsupported child delete finalizer dialect `{dialect}`")
        return renderer(target_table, deleted_keys_staging_table, key)


def _mssql(target: str, staging: str, key: tuple[str, ...]) -> ChildDeleteFinalizerPlan:
    predicate = " AND ".join(f"target.[{column}] = deleted_keys.[{column}]" for column in key)
    return ChildDeleteFinalizerPlan(
        dialect="mssql",
        operation="child_delete",
        target_table=target,
        deleted_keys_staging_table=staging,
        unique_key=key,
        statements=(
            f"DELETE target\nFROM {target} AS target\nINNER JOIN {staging} AS deleted_keys\n  ON {predicate};",
        ),
    )


def _postgres(target: str, staging: str, key: tuple[str, ...]) -> ChildDeleteFinalizerPlan:
    predicate = " AND ".join(f'target."{column}" = deleted_keys."{column}"' for column in key)
    return ChildDeleteFinalizerPlan(
        dialect="postgres",
        operation="child_delete",
        target_table=target,
        deleted_keys_staging_table=staging,
        unique_key=key,
        statements=(f"DELETE FROM {target} AS target\nUSING {staging} AS deleted_keys\nWHERE {predicate};",),
    )


def _bigquery(target: str, staging: str, key: tuple[str, ...]) -> ChildDeleteFinalizerPlan:
    predicate = " AND ".join(f"target.`{column}` = deleted_keys.`{column}`" for column in key)
    return ChildDeleteFinalizerPlan(
        dialect="bigquery",
        operation="child_delete",
        target_table=target,
        deleted_keys_staging_table=staging,
        unique_key=key,
        statements=(
            f"DELETE FROM `{target}` AS target\nWHERE EXISTS (SELECT 1 FROM `{staging}` AS deleted_keys WHERE {predicate});",
        ),
    )


def _clickhouse(target: str, staging: str, key: tuple[str, ...]) -> ChildDeleteFinalizerPlan:
    key_expr = _tuple_expr(key)
    return ChildDeleteFinalizerPlan(
        dialect="clickhouse",
        operation="child_delete",
        target_table=target,
        deleted_keys_staging_table=staging,
        unique_key=key,
        statements=(f"DELETE FROM {target} WHERE {key_expr} IN (SELECT {key_expr} FROM {staging});",),
        warnings=("ClickHouse lightweight deletes are asynchronous until background cleanup completes.",),
    )


def _kafka(target: str, staging: str, key: tuple[str, ...]) -> ChildDeleteFinalizerPlan:
    return ChildDeleteFinalizerPlan(
        dialect="kafka",
        operation="child_delete_events",
        target_table=target,
        deleted_keys_staging_table=staging,
        unique_key=key,
        statements=(),
        requires_staging=True,
        event_contract={"op": "delete", "key_columns": list(key), "source": staging, "target": target},
    )


def _tuple_expr(key: tuple[str, ...]) -> str:
    if len(key) == 1:
        return key[0]
    return "tuple(" + ", ".join(key) + ")"


_RENDERERS = {
    "mssql": _mssql,
    "postgres": _postgres,
    "bigquery": _bigquery,
    "clickhouse": _clickhouse,
    "kafka": _kafka,
}
