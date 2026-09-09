"""Generic shadow-table migration planning contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any, Protocol


class ShadowMigrationStrategy(StrEnum):
    BLOCK = "block"
    ONLINE_SAFE = "online_safe"
    SHADOW = "shadow"


@dataclass(frozen=True, slots=True)
class ColumnProjectionItem:
    column: str
    expression: str
    decision: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ColumnProjectionPlan:
    items: tuple[ColumnProjectionItem, ...]
    blockers: tuple[str, ...] = ()

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(item.column for item in self.items)

    @property
    def expressions(self) -> tuple[str, ...]:
        return tuple(item.expression for item in self.items)

    def to_dict(self) -> dict[str, object]:
        return {"items": [item.to_dict() for item in self.items], "blockers": list(self.blockers)}


@dataclass(frozen=True, slots=True)
class MigrationOperation:
    name: str
    sql: str
    operation_type: str = "sql"

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MigrationPhase:
    name: str
    operations: tuple[MigrationOperation, ...] = ()
    validations: tuple[MigrationOperation, ...] = ()
    preconditions: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "preconditions": list(self.preconditions),
            "operations": [operation.to_dict() for operation in self.operations],
            "validations": [validation.to_dict() for validation in self.validations],
        }


@dataclass(frozen=True, slots=True)
class ShadowMigrationPlan:
    strategy: str
    actual_table: str
    shadow_table: str | None = None
    phases: tuple[MigrationPhase, ...] = ()
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    rollback: Mapping[str, Any] = field(default_factory=lambda: {"supported": False, "ddl": []})
    projection: ColumnProjectionPlan | None = None

    def phase(self, name: str) -> MigrationPhase:
        for phase in self.phases:
            if phase.name == name:
                return phase
        raise KeyError(name)

    def to_dict(self) -> dict[str, object]:
        return {
            "strategy": self.strategy,
            "actual_table": self.actual_table,
            "shadow_table": self.shadow_table,
            "phases": [phase.to_dict() for phase in self.phases],
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "rollback": dict(self.rollback),
            "projection": self.projection.to_dict() if self.projection else None,
        }


class TargetShadowMigrationDialect(Protocol):
    def render_create_shadow(self, *, desired: Any, shadow_table: str) -> str: ...

    def render_backfill(
        self,
        *,
        actual_table: str,
        shadow_table: str,
        columns: Sequence[str],
        expressions: Sequence[str],
    ) -> str: ...

    def render_validation_checks(
        self,
        *,
        actual_table: str,
        shadow_table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
    ) -> tuple[str, ...]: ...

    def render_cutover(self, *, actual_table: str, shadow_table: str) -> str: ...

    def render_contract(self, *, retained_table: str) -> str: ...

    def render_rollback(self, *, actual_table: str, shadow_table: str) -> tuple[str, ...]: ...


class ColumnProjectionPlanner:
    """Build target-independent column projection decisions."""

    def plan(self, *, desired: Any, actual: Any) -> ColumnProjectionPlan:
        actual_columns = {name.lower(): column for name, column in actual.columns.items()}
        items: list[ColumnProjectionItem] = []
        blockers: list[str] = []
        for column in desired.columns.values():
            actual_column = actual_columns.get(column.name.lower())
            if actual_column is None:
                if column.nullable or _is_nullable_type(column.target_type):
                    items.append(ColumnProjectionItem(column.name, "NULL", "new_nullable_column"))
                else:
                    blockers.append(f"migration.shadow_projection.missing_not_null_default:{column.name}")
                continue
            if _norm_type(actual_column.target_type) == _norm_type(column.target_type):
                items.append(ColumnProjectionItem(column.name, _quote_identifier(column.name), "identity"))
            elif _cast_supported(actual_column, column):
                items.append(
                    ColumnProjectionItem(
                        column.name,
                        f"CAST({_quote_identifier(column.name)}, '{column.target_type}')",
                        "cast",
                    )
                )
            else:
                blockers.append(f"migration.shadow_projection.unsupported_cast:{column.name}")
        return ColumnProjectionPlan(items=tuple(items), blockers=tuple(blockers))


@dataclass(frozen=True, slots=True)
class ShadowMigrationPlanner:
    dialect: TargetShadowMigrationDialect
    projection_planner: ColumnProjectionPlanner = field(default_factory=ColumnProjectionPlanner)

    def plan(
        self,
        *,
        desired: Any,
        actual: Any,
        strategy: ShadowMigrationStrategy | str,
        changes: Sequence[Mapping[str, Any]],
    ) -> ShadowMigrationPlan:
        resolved_strategy = _strategy(strategy)
        shadow_changes = tuple(change for change in changes if _is_shadow_change(change))
        if not shadow_changes:
            return ShadowMigrationPlan(strategy=resolved_strategy.value, actual_table=actual.table)
        if resolved_strategy is not ShadowMigrationStrategy.SHADOW:
            return ShadowMigrationPlan(
                strategy=resolved_strategy.value,
                actual_table=actual.table,
                blockers=tuple(f"migration.shadow_strategy_required:{change.get('path')}" for change in shadow_changes),
            )
        projection = self.projection_planner.plan(desired=desired, actual=actual)
        if projection.blockers:
            return ShadowMigrationPlan(
                strategy=resolved_strategy.value,
                actual_table=actual.table,
                blockers=projection.blockers,
                projection=projection,
            )
        shadow_table = _shadow_table_name(desired, actual, shadow_changes)
        phases = _phases(
            desired=desired,
            actual=actual,
            shadow_table=shadow_table,
            projection=projection,
            dialect=self.dialect,
        )
        return ShadowMigrationPlan(
            strategy=resolved_strategy.value,
            actual_table=actual.table,
            shadow_table=shadow_table,
            phases=phases,
            rollback={
                "supported": True,
                "supported_until_phase": "contract",
                "ddl": list(self.dialect.render_rollback(actual_table=actual.table, shadow_table=shadow_table)),
            },
            projection=projection,
        )


def _phases(
    *,
    desired: Any,
    actual: Any,
    shadow_table: str,
    projection: ColumnProjectionPlan,
    dialect: TargetShadowMigrationDialect,
) -> tuple[MigrationPhase, ...]:
    return (
        MigrationPhase(
            name="prepare",
            preconditions=("actual_fingerprint_matches", "migration_lock_acquired", "atomic_cutover_supported"),
        ),
        MigrationPhase(
            name="create_shadow",
            operations=(
                MigrationOperation(
                    name="create_shadow_table",
                    sql=dialect.render_create_shadow(desired=desired, shadow_table=shadow_table),
                ),
            ),
        ),
        MigrationPhase(
            name="backfill",
            operations=(
                MigrationOperation(
                    name="backfill_shadow_table",
                    sql=dialect.render_backfill(
                        actual_table=actual.table,
                        shadow_table=shadow_table,
                        columns=projection.columns,
                        expressions=projection.expressions,
                    ),
                ),
            ),
        ),
        MigrationPhase(
            name="validate",
            validations=tuple(
                MigrationOperation(name=f"validate_{index}", sql=sql, operation_type="validation")
                for index, sql in enumerate(
                    dialect.render_validation_checks(
                        actual_table=actual.table,
                        shadow_table=shadow_table,
                        columns=projection.columns,
                        key_columns=desired.order_by or actual.primary_key,
                    ),
                    start=1,
                )
            ),
        ),
        MigrationPhase(
            name="cutover",
            operations=(
                MigrationOperation(
                    name="atomic_cutover",
                    sql=dialect.render_cutover(actual_table=actual.table, shadow_table=shadow_table),
                ),
            ),
        ),
        MigrationPhase(
            name="contract",
            operations=(
                MigrationOperation(
                    name="drop_retained_table",
                    sql=dialect.render_contract(retained_table=shadow_table),
                ),
            ),
        ),
    )


def _is_shadow_change(change: Mapping[str, Any]) -> bool:
    change_type = str(change.get("change_type", ""))
    path = str(change.get("path", ""))
    return change_type in {
        "engine",
        "partition_by",
        "order_by",
        "primary_key",
        "ttl",
        "column_type",
    } or path.startswith("columns.")


def _strategy(value: ShadowMigrationStrategy | str) -> ShadowMigrationStrategy:
    if isinstance(value, ShadowMigrationStrategy):
        return value
    return ShadowMigrationStrategy(str(value))


def _shadow_table_name(
    desired: Any,
    actual: Any,
    changes: Sequence[Mapping[str, Any]],
) -> str:
    parts = desired.table.split(".")
    leaf = _safe_identifier(parts[-1] if parts else desired.table)
    schema = ".".join(parts[:-1])
    digest = _fingerprint({"desired": desired.to_dict(), "actual": actual.to_dict(), "changes": list(changes)})[:12]
    shadow = f"__dpone_shadow_{leaf}_{digest}"
    return f"{schema}.{shadow}" if schema else shadow


def _fingerprint(payload: Any) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _cast_supported(actual: Any, desired: Any) -> bool:
    if not actual.target_type or not desired.target_type:
        return False
    return True


def _is_nullable_type(value: str) -> bool:
    return _norm_type(value).startswith("nullable(")


def _norm_type(value: str) -> str:
    return "".join(str(value).lower().split())


def _quote_identifier(value: str) -> str:
    return "`" + str(value).replace("`", "``") + "`"


def _safe_identifier(value: str) -> str:
    safe = "".join(char if char.isalnum() or char == "_" else "_" for char in value)
    return safe or "table"


__all__ = [
    "ColumnProjectionItem",
    "ColumnProjectionPlan",
    "ColumnProjectionPlanner",
    "MigrationOperation",
    "MigrationPhase",
    "ShadowMigrationPlan",
    "ShadowMigrationPlanner",
    "ShadowMigrationStrategy",
    "TargetShadowMigrationDialect",
]
