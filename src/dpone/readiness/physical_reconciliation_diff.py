"""Pure physical table drift comparison primitives."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Protocol


class PhysicalColumnState(Protocol):
    @property
    def target_type(self) -> str: ...


class PhysicalTableState(Protocol):
    @property
    def columns(self) -> Mapping[str, PhysicalColumnState]: ...

    @property
    def table_settings(self) -> Mapping[str, Any]: ...

    @property
    def table_settings_error(self) -> str | None: ...


@dataclass(frozen=True, slots=True)
class PhysicalDriftChange:
    change_type: str
    path: str
    desired: Any = None
    actual: Any = None
    recommendation: str = ""

    def to_dict(self) -> dict[str, object]:
        return {key: value for key, value in asdict(self).items() if value not in (None, "")}


def scalar_changes(path: str, desired: str | None, actual: str | None) -> tuple[PhysicalDriftChange, ...]:
    if _norm_scalar(desired) == _norm_scalar(actual):
        return ()
    if desired is None and actual in (None, ""):
        return ()
    return (
        PhysicalDriftChange(
            change_type=path,
            path=path,
            desired=desired,
            actual=actual,
            recommendation="Plan a shadow table migration for physical layout drift.",
        ),
    )


def key_changes(path: str, desired: Sequence[str], actual: Sequence[str]) -> tuple[PhysicalDriftChange, ...]:
    if _norm_key(desired) == _norm_key(actual):
        return ()
    return (
        PhysicalDriftChange(
            change_type=path,
            path=path,
            desired=list(desired),
            actual=list(actual),
            recommendation="Plan a shadow table migration; key layout drift is not auto-applied.",
        ),
    )


def column_changes(desired: PhysicalTableState, actual: PhysicalTableState) -> tuple[PhysicalDriftChange, ...]:
    changes: list[PhysicalDriftChange] = []
    actual_columns = {name.lower(): column for name, column in actual.columns.items()}
    for name, column in desired.columns.items():
        actual_column = actual_columns.get(name.lower())
        if actual_column is None:
            changes.append(
                PhysicalDriftChange(
                    "column_missing",
                    f"columns.{name}",
                    desired=column.target_type,
                    actual=None,
                    recommendation="Run schema plan or expand-contract before physical reconciliation.",
                )
            )
        elif _norm_type(column.target_type) != _norm_type(actual_column.target_type):
            changes.append(
                PhysicalDriftChange(
                    "column_type",
                    f"columns.{name}.target_type",
                    desired=column.target_type,
                    actual=actual_column.target_type,
                    recommendation="Run schema plan or expand-contract before physical reconciliation.",
                )
            )
    return tuple(changes)


def table_setting_changes(desired: PhysicalTableState, actual: PhysicalTableState) -> tuple[PhysicalDriftChange, ...]:
    if desired.table_settings and actual.table_settings_error:
        return (
            PhysicalDriftChange(
                "table_settings_unparsed",
                "table_settings",
                desired=dict(desired.table_settings),
                actual=actual.table_settings_error,
                recommendation="Inspect SHOW CREATE TABLE and provide parseable table settings evidence.",
            ),
        )
    changes: list[PhysicalDriftChange] = []
    for key, desired_value in desired.table_settings.items():
        actual_value = actual.table_settings.get(key)
        if _norm_setting_value(desired_value) != _norm_setting_value(actual_value):
            changes.append(
                PhysicalDriftChange(
                    "table_setting",
                    f"table_settings.{key}",
                    desired=desired_value,
                    actual=actual_value,
                    recommendation=(
                        "Use reconciliation.mode=safe_window with approval for blocking MSSQL storage changes, "
                        "or auto_safe when the target dialect marks this table setting as online-mutable."
                    ),
                )
            )
    return tuple(changes)


def extra_table_setting_warnings(desired: PhysicalTableState, actual: PhysicalTableState) -> tuple[str, ...]:
    return tuple(
        f"physical_design.warning:table_settings.{key}"
        for key in actual.table_settings
        if key not in desired.table_settings
    )


def _norm_scalar(value: str | None) -> str:
    return "" if value is None else "".join(str(value).lower().split())


def _norm_key(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(_norm_scalar(item).strip("`") for item in values if _norm_scalar(item))


def _norm_type(value: str) -> str:
    return _norm_scalar(value)


def _norm_setting_value(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    return "" if value is None else str(value)
