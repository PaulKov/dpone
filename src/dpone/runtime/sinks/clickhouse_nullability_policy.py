"""ClickHouse nullability policies for DDL and insert settings."""

from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from dpone.runtime.physical_design.nullability import (
    ColumnNullabilityOptions,
    NullabilityDecision,
    NullabilityMode,
    NullabilityOptions,
    NullabilityPolicy,
    NullHandling,
)

ClickHouseNullabilityMode = NullabilityMode
ClickHouseNullHandling = NullHandling
ClickHouseColumnNullabilityOptions = ColumnNullabilityOptions
ClickHouseNullabilityDecision = NullabilityDecision

_CLICKHOUSE_NULLABILITY_FIELD = "physical_design.storage.clickhouse.nullability"


@dataclass(frozen=True, slots=True)
class ClickHouseNullabilityOptions(NullabilityOptions):
    """Normalized ClickHouse nullability config from physical design."""

    @classmethod
    def from_load_config(cls, load_config: Any) -> ClickHouseNullabilityOptions:
        return cls.from_options(getattr(load_config, "options", {}) or {})

    @classmethod
    def from_options(cls, options: Mapping[str, Any] | None) -> ClickHouseNullabilityOptions:
        physical_design = _mapping((options or {}).get("physical_design"))
        clickhouse = _mapping(_mapping(physical_design.get("storage")).get("clickhouse"))
        return cls.from_config(_mapping(clickhouse.get("nullability")))

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> ClickHouseNullabilityOptions:
        base = NullabilityOptions.from_config(raw, field_prefix=_CLICKHOUSE_NULLABILITY_FIELD)
        return cls(mode=base.mode, null_handling=base.null_handling, columns=base.columns)


@dataclass(frozen=True, slots=True)
class ClickHouseTypeNullabilityDialect:
    """ClickHouse adapter for Nullable(...) type syntax."""

    def is_nullable(self, target_type: str) -> bool:
        return is_nullable_type(target_type)

    def strip_nullable(self, target_type: str) -> str:
        return strip_nullable_type(target_type)


@dataclass(frozen=True, slots=True)
class ClickHouseNullabilityPolicy:
    """Resolve ClickHouse DDL type nullability without touching data values."""

    generic_policy: NullabilityPolicy = field(
        default_factory=lambda: NullabilityPolicy(dialect=ClickHouseTypeNullabilityDialect())
    )

    def options_from_config(self, raw: Mapping[str, Any] | None) -> ClickHouseNullabilityOptions:
        return ClickHouseNullabilityOptions.from_config(raw)

    def resolve(
        self,
        *,
        load_config: Any,
        column: str,
        mapped_type: str,
    ) -> ClickHouseNullabilityDecision:
        return self.resolve_options(
            options=ClickHouseNullabilityOptions.from_load_config(load_config),
            column=column,
            mapped_type=mapped_type,
        )

    def resolve_options(
        self,
        *,
        options: ClickHouseNullabilityOptions,
        column: str,
        mapped_type: str,
    ) -> ClickHouseNullabilityDecision:
        decision = self.generic_policy.resolve_options(
            options=options,
            column=column,
            mapped_type=mapped_type,
        )
        return ClickHouseNullabilityDecision(
            target_type=decision.target_type,
            target_nullable=decision.target_nullable,
            null_handling=decision.null_handling,
            decision_source=decision.decision_source,
            reason=f"clickhouse {decision.reason}",
        )


@dataclass(frozen=True, slots=True)
class ClickHouseNullInsertPolicy:
    """Translate ClickHouse nullability config into ClickHouse-native insert settings."""

    options: ClickHouseNullabilityOptions = field(default_factory=ClickHouseNullabilityOptions)

    @classmethod
    def from_load_config(cls, load_config: Any) -> ClickHouseNullInsertPolicy:
        return cls(ClickHouseNullabilityOptions.from_load_config(load_config))

    @classmethod
    def from_options(cls, options: Mapping[str, Any] | None) -> ClickHouseNullInsertPolicy:
        return cls(ClickHouseNullabilityOptions.from_options(options))

    def merge_format_settings(self, settings: Mapping[str, Any] | None) -> dict[str, Any]:
        merged = dict(settings or {})
        if self.options.has_defaulting_policy():
            self._reject_false_setting(merged, "input_format_null_as_default")
            merged.setdefault("input_format_null_as_default", 1)
        elif self.options.has_fail_fast_policy():
            self._reject_true_setting(merged, "input_format_null_as_default")
        return merged

    def driver_settings(self) -> dict[str, Any]:
        if self.options.has_defaulting_policy():
            return {"input_format_null_as_default": True}
        return {}

    def insert_select_settings_clause(self) -> str:
        if self.options.has_defaulting_policy():
            return " SETTINGS insert_null_as_default=1"
        return ""

    def validate_rows(self, columns: Sequence[str], rows: Iterable[Sequence[Any]]) -> None:
        positions = self._fail_fast_positions(columns)
        if not positions:
            return
        for row in rows:
            for column, position in positions.items():
                if position < len(row) and row[position] is None:
                    raise ValueError(f"ClickHouse null_handling=fail_fast rejected NULL in column {column}")

    def validate_delimited_file(self, file_path: str, columns: Sequence[str], *, delimiter: str) -> None:
        positions = self._fail_fast_positions(columns)
        if not positions:
            return
        with open(file_path, encoding="utf-8", newline="") as handle:
            for row_number, row in enumerate(csv.reader(handle, delimiter=delimiter), start=1):
                for column, position in positions.items():
                    if position < len(row) and row[position] in {"", "\\N"}:
                        raise ValueError(
                            f"ClickHouse null_handling=fail_fast rejected NULL in column {column} at row {row_number}"
                        )

    def _fail_fast_positions(self, columns: Sequence[str]) -> dict[str, int]:
        return {
            column: index
            for index, column in enumerate(columns)
            if self.options.effective_mode(column) == "non_nullable_by_default"
            and self.options.effective_null_handling(column) == "fail_fast"
        }

    @staticmethod
    def _reject_false_setting(settings: Mapping[str, Any], key: str) -> None:
        if key in settings and not _truthy(settings[key]):
            raise ValueError(f"{key}=0 conflicts with clickhouse null_handling=default")

    @staticmethod
    def _reject_true_setting(settings: Mapping[str, Any], key: str) -> None:
        if key in settings and _truthy(settings[key]):
            raise ValueError(f"{key}=1 conflicts with clickhouse null_handling=fail_fast")


def nullable_clickhouse_type(clickhouse_type: str) -> str:
    """Wrap a ClickHouse type in Nullable when it is not already nullable."""

    return clickhouse_type if is_nullable_type(clickhouse_type) else f"Nullable({clickhouse_type})"


def strip_nullable_type(clickhouse_type: str) -> str:
    """Remove an outer ClickHouse Nullable wrapper when present."""

    value = str(clickhouse_type).strip()
    inner = _unwrap_call(value, "Nullable")
    if inner is not None:
        return inner
    low_cardinality = _unwrap_call(value, "LowCardinality")
    if low_cardinality is None:
        return value
    inner_nullable = _unwrap_call(low_cardinality, "Nullable")
    return f"LowCardinality({inner_nullable})" if inner_nullable is not None else value


def is_nullable_type(clickhouse_type: str) -> bool:
    """Return whether a ClickHouse type accepts NULL values."""

    value = str(clickhouse_type).strip()
    if _unwrap_call(value, "Nullable") is not None:
        return True
    low_cardinality = _unwrap_call(value, "LowCardinality")
    return low_cardinality is not None and _unwrap_call(low_cardinality, "Nullable") is not None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _truthy(value: Any) -> bool:
    return value in {1, "1", True, "true", "True"}


def _unwrap_call(value: str, name: str) -> str | None:
    prefix = f"{name}("
    if not value.startswith(prefix) or not value.endswith(")"):
        return None
    return value[len(prefix) : -1].strip()


__all__ = [
    "ClickHouseColumnNullabilityOptions",
    "ClickHouseNullHandling",
    "ClickHouseNullInsertPolicy",
    "ClickHouseNullabilityDecision",
    "ClickHouseNullabilityMode",
    "ClickHouseNullabilityOptions",
    "ClickHouseNullabilityPolicy",
    "ClickHouseTypeNullabilityDialect",
    "is_nullable_type",
    "nullable_clickhouse_type",
    "strip_nullable_type",
]
