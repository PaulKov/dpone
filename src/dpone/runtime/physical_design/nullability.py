"""Source-agnostic physical-design nullability policy contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

NullabilityMode = Literal["preserve_source", "non_nullable_by_default"]
NullHandling = Literal["default", "fail_fast"]

_NULLABILITY_MODES = {"preserve_source", "non_nullable_by_default"}
_NULL_HANDLING_MODES = {"default", "fail_fast"}


class TargetNullabilityDialect(Protocol):
    """Target SQL/type-system adapter for nullable type syntax."""

    def is_nullable(self, target_type: str) -> bool:
        """Return whether target type syntax accepts NULL values."""

    def strip_nullable(self, target_type: str) -> str:
        """Return target type syntax with the outer nullable marker removed."""


@dataclass(frozen=True, slots=True)
class ColumnNullabilityOptions:
    """Per-column physical-design nullability override."""

    mode: NullabilityMode | None = None
    null_handling: NullHandling | None = None

    @classmethod
    def from_config(
        cls,
        raw: Mapping[str, Any] | None,
        *,
        field_prefix: str = "nullability",
    ) -> ColumnNullabilityOptions:
        values = dict(raw or {})
        mode = _optional_enum(values.get("mode"), _NULLABILITY_MODES, f"{field_prefix}.columns.*.mode")
        handling = _optional_enum(
            values.get("null_handling"),
            _NULL_HANDLING_MODES,
            f"{field_prefix}.columns.*.null_handling",
        )
        return cls(
            mode=mode,  # type: ignore[arg-type]
            null_handling=handling,  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class NullabilityOptions:
    """Normalized physical-design nullability options."""

    mode: NullabilityMode = "preserve_source"
    null_handling: NullHandling = "default"
    columns: Mapping[str, ColumnNullabilityOptions] = field(default_factory=dict)

    @classmethod
    def from_config(
        cls,
        raw: Mapping[str, Any] | None,
        *,
        field_prefix: str = "nullability",
    ) -> NullabilityOptions:
        values = _mapping(raw)
        mode = _enum(values.get("mode", "preserve_source"), _NULLABILITY_MODES, f"{field_prefix}.mode")
        handling = _enum(values.get("null_handling", "default"), _NULL_HANDLING_MODES, f"{field_prefix}.null_handling")
        columns = _mapping(values.get("columns"))
        return cls(
            mode=mode,  # type: ignore[arg-type]
            null_handling=handling,  # type: ignore[arg-type]
            columns={
                str(name): ColumnNullabilityOptions.from_config(_mapping(config), field_prefix=field_prefix)
                for name, config in columns.items()
            },
        )

    def column_options(self, column: str) -> ColumnNullabilityOptions:
        return self.columns.get(column) or self.columns.get(column.lower()) or ColumnNullabilityOptions()

    def effective_mode(self, column: str) -> NullabilityMode:
        return self.column_options(column).mode or self.mode

    def effective_null_handling(self, column: str) -> NullHandling:
        return self.column_options(column).null_handling or self.null_handling

    def has_defaulting_policy(self) -> bool:
        if self.mode == "non_nullable_by_default" and self.null_handling == "default":
            return True
        return any(
            (column.mode or self.mode) == "non_nullable_by_default"
            and (column.null_handling or self.null_handling) == "default"
            for column in self.columns.values()
        )

    def has_fail_fast_policy(self) -> bool:
        if self.mode == "non_nullable_by_default" and self.null_handling == "fail_fast":
            return True
        return any(
            (column.mode or self.mode) == "non_nullable_by_default"
            and (column.null_handling or self.null_handling) == "fail_fast"
            for column in self.columns.values()
        )


@dataclass(frozen=True, slots=True)
class NullabilityDecision:
    """Final target-type nullability decision for one column."""

    target_type: str
    target_nullable: bool
    null_handling: NullHandling
    decision_source: str
    reason: str


@dataclass(frozen=True, slots=True)
class NullabilityPolicy:
    """Resolve inferred target type nullability through a target dialect."""

    dialect: TargetNullabilityDialect

    def resolve_options(
        self,
        *,
        options: NullabilityOptions,
        column: str,
        mapped_type: str,
    ) -> NullabilityDecision:
        mode = options.effective_mode(column)
        handling = options.effective_null_handling(column)
        target_type = self.dialect.strip_nullable(mapped_type) if mode == "non_nullable_by_default" else mapped_type
        changed = target_type != mapped_type
        return NullabilityDecision(
            target_type=target_type,
            target_nullable=self.dialect.is_nullable(target_type),
            null_handling=handling,
            decision_source="physical_design" if changed else "source_mapping",
            reason=f"nullability {mode}",
        )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _enum(value: Any, allowed: set[str], field_name: str) -> str:
    text = str(value)
    if text not in allowed:
        raise ValueError(f"{field_name} is invalid")
    return text


def _optional_enum(value: Any, allowed: set[str], field_name: str) -> str | None:
    if value is None:
        return None
    return _enum(value, allowed, field_name)


__all__ = [
    "ColumnNullabilityOptions",
    "NullHandling",
    "NullabilityDecision",
    "NullabilityMode",
    "NullabilityOptions",
    "NullabilityPolicy",
    "TargetNullabilityDialect",
]
