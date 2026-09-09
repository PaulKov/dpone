"""Typed runtime options for finite schema-evolution policy decisions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast


class SchemaEvolutionError(RuntimeError):
    """Stable schema-evolution runtime failure."""

    def __init__(self, message: str, *, code: str = "DPONE_SCHEMA_EVOLUTION_ERROR") -> None:
        self.code = code
        rendered = f"{code}: {message}" if code != "DPONE_SCHEMA_EVOLUTION_ERROR" else message
        super().__init__(rendered)


def blocked_schema_evolution(message: str) -> SchemaEvolutionError:
    return SchemaEvolutionError(message, code="DPONE_SCHEMA_EVOLUTION_BLOCKED")


def accepts_existing_nullable_target(load_config: Any) -> bool:
    """Return whether an explicit policy accepts a nullable legacy target."""

    options = getattr(load_config, "options", {}) or {}
    return SchemaEvolutionRuntimeOptions.from_load_options(options).target_nullability == "accept_existing_nullable"


@dataclass(frozen=True)
class SchemaEvolutionRuntimeOptions:
    enabled: bool = True
    mode: Literal["strict", "additive", "widening"] = "widening"
    apply_safe: bool = True
    on_breaking: Literal["fail", "ignore"] = "fail"
    target_nullability: Literal["strict", "accept_existing_nullable"] = "strict"
    on_type_change: Literal["fail", "new_column"] = "fail"
    new_column_prefix: str = "__dpone__nc__"
    allow_reserved_dpone_columns: bool = False
    tables: Literal["evolve", "freeze", "ignore"] = "evolve"
    columns: Literal["evolve", "freeze", "ignore", "quarantine"] = "evolve"
    data_type: Literal["widen", "variant_column", "freeze", "quarantine"] = "widen"
    ddl_mode: Literal["online", "safe_window", "plan_only", "manual_approval"] = "online"
    lock_timeout_seconds: int | None = None
    statement_timeout_seconds: int | None = None
    max_table_size_for_inline_ddl: int | None = None
    on_schema_change: Literal["apply", "notify", "fail", "disable_pipeline"] = "apply"
    ledger_path: str | None = None
    allow_blocking_online: bool = False

    @classmethod
    def from_load_options(
        cls,
        options: Mapping[str, Any] | None,
    ) -> SchemaEvolutionRuntimeOptions:
        raw = (options or {}).get("schema_evolution", {})
        if raw is False:
            return cls(enabled=False)
        if raw is True or raw is None:
            raw = {}
        if not isinstance(raw, Mapping):
            raise ValueError("sink.options.schema_evolution must be an object or boolean")
        data_type = _literal(
            raw.get("data_type", "widen"),
            {"widen", "variant_column", "freeze", "quarantine"},
            "data_type",
        )
        type_change = raw.get("on_type_change", "fail")
        if data_type == "variant_column" and type_change == "fail":
            type_change = "new_column"
        return cls(
            enabled=bool(raw.get("enabled", True)),
            mode=_literal(raw.get("mode", "widening"), {"strict", "additive", "widening"}, "mode"),
            apply_safe=bool(raw.get("apply_safe", True)),
            on_breaking=_literal(raw.get("on_breaking", "fail"), {"fail", "ignore"}, "on_breaking"),
            target_nullability=_literal(
                raw.get("target_nullability", "strict"),
                {"strict", "accept_existing_nullable"},
                "target_nullability",
            ),
            on_type_change=_literal(type_change, {"fail", "new_column"}, "on_type_change"),
            new_column_prefix=str(raw.get("new_column_prefix", "__dpone__nc__")),
            allow_reserved_dpone_columns=bool(raw.get("allow_reserved_dpone_columns", False)),
            tables=_literal(raw.get("tables", "evolve"), {"evolve", "freeze", "ignore"}, "tables"),
            columns=_literal(
                raw.get("columns", "evolve"),
                {"evolve", "freeze", "ignore", "quarantine"},
                "columns",
            ),
            data_type=data_type,
            ddl_mode=_literal(
                raw.get("ddl_mode", "online"),
                {"online", "safe_window", "plan_only", "manual_approval"},
                "ddl_mode",
            ),
            lock_timeout_seconds=_optional_int(raw.get("lock_timeout_seconds"), "lock_timeout_seconds"),
            statement_timeout_seconds=_optional_int(
                raw.get("statement_timeout_seconds"),
                "statement_timeout_seconds",
            ),
            max_table_size_for_inline_ddl=_optional_int(
                raw.get("max_table_size_for_inline_ddl"),
                "max_table_size_for_inline_ddl",
            ),
            on_schema_change=_literal(
                raw.get("on_schema_change", "apply"),
                {"apply", "notify", "fail", "disable_pipeline"},
                "on_schema_change",
            ),
            ledger_path=str(raw["ledger_path"]) if raw.get("ledger_path") else None,
            allow_blocking_online=bool(raw.get("allow_blocking_online", False)),
        )


def _literal(value: Any, allowed: set[str], field_name: str):
    normalized = str(value).strip().lower()
    if normalized not in allowed:
        raise ValueError(f"schema_evolution.{field_name} must be one of: {', '.join(sorted(allowed))}")
    return cast(Any, normalized)


def _optional_int(value: Any, field_name: str) -> int | None:
    if value is None or value == "":
        return None
    result = int(value)
    if result < 0:
        raise ValueError(f"schema_evolution.{field_name} must be >= 0")
    return result


__all__ = [
    "accepts_existing_nullable_target",
    "blocked_schema_evolution",
    "SchemaEvolutionError",
    "SchemaEvolutionRuntimeOptions",
]
