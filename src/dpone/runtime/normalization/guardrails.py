"""Explosion guardrail configuration for nested normalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

ExplosionPolicy = Literal["fail", "warn"]


@dataclass(frozen=True, slots=True)
class NormalizationGuardrails:
    """Configurable limits that prevent accidental row/table explosions."""

    max_child_tables: int | None = None
    max_rows_per_root: int | None = None
    max_array_length: int | None = None
    on_explosion: ExplosionPolicy = "fail"

    @classmethod
    def from_config(cls, config: Any) -> NormalizationGuardrails:
        if not isinstance(config, dict):
            return cls()
        return cls(
            max_child_tables=_optional_int(config.get("max_child_tables")),
            max_rows_per_root=_optional_int(config.get("max_rows_per_root")),
            max_array_length=_optional_int(config.get("max_array_length")),
            on_explosion=str(config.get("on_explosion", "fail")),  # type: ignore[arg-type]
        )


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
