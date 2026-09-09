"""Portable type inference models.

The runtime type-system layer is deliberately pure: it does not open
connections, does not import optional database clients, and only describes what
dpone believes about source columns before target-specific rendering happens.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal

ConflictPolicy = Literal["fail", "variant_column", "quarantine"]


@dataclass(frozen=True, slots=True)
class TypeInferenceOptions:
    enabled: bool = True
    prefer_source_metadata: bool = True
    sample_rows: int = 10000
    max_sample_rows: int = 100000
    confidence_threshold: float = 0.98
    empty_string_is_null: bool = False
    conflict_policy: ConflictPolicy = "fail"

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> TypeInferenceOptions:
        values = dict(raw or {})
        if values is False:  # type: ignore[comparison-overlap]
            return cls(enabled=False)
        conflict_policy = str(values.get("conflict_policy", "fail"))
        if conflict_policy not in {"fail", "variant_column", "quarantine"}:
            raise ValueError("type_inference.conflict_policy must be one of: fail, variant_column, quarantine")
        sample_rows = int(values.get("sample_rows", 10000))
        max_sample_rows = int(values.get("max_sample_rows", 100000))
        if max_sample_rows > 100000:
            raise ValueError("type_inference.max_sample_rows must be <= 100000")
        if sample_rows < 0 or max_sample_rows < 0:
            raise ValueError("type_inference sample sizes must be >= 0")
        return cls(
            enabled=bool(values.get("enabled", True)),
            prefer_source_metadata=bool(values.get("prefer_source_metadata", True)),
            sample_rows=min(sample_rows, max_sample_rows),
            max_sample_rows=max_sample_rows,
            confidence_threshold=float(values.get("confidence_threshold", 0.98)),
            empty_string_is_null=bool(values.get("empty_string_is_null", False)),
            conflict_policy=conflict_policy,  # type: ignore[arg-type]
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ColumnProfile:
    name: str
    row_count: int
    null_count: int
    empty_string_count: int
    distinct_count: int
    distinct_ratio: float
    observed_types: tuple[str, ...]
    max_length: int | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class InferredColumn:
    name: str
    logical_type: str
    nullable: bool
    confidence: float
    decision_source: str
    reason: str
    precision: int | None = None
    scale: int | None = None
    timezone: bool | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TypeInferenceReport:
    options: TypeInferenceOptions
    columns: Mapping[str, InferredColumn]
    profiles: Mapping[str, ColumnProfile]
    schema_contract: Mapping[str, Any]
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "options": self.options.to_dict(),
            "columns": {name: column.to_dict() for name, column in self.columns.items()},
            "profiles": {name: profile.to_dict() for name, profile in self.profiles.items()},
            "schema_contract": dict(self.schema_contract),
            "warnings": list(self.warnings),
        }
