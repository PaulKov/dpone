"""Quality evidence for ClickHouse CDC typed materialization."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

SchemaDriftMode = Literal["off", "warn_additive", "strict"]


@dataclass(frozen=True, slots=True)
class ClickHouseCdcTypedQualityPolicy:
    """Fail-closed quality policy for typed CDC serving tables."""

    enabled: bool = True
    fail_on_parse_errors: bool = True
    max_parse_error_ratio: float = 0.0
    quarantine_dataset: str | None = None
    schema_drift_mode: SchemaDriftMode = "warn_additive"
    sample_limit: int = 100

    def __post_init__(self) -> None:
        if self.max_parse_error_ratio < 0:
            raise ValueError("max_parse_error_ratio must be non-negative")
        if self.schema_drift_mode not in {"off", "warn_additive", "strict"}:
            raise ValueError("schema_drift_mode must be off, warn_additive, or strict")
        if self.sample_limit < 1:
            raise ValueError("sample_limit must be positive")

    def to_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "fail_on_parse_errors": self.fail_on_parse_errors,
            "max_parse_error_ratio": self.max_parse_error_ratio,
            "quarantine_dataset": self.quarantine_dataset,
            "sample_limit": self.sample_limit,
            "schema_drift_mode": self.schema_drift_mode,
        }


@dataclass(frozen=True, slots=True)
class ClickHouseCdcTypedSchemaDrift:
    """Payload/schema drift detected from sampled latest CDC payloads."""

    additive_payload_keys: tuple[str, ...] = tuple()
    missing_required_keys: tuple[str, ...] = tuple()
    sampled_payload_rows: int = 0

    @property
    def blockers(self) -> tuple[str, ...]:
        if self.missing_required_keys:
            return ("clickhouse_cdc_typed_materialization.schema_drift",)
        return tuple()

    def warnings(self, *, mode: SchemaDriftMode) -> tuple[str, ...]:
        if mode == "warn_additive" and self.additive_payload_keys:
            return ("clickhouse_cdc_typed_materialization.additive_payload_keys",)
        return tuple()

    def strict_blockers(self, *, mode: SchemaDriftMode) -> tuple[str, ...]:
        if mode == "strict" and self.additive_payload_keys:
            return ("clickhouse_cdc_typed_materialization.schema_drift",)
        return tuple()

    def to_dict(self) -> dict[str, object]:
        return {
            "additive_payload_keys": list(self.additive_payload_keys),
            "missing_required_keys": list(self.missing_required_keys),
            "sampled_payload_rows": self.sampled_payload_rows,
        }


@dataclass(frozen=True, slots=True)
class ClickHouseCdcTypedParseFailure:
    """Parse failure count for one projected typed column."""

    column_name: str
    clickhouse_type: str
    failed_rows: int

    def to_dict(self) -> dict[str, object]:
        return {
            "column_name": self.column_name,
            "clickhouse_type": self.clickhouse_type,
            "failed_rows": self.failed_rows,
        }


@dataclass(frozen=True, slots=True)
class ClickHouseCdcTypedParseQuarantine:
    """Parse quarantine evidence for typed projection failures."""

    failures: tuple[ClickHouseCdcTypedParseFailure, ...]
    rows_checked: int
    quarantine_dataset: str | None
    json_path: str
    markdown_path: str

    @property
    def failed_rows(self) -> int:
        return sum(item.failed_rows for item in self.failures)

    @property
    def failed_ratio(self) -> float:
        if self.rows_checked <= 0:
            return 0.0
        return self.failed_rows / self.rows_checked

    def blockers(self, *, policy: ClickHouseCdcTypedQualityPolicy) -> tuple[str, ...]:
        if not self.failures or not policy.fail_on_parse_errors:
            return tuple()
        if self.failed_ratio > policy.max_parse_error_ratio:
            return ("clickhouse_cdc_typed_materialization.parse_quarantine",)
        return tuple()

    def warnings(self) -> tuple[str, ...]:
        if self.failures:
            return ("clickhouse_cdc_typed_materialization.parse_quarantine",)
        return tuple()

    def to_dict(self) -> dict[str, object]:
        return {
            "failed_ratio": self.failed_ratio,
            "failed_rows": self.failed_rows,
            "failures": [item.to_dict() for item in self.failures],
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
            "quarantine_dataset": self.quarantine_dataset,
            "rows_checked": self.rows_checked,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# ClickHouse CDC typed parse quarantine",
            "",
            f"- Failed rows: `{self.failed_rows}`",
            f"- Rows checked: `{self.rows_checked}`",
            f"- Failed ratio: `{self.failed_ratio:.6f}`",
            f"- Quarantine dataset: `{self.quarantine_dataset or 'none'}`",
            "",
            "| column | ClickHouse type | failed rows |",
            "|---|---|---:|",
        ]
        if self.failures:
            lines.extend(
                f"| `{item.column_name}` | `{item.clickhouse_type}` | `{item.failed_rows}` |" for item in self.failures
            )
        else:
            lines.append("| none | none | 0 |")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.json_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


@dataclass(frozen=True, slots=True)
class ClickHouseCdcTypedQualityEvidence:
    """Combined quality evidence embedded into typed materialization reports."""

    policy: ClickHouseCdcTypedQualityPolicy
    schema_drift: ClickHouseCdcTypedSchemaDrift
    parse_quarantine: ClickHouseCdcTypedParseQuarantine
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    metrics: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "blockers": list(self.blockers),
            "metrics": dict(self.metrics),
            "parse_quarantine": self.parse_quarantine.to_dict(),
            "policy": self.policy.to_dict(),
            "schema_drift": self.schema_drift.to_dict(),
            "warnings": list(self.warnings),
        }


__all__ = [
    "ClickHouseCdcTypedParseFailure",
    "ClickHouseCdcTypedParseQuarantine",
    "ClickHouseCdcTypedQualityEvidence",
    "ClickHouseCdcTypedQualityPolicy",
    "ClickHouseCdcTypedSchemaDrift",
    "SchemaDriftMode",
]
