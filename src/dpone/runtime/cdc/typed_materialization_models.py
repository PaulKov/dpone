"""Models for ClickHouse CDC typed materialization."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from .typed_materialization_common import (
    COLUMN_NAME_RE,
    SCHEMA_VERSION,
    SUPPORTED_TYPE_RE,
    DeleteMode,
    qualified,
    quote_identifier,
    split_dataset,
)
from .typed_materialization_quality import ClickHouseCdcTypedQualityEvidence, ClickHouseCdcTypedQualityPolicy


@dataclass(frozen=True, slots=True)
class ClickHouseCdcTypedColumn:
    """One typed serving column projected from a CDC payload JSON key."""

    name: str
    clickhouse_type: str
    payload_key: str | None = None
    required: bool = False

    def __post_init__(self) -> None:
        if not COLUMN_NAME_RE.match(self.name):
            raise ValueError(f"Unsafe ClickHouse column name: {self.name!r}")
        if not SUPPORTED_TYPE_RE.match(self.clickhouse_type):
            raise ValueError(f"Unsupported ClickHouse type: {self.clickhouse_type!r}")
        if self.payload_key is None:
            object.__setattr__(self, "payload_key", self.name)

    @classmethod
    def from_cli(cls, value: str) -> ClickHouseCdcTypedColumn:
        if "=" not in value:
            raise ValueError("--column must use name=Type")
        name, raw_type = value.split("=", 1)
        return cls(name=name.strip(), clickhouse_type=raw_type.strip())

    @property
    def resolved_payload_key(self) -> str:
        return self.payload_key or self.name

    @property
    def ddl_fragment(self) -> str:
        return f"{quote_identifier(self.name)} {self.clickhouse_type}"

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "clickhouse_type": self.clickhouse_type,
            "payload_key": self.resolved_payload_key,
            "required": self.required,
        }


@dataclass(frozen=True, slots=True)
class ClickHouseCdcTypedMaterializationPlan:
    """Resolved CDC log and typed target serving table."""

    cdc_database: str
    cdc_table: str
    target_database: str
    target_table: str
    unique_key: tuple[str, ...]
    columns: tuple[ClickHouseCdcTypedColumn, ...]

    @classmethod
    def from_datasets(
        cls,
        *,
        cdc_dataset: str,
        target_dataset: str,
        unique_key: Sequence[str],
        columns: Sequence[ClickHouseCdcTypedColumn],
        default_database: str,
    ) -> ClickHouseCdcTypedMaterializationPlan:
        normalized_key = tuple(item for item in unique_key if item)
        if not normalized_key:
            raise ValueError("Typed CDC materialization requires at least one unique key column")
        normalized_columns = tuple(columns)
        if not normalized_columns:
            raise ValueError("Typed CDC materialization requires at least one projected column")
        column_names = [column.name for column in normalized_columns]
        if len(column_names) != len(set(column_names)):
            raise ValueError("Typed CDC materialization column names must be unique")
        cdc_database, cdc_table = split_dataset(cdc_dataset, default_database=default_database)
        target_database, target_table = split_dataset(target_dataset, default_database=default_database)
        return cls(
            cdc_database=cdc_database,
            cdc_table=cdc_table,
            target_database=target_database,
            target_table=target_table,
            unique_key=normalized_key,
            columns=normalized_columns,
        )

    @property
    def cdc_dataset(self) -> str:
        return f"{self.cdc_database}.{self.cdc_table}"

    @property
    def target_dataset(self) -> str:
        return f"{self.target_database}.{self.target_table}"

    @property
    def qualified_cdc_table(self) -> str:
        return qualified(self.cdc_database, self.cdc_table)

    @property
    def qualified_target_table(self) -> str:
        return qualified(self.target_database, self.target_table)

    @property
    def shadow_table(self) -> str:
        return f"{self.target_table}__dpone_typed_materialization_shadow"

    @property
    def old_table(self) -> str:
        return f"{self.target_table}__dpone_typed_materialization_old"

    @property
    def qualified_shadow_table(self) -> str:
        return qualified(self.target_database, self.shadow_table)

    @property
    def qualified_old_table(self) -> str:
        return qualified(self.target_database, self.old_table)

    @property
    def artifact_uri(self) -> str:
        return f"clickhouse://{self.target_dataset}"


@dataclass(frozen=True, slots=True)
class ClickHouseCdcTypedMaterializationPolicy:
    """Typed materialization policy for delete handling and DDL behavior."""

    delete_mode: DeleteMode = "exclude_deleted"
    strict: bool = True
    quality: ClickHouseCdcTypedQualityPolicy | None = None

    def __post_init__(self) -> None:
        if self.delete_mode not in {"exclude_deleted", "tombstone"}:
            raise ValueError("delete_mode must be exclude_deleted or tombstone")


@dataclass(frozen=True, slots=True)
class ClickHouseCdcTypedMaterializationReport:
    """Stable JSON/Markdown evidence for typed CDC materialization."""

    plan: ClickHouseCdcTypedMaterializationPlan
    delete_mode: str
    rows_source_events: int
    rows_materialized: int
    rows_deleted: int
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    metrics: Mapping[str, object]
    output_dir: str
    json_path: str
    markdown_path: str
    quality_evidence: ClickHouseCdcTypedQualityEvidence | None = None

    @property
    def typed_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.plan.columns)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "cdc_dataset": self.plan.cdc_dataset,
            "target_dataset": self.plan.target_dataset,
            "unique_key": list(self.plan.unique_key),
            "delete_mode": self.delete_mode,
            "typed_columns": [column.to_dict() for column in self.plan.columns],
            "schema_policy": {
                "mode": "shadow_replace",
                "ddl_strategy": "create_shadow_and_rename",
                "strict": True,
            },
            "rows_source_events": self.rows_source_events,
            "rows_materialized": self.rows_materialized,
            "rows_deleted": self.rows_deleted,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "quality_evidence": self.quality_evidence.to_dict() if self.quality_evidence else None,
            "artifact_uri": self.plan.artifact_uri,
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# ClickHouse CDC typed materialization",
            "",
            f"- CDC log: `{self.plan.cdc_dataset}`",
            f"- Target: `{self.plan.target_dataset}`",
            f"- Unique key: `{', '.join(self.plan.unique_key)}`",
            f"- Delete mode: `{self.delete_mode}`",
            f"- Passed: `{self.passed}`",
            f"- Source events: `{self.rows_source_events}`",
            f"- Rows materialized: `{self.rows_materialized}`",
            f"- Deleted latest keys: `{self.rows_deleted}`",
            "",
            "## Typed columns",
            "",
            "| column | ClickHouse type | payload key | required |",
            "|---|---|---|---:|",
        ]
        for column in self.plan.columns:
            lines.append(
                f"| `{column.name}` | `{column.clickhouse_type}` | `{column.resolved_payload_key}` | "
                f"`{column.required}` |"
            )
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.extend(["", "## Metrics", ""])
        lines.extend(f"- `{name}`: `{value}`" for name, value in self.metrics.items())
        if self.quality_evidence:
            lines.extend(["", "## Quality evidence", ""])
            if self.quality_evidence.blockers:
                lines.extend(f"- blocker `{item}`" for item in self.quality_evidence.blockers)
            else:
                lines.append("- blockers: none")
            if self.quality_evidence.warnings:
                lines.extend(f"- warning `{item}`" for item in self.quality_evidence.warnings)
            else:
                lines.append("- warnings: none")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


__all__ = [
    "ClickHouseCdcTypedColumn",
    "ClickHouseCdcTypedMaterializationPlan",
    "ClickHouseCdcTypedMaterializationPolicy",
    "ClickHouseCdcTypedMaterializationReport",
]
