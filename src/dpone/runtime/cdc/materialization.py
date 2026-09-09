"""ClickHouse CDC log materialization into current-state serving tables."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from dpone.runtime.cdc.materialization_reports import (
    materialization_report_json,
    materialization_report_markdown,
    write_materialization_report,
)

SCHEMA_VERSION = "dpone.cdc_clickhouse_materialization.v1"

DeleteMode = Literal["exclude_deleted", "tombstone"]

_SERVING_COLUMNS: tuple[tuple[str, str], ...] = (
    ("dpone_cdc_stream_id", "String"),
    ("dpone_cdc_route_id", "String"),
    ("dpone_cdc_pipeline_name", "String"),
    ("dpone_cdc_source_schema", "String"),
    ("dpone_cdc_source_table", "String"),
    ("dpone_cdc_operation", "LowCardinality(String)"),
    ("dpone_cdc_position", "String"),
    ("dpone_cdc_sequence", "String"),
    ("dpone_cdc_event_hash", "String"),
    ("dpone_cdc_unique_key_hash", "String"),
    ("dpone_cdc_unique_key_json", "String"),
    ("dpone_cdc_payload_json", "String"),
    ("dpone_cdc_before_json", "String"),
    ("dpone_cdc_deleted", "UInt8"),
    ("dpone_cdc_ingested_at", "DateTime64(3, 'UTC')"),
    ("dpone_cdc_materialized_at", "DateTime64(3, 'UTC')"),
)


@dataclass(frozen=True, slots=True)
class ClickHouseCdcMaterializationPlan:
    """Resolved CDC log and current-state target tables for one materialization."""

    cdc_database: str
    cdc_table: str
    target_database: str
    target_table: str
    unique_key: tuple[str, ...]

    @classmethod
    def from_datasets(
        cls,
        *,
        cdc_dataset: str,
        target_dataset: str,
        unique_key: Sequence[str],
        default_database: str,
    ) -> ClickHouseCdcMaterializationPlan:
        normalized_key = tuple(item for item in unique_key if item)
        if not normalized_key:
            raise ValueError("ClickHouse CDC materialization requires at least one unique key column")
        cdc_database, cdc_table = _split_dataset(cdc_dataset, default_database=default_database)
        target_database, target_table = _split_dataset(target_dataset, default_database=default_database)
        return cls(
            cdc_database=cdc_database,
            cdc_table=cdc_table,
            target_database=target_database,
            target_table=target_table,
            unique_key=normalized_key,
        )

    @property
    def cdc_dataset(self) -> str:
        return f"{self.cdc_database}.{self.cdc_table}"

    @property
    def target_dataset(self) -> str:
        return f"{self.target_database}.{self.target_table}"

    @property
    def qualified_cdc_table(self) -> str:
        return _qualified(self.cdc_database, self.cdc_table)

    @property
    def qualified_target_table(self) -> str:
        return _qualified(self.target_database, self.target_table)

    @property
    def shadow_table(self) -> str:
        return f"{self.target_table}__dpone_materialization_shadow"

    @property
    def old_table(self) -> str:
        return f"{self.target_table}__dpone_materialization_old"

    @property
    def qualified_shadow_table(self) -> str:
        return _qualified(self.target_database, self.shadow_table)

    @property
    def qualified_old_table(self) -> str:
        return _qualified(self.target_database, self.old_table)

    @property
    def artifact_uri(self) -> str:
        return f"clickhouse://{self.target_dataset}"


@dataclass(frozen=True, slots=True)
class ClickHouseCdcMaterializationPolicy:
    """Materialization policy for delete handling and safety checks."""

    delete_mode: DeleteMode = "exclude_deleted"
    strict: bool = True

    def __post_init__(self) -> None:
        if self.delete_mode not in {"exclude_deleted", "tombstone"}:
            raise ValueError("delete_mode must be exclude_deleted or tombstone")


@dataclass(frozen=True, slots=True)
class ClickHouseCdcMaterializationReport:
    """Stable JSON/Markdown contract for one ClickHouse materialization run."""

    plan: ClickHouseCdcMaterializationPlan
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

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "cdc_dataset": self.plan.cdc_dataset,
            "target_dataset": self.plan.target_dataset,
            "unique_key": list(self.plan.unique_key),
            "delete_mode": self.delete_mode,
            "rows_source_events": self.rows_source_events,
            "rows_materialized": self.rows_materialized,
            "rows_deleted": self.rows_deleted,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "artifact_uri": self.plan.artifact_uri,
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return materialization_report_json(self)

    def to_markdown(self) -> str:
        return materialization_report_markdown(self)

    def write(self) -> None:
        write_materialization_report(self)


class ClickHouseCdcMaterializationService:
    """Materialize a normalized ClickHouse CDC log into one current-state table."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def materialize(
        self,
        *,
        plan: ClickHouseCdcMaterializationPlan,
        policy: ClickHouseCdcMaterializationPolicy,
        output_dir: str | Path,
    ) -> ClickHouseCdcMaterializationReport:
        directory = Path(output_dir)
        json_path = directory / "cdc_materialization.json"
        markdown_path = directory / "cdc_materialization.md"
        rows_source_events = 0
        rows_deleted = 0
        rows_materialized = 0
        try:
            rows_source_events = self._count_rows(plan.qualified_cdc_table)
            rows_deleted = self._count_deleted_latest_keys(plan)
            self._replace_shadow(plan=plan, policy=policy)
            rows_materialized = self._count_rows(plan.qualified_shadow_table)
            table_exists = self._target_table_exists(plan)
            self._swap_shadow_into_target(plan, table_exists=table_exists)
            report = self._report(
                plan=plan,
                policy=policy,
                output_dir=directory,
                json_path=json_path,
                markdown_path=markdown_path,
                rows_source_events=rows_source_events,
                rows_materialized=rows_materialized,
                rows_deleted=rows_deleted,
                passed=True,
                blockers=tuple(),
                warnings=tuple(),
                metrics=_success_metrics(plan=plan, policy=policy, table_existed=table_exists),
            )
        except Exception as exc:
            report = self._report(
                plan=plan,
                policy=policy,
                output_dir=directory,
                json_path=json_path,
                markdown_path=markdown_path,
                rows_source_events=rows_source_events,
                rows_materialized=rows_materialized,
                rows_deleted=rows_deleted,
                passed=False,
                blockers=("clickhouse_cdc_materialization.failed",),
                warnings=tuple(),
                metrics={
                    "error": str(exc),
                    "dedupe_key": "dpone_cdc_unique_key_hash",
                    "target_dataset": plan.target_dataset,
                },
            )
        report.write()
        return report

    def _replace_shadow(
        self,
        *,
        plan: ClickHouseCdcMaterializationPlan,
        policy: ClickHouseCdcMaterializationPolicy,
    ) -> None:
        self._connector.execute_query(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(plan.target_database)}")
        self._connector.execute_query(f"DROP TABLE IF EXISTS {plan.qualified_shadow_table}")
        self._connector.execute_query(f"DROP TABLE IF EXISTS {plan.qualified_old_table}")
        self._connector.execute_query(_create_table_sql(plan.qualified_shadow_table))
        self._connector.execute_query(_insert_latest_sql(plan=plan, policy=policy))

    def _swap_shadow_into_target(self, plan: ClickHouseCdcMaterializationPlan, *, table_exists: bool) -> None:
        if table_exists:
            self._connector.execute_query(
                f"RENAME TABLE {plan.qualified_target_table} TO {plan.qualified_old_table}, "
                f"{plan.qualified_shadow_table} TO {plan.qualified_target_table}"
            )
            self._connector.execute_query(f"DROP TABLE IF EXISTS {plan.qualified_old_table}")
            return
        self._connector.execute_query(f"RENAME TABLE {plan.qualified_shadow_table} TO {plan.qualified_target_table}")

    def _target_table_exists(self, plan: ClickHouseCdcMaterializationPlan) -> bool:
        rows = self._connector.get_records(
            "SELECT count() AS exists FROM system.tables "
            f"WHERE database = {_quote_literal(plan.target_database)} AND name = {_quote_literal(plan.target_table)}",
            as_dict=True,
        )
        if not rows:
            return False
        return int(rows[0].get("exists", 0) or 0) > 0

    def _count_rows(self, qualified_table: str) -> int:
        rows = self._connector.get_records(f"SELECT count() AS rows FROM {qualified_table}", as_dict=True)
        if not rows:
            return 0
        return int(rows[0].get("rows", 0) or 0)

    def _count_deleted_latest_keys(self, plan: ClickHouseCdcMaterializationPlan) -> int:
        rows = self._connector.get_records(
            f"""
SELECT count() AS rows
FROM ({_latest_rows_select_sql(plan)})
WHERE rn = 1 AND dpone_cdc_deleted = 1
""".strip(),
            as_dict=True,
        )
        if not rows:
            return 0
        return int(rows[0].get("rows", 0) or 0)

    def _report(
        self,
        *,
        plan: ClickHouseCdcMaterializationPlan,
        policy: ClickHouseCdcMaterializationPolicy,
        output_dir: Path,
        json_path: Path,
        markdown_path: Path,
        rows_source_events: int,
        rows_materialized: int,
        rows_deleted: int,
        passed: bool,
        blockers: tuple[str, ...],
        warnings: tuple[str, ...],
        metrics: Mapping[str, object],
    ) -> ClickHouseCdcMaterializationReport:
        return ClickHouseCdcMaterializationReport(
            plan=plan,
            delete_mode=policy.delete_mode,
            rows_source_events=rows_source_events,
            rows_materialized=rows_materialized,
            rows_deleted=rows_deleted,
            passed=passed,
            blockers=blockers,
            warnings=warnings,
            metrics=metrics,
            output_dir=str(output_dir),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )


def _success_metrics(
    *,
    plan: ClickHouseCdcMaterializationPlan,
    policy: ClickHouseCdcMaterializationPolicy,
    table_existed: bool,
) -> dict[str, object]:
    return {
        "dedupe_key": "dpone_cdc_unique_key_hash",
        "delete_mode": policy.delete_mode,
        "materialization_mode": "shadow_table_replace",
        "ordering": "dpone_cdc_ingested_at,length(dpone_cdc_position),dpone_cdc_position,dpone_cdc_sequence",
        "source_dataset": plan.cdc_dataset,
        "target_dataset": plan.target_dataset,
        "target_table_existed": table_existed,
    }


def _create_table_sql(qualified_table: str) -> str:
    columns = ",\n    ".join(f"{_quote_identifier(name)} {kind}" for name, kind in _SERVING_COLUMNS)
    return f"""
CREATE TABLE {qualified_table} (
    {columns}
)
ENGINE = MergeTree
ORDER BY (`dpone_cdc_unique_key_hash`)
""".strip()


def _insert_latest_sql(
    *,
    plan: ClickHouseCdcMaterializationPlan,
    policy: ClickHouseCdcMaterializationPolicy,
) -> str:
    column_names = [name for name, _ in _SERVING_COLUMNS]
    insert_columns = ", ".join(_quote_identifier(name) for name in column_names)
    select_columns = ",\n    ".join(
        _quote_identifier(name) for name in column_names if name != "dpone_cdc_materialized_at"
    )
    deleted_filter = "" if policy.delete_mode == "tombstone" else " AND dpone_cdc_deleted = 0"
    return f"""
INSERT INTO {plan.qualified_shadow_table} ({insert_columns})
SELECT
    {select_columns},
    now64(3, 'UTC') AS `dpone_cdc_materialized_at`
FROM ({_latest_rows_select_sql(plan)})
WHERE rn = 1{deleted_filter}
""".strip()


def _latest_rows_select_sql(plan: ClickHouseCdcMaterializationPlan) -> str:
    return f"""
SELECT
    *,
    row_number() OVER (
        PARTITION BY dpone_cdc_unique_key_hash
        ORDER BY
            dpone_cdc_ingested_at DESC,
            length(dpone_cdc_position) DESC,
            dpone_cdc_position DESC,
            dpone_cdc_sequence DESC,
            dpone_cdc_event_hash DESC
    ) AS rn
FROM {plan.qualified_cdc_table}
""".strip()


def _split_dataset(value: str, *, default_database: str) -> tuple[str, str]:
    parts = [part.strip() for part in value.split(".") if part.strip()]
    if len(parts) == 1:
        return default_database, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"ClickHouse dataset must be table or database.table: {value!r}")


def _qualified(database: str, table: str) -> str:
    return f"{_quote_identifier(database)}.{_quote_identifier(table)}"


def _quote_identifier(value: str) -> str:
    if not value:
        raise ValueError("ClickHouse identifier cannot be empty")
    return "`" + value.replace("`", "``") + "`"


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


__all__ = [
    "ClickHouseCdcMaterializationPlan",
    "ClickHouseCdcMaterializationPolicy",
    "ClickHouseCdcMaterializationReport",
    "ClickHouseCdcMaterializationService",
]
