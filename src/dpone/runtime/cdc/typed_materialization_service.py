"""Runtime service for ClickHouse CDC typed materialization."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .typed_materialization_common import qualified, quote_identifier, quote_literal, split_dataset
from .typed_materialization_models import (
    ClickHouseCdcTypedMaterializationPlan,
    ClickHouseCdcTypedMaterializationPolicy,
    ClickHouseCdcTypedMaterializationReport,
)
from .typed_materialization_quality import (
    ClickHouseCdcTypedParseFailure,
    ClickHouseCdcTypedParseQuarantine,
    ClickHouseCdcTypedQualityEvidence,
    ClickHouseCdcTypedSchemaDrift,
)
from .typed_materialization_quarantine_sql import (
    create_parse_quarantine_table_sql,
    insert_parse_quarantine_sql,
    parse_failure_counts_sql,
)
from .typed_materialization_sql import create_table_sql, insert_latest_sql, latest_rows_select_sql


class ClickHouseCdcTypedMaterializationService:
    """Materialize a normalized CDC log into a typed ClickHouse serving table."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def materialize(
        self,
        *,
        plan: ClickHouseCdcTypedMaterializationPlan,
        policy: ClickHouseCdcTypedMaterializationPolicy,
        output_dir: str | Path,
    ) -> ClickHouseCdcTypedMaterializationReport:
        directory = Path(output_dir)
        json_path = directory / "cdc_typed_materialization.json"
        markdown_path = directory / "cdc_typed_materialization.md"
        rows_source_events = 0
        rows_deleted = 0
        rows_materialized = 0
        quality_evidence: ClickHouseCdcTypedQualityEvidence | None = None
        try:
            rows_source_events = self._count_rows(plan.qualified_cdc_table)
            rows_deleted = self._count_deleted_latest_keys(plan)
            quality_evidence = self._evaluate_quality(
                plan=plan,
                policy=policy,
                output_dir=directory,
                rows_checked=rows_source_events,
            )
            if quality_evidence and quality_evidence.blockers:
                report = self._report(
                    plan=plan,
                    policy=policy,
                    output_dir=directory,
                    json_path=json_path,
                    markdown_path=markdown_path,
                    rows_source_events=rows_source_events,
                    rows_materialized=0,
                    rows_deleted=rows_deleted,
                    passed=False,
                    blockers=quality_evidence.blockers,
                    warnings=quality_evidence.warnings,
                    metrics=dict(quality_evidence.metrics),
                    quality_evidence=quality_evidence,
                )
                report.write()
                return report
            self._replace_shadow(plan=plan, policy=policy)
            rows_materialized = self._count_rows(plan.qualified_shadow_table)
            table_exists = self._target_table_exists(plan)
            self._swap_shadow_into_target(plan, table_exists=table_exists)
            warnings = quality_evidence.warnings if quality_evidence else tuple()
            metrics = _success_metrics(plan=plan, policy=policy, table_existed=table_exists)
            if quality_evidence:
                metrics.update(quality_evidence.metrics)
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
                warnings=warnings,
                metrics=metrics,
                quality_evidence=quality_evidence,
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
                blockers=("clickhouse_cdc_typed_materialization.failed",),
                warnings=tuple(),
                metrics={
                    "error": str(exc),
                    "projection_mode": "clickhouse_json_extract",
                    "target_dataset": plan.target_dataset,
                },
                quality_evidence=quality_evidence,
            )
        report.write()
        return report

    def _replace_shadow(
        self,
        *,
        plan: ClickHouseCdcTypedMaterializationPlan,
        policy: ClickHouseCdcTypedMaterializationPolicy,
    ) -> None:
        self._connector.execute_query(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(plan.target_database)}")
        self._connector.execute_query(f"DROP TABLE IF EXISTS {plan.qualified_shadow_table}")
        self._connector.execute_query(f"DROP TABLE IF EXISTS {plan.qualified_old_table}")
        self._connector.execute_query(create_table_sql(plan.qualified_shadow_table, plan.columns))
        self._connector.execute_query(insert_latest_sql(plan=plan, policy=policy))

    def _swap_shadow_into_target(self, plan: ClickHouseCdcTypedMaterializationPlan, *, table_exists: bool) -> None:
        if table_exists:
            self._connector.execute_query(
                f"RENAME TABLE {plan.qualified_target_table} TO {plan.qualified_old_table}, "
                f"{plan.qualified_shadow_table} TO {plan.qualified_target_table}"
            )
            self._connector.execute_query(f"DROP TABLE IF EXISTS {plan.qualified_old_table}")
            return
        self._connector.execute_query(f"RENAME TABLE {plan.qualified_shadow_table} TO {plan.qualified_target_table}")

    def _target_table_exists(self, plan: ClickHouseCdcTypedMaterializationPlan) -> bool:
        rows = self._connector.get_records(
            "SELECT count() AS exists FROM system.tables "
            f"WHERE database = {quote_literal(plan.target_database)} AND name = {quote_literal(plan.target_table)}",
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

    def _count_deleted_latest_keys(self, plan: ClickHouseCdcTypedMaterializationPlan) -> int:
        rows = self._connector.get_records(
            f"""
SELECT count() AS rows
FROM ({latest_rows_select_sql(plan)})
WHERE rn = 1 AND dpone_cdc_deleted = 1
""".strip(),
            as_dict=True,
        )
        if not rows:
            return 0
        return int(rows[0].get("rows", 0) or 0)

    def _evaluate_quality(
        self,
        *,
        plan: ClickHouseCdcTypedMaterializationPlan,
        policy: ClickHouseCdcTypedMaterializationPolicy,
        output_dir: Path,
        rows_checked: int,
    ) -> ClickHouseCdcTypedQualityEvidence | None:
        quality_policy = policy.quality
        if quality_policy is None or not quality_policy.enabled:
            return None
        schema_drift = self._schema_drift(plan=plan, policy=policy)
        parse_quarantine = self._parse_quarantine(
            plan=plan,
            policy=policy,
            output_dir=output_dir,
            rows_checked=rows_checked,
        )
        blockers = (
            *schema_drift.blockers,
            *schema_drift.strict_blockers(mode=quality_policy.schema_drift_mode),
            *parse_quarantine.blockers(policy=quality_policy),
        )
        warnings = (
            *schema_drift.warnings(mode=quality_policy.schema_drift_mode),
            *parse_quarantine.warnings(),
        )
        return ClickHouseCdcTypedQualityEvidence(
            policy=quality_policy,
            schema_drift=schema_drift,
            parse_quarantine=parse_quarantine,
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=tuple(dict.fromkeys(warnings)),
            metrics={
                "parse_failed_rows": parse_quarantine.failed_rows,
                "parse_failed_ratio": parse_quarantine.failed_ratio,
                "schema_additive_key_count": len(schema_drift.additive_payload_keys),
                "schema_missing_required_key_count": len(schema_drift.missing_required_keys),
            },
        )

    def _schema_drift(
        self,
        *,
        plan: ClickHouseCdcTypedMaterializationPlan,
        policy: ClickHouseCdcTypedMaterializationPolicy,
    ) -> ClickHouseCdcTypedSchemaDrift:
        quality_policy = policy.quality
        if quality_policy is None or quality_policy.schema_drift_mode == "off":
            return ClickHouseCdcTypedSchemaDrift()
        rows = self._connector.get_records(
            f"""
SELECT dpone_cdc_payload_json
FROM ({latest_rows_select_sql(plan)})
WHERE rn = 1{_delete_filter(policy)}
LIMIT {quality_policy.sample_limit}
""".strip(),
            as_dict=True,
        )
        key_sets = [_payload_keys(row.get("dpone_cdc_payload_json")) for row in rows]
        observed_keys = set().union(*key_sets) if key_sets else set()
        declared_keys = {column.resolved_payload_key for column in plan.columns}
        required_keys = {column.resolved_payload_key for column in plan.columns if column.required}
        missing_required = sorted(
            key for key in required_keys if any(key not in key_set for key_set in key_sets) or not key_sets
        )
        return ClickHouseCdcTypedSchemaDrift(
            additive_payload_keys=tuple(sorted(observed_keys - declared_keys)),
            missing_required_keys=tuple(missing_required),
            sampled_payload_rows=len(key_sets),
        )

    def _parse_quarantine(
        self,
        *,
        plan: ClickHouseCdcTypedMaterializationPlan,
        policy: ClickHouseCdcTypedMaterializationPolicy,
        output_dir: Path,
        rows_checked: int,
    ) -> ClickHouseCdcTypedParseQuarantine:
        failures = self._parse_failures(plan=plan, policy=policy)
        quality_policy = policy.quality
        quarantine_dataset = quality_policy.quarantine_dataset if quality_policy else None
        json_path = output_dir / "cdc_typed_parse_quarantine.json"
        markdown_path = output_dir / "cdc_typed_parse_quarantine.md"
        quarantine = ClickHouseCdcTypedParseQuarantine(
            failures=failures,
            rows_checked=rows_checked,
            quarantine_dataset=quarantine_dataset,
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        if quarantine_dataset and quarantine.failed_rows:
            qualified_quarantine = _qualified_dataset(quarantine_dataset, default_database=plan.target_database)
            self._connector.execute_query(create_parse_quarantine_table_sql(qualified_quarantine))
            self._connector.execute_query(
                insert_parse_quarantine_sql(
                    plan=plan,
                    policy=policy,
                    qualified_quarantine_table=qualified_quarantine,
                )
            )
        quarantine.write()
        return quarantine

    def _parse_failures(
        self,
        *,
        plan: ClickHouseCdcTypedMaterializationPlan,
        policy: ClickHouseCdcTypedMaterializationPolicy,
    ) -> tuple[ClickHouseCdcTypedParseFailure, ...]:
        rows = self._connector.get_records(parse_failure_counts_sql(plan=plan, policy=policy), as_dict=True)
        failures: list[ClickHouseCdcTypedParseFailure] = []
        for row in rows:
            failed_rows = int(row.get("failed_rows", 0) or 0)
            if failed_rows <= 0:
                continue
            failures.append(
                ClickHouseCdcTypedParseFailure(
                    column_name=str(row.get("column_name", "")),
                    clickhouse_type=str(row.get("clickhouse_type", "")),
                    failed_rows=failed_rows,
                )
            )
        return tuple(failures)

    def _report(
        self,
        *,
        plan: ClickHouseCdcTypedMaterializationPlan,
        policy: ClickHouseCdcTypedMaterializationPolicy,
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
        quality_evidence: ClickHouseCdcTypedQualityEvidence | None = None,
    ) -> ClickHouseCdcTypedMaterializationReport:
        return ClickHouseCdcTypedMaterializationReport(
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
            quality_evidence=quality_evidence,
        )


def _success_metrics(
    *,
    plan: ClickHouseCdcTypedMaterializationPlan,
    policy: ClickHouseCdcTypedMaterializationPolicy,
    table_existed: bool,
) -> dict[str, object]:
    return {
        "delete_mode": policy.delete_mode,
        "materialization_mode": "typed_shadow_table_replace",
        "projection_mode": "clickhouse_json_extract",
        "schema_mode": "shadow_replace",
        "typed_column_count": len(plan.columns),
        "typed_columns": [column.name for column in plan.columns],
        "source_dataset": plan.cdc_dataset,
        "target_dataset": plan.target_dataset,
        "target_table_existed": table_existed,
    }


def _delete_filter(policy: ClickHouseCdcTypedMaterializationPolicy) -> str:
    return "" if policy.delete_mode == "tombstone" else " AND dpone_cdc_deleted = 0"


def _payload_keys(value: object) -> set[str]:
    if not isinstance(value, str) or not value:
        return set()
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return set()
    if not isinstance(payload, dict):
        return set()
    return {str(key) for key in payload}


def _qualified_dataset(value: str, *, default_database: str) -> str:
    database, table = split_dataset(value, default_database=default_database)
    return qualified(database, table)


__all__ = ["ClickHouseCdcTypedMaterializationService"]
