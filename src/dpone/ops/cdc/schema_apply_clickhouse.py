"""ClickHouse DDL planning for CDC schema apply."""

from __future__ import annotations

import re

from dpone.ops.cdc.schema_apply_models import CdcSchemaApplyPlan, CdcSchemaApplyPolicy
from dpone.ops.cdc.schema_evolution_models import CdcSchemaChangeEvent
from dpone.runtime.cdc.typed_materialization_common import COLUMN_NAME_RE, qualified, quote_identifier, quote_literal


class ClickHouseCdcSchemaDdlPlanner:
    """Plan safe ClickHouse DDL for one captured CDC schema change."""

    def plan(
        self,
        *,
        change: CdcSchemaChangeEvent,
        target_dataset: str,
        policy: CdcSchemaApplyPolicy,
    ) -> CdcSchemaApplyPlan:
        blockers = list(_base_blockers(change=change, target_dataset=target_dataset))
        if change.breaking and policy.require_approval:
            blockers.append("cdc_schema_apply.breaking_change")
        if change.kind != "add_column":
            blockers.append("cdc_schema_apply.unsupported_change")
        target_type = _clickhouse_type(change.new_type, nullable=policy.force_nullable_additions or change.new_nullable)
        if not target_type:
            blockers.append("cdc_schema_apply.unsupported_type")
        ddl = ""
        backfill_sql = ""
        if not blockers:
            database, table = _split_dataset(target_dataset)
            quoted_table = qualified(database, table)
            quoted_column = quote_identifier(change.target_column)
            ddl = f"ALTER TABLE {quoted_table} ADD COLUMN IF NOT EXISTS {quoted_column} {target_type}"
            backfill_sql = _backfill_sql(
                qualified_table=quoted_table,
                column=change.target_column,
                payload_key=change.source_column,
                clickhouse_type=target_type,
            )
        return CdcSchemaApplyPlan(
            change=change,
            target_dataset=target_dataset,
            operation=change.kind,
            ddl=ddl,
            backfill_sql=backfill_sql,
            source_type=change.new_type,
            target_type=target_type,
            passed=not blockers,
            blockers=tuple(dict.fromkeys(blockers)),
        )


def _base_blockers(*, change: CdcSchemaChangeEvent, target_dataset: str) -> tuple[str, ...]:
    blockers: list[str] = []
    if not change.change_id:
        blockers.append("cdc_schema_apply.missing_change_id")
    if not change.target_column or not COLUMN_NAME_RE.match(change.target_column):
        blockers.append("cdc_schema_apply.unsafe_column")
    try:
        _split_dataset(target_dataset)
    except ValueError:
        blockers.append("cdc_schema_apply.invalid_target_dataset")
    return tuple(blockers)


def _split_dataset(value: str) -> tuple[str, str]:
    parts = [part.strip() for part in value.split(".") if part.strip()]
    if len(parts) != 2:
        raise ValueError("CDC schema apply target dataset must be database.table")
    database, table = parts
    if not COLUMN_NAME_RE.match(database) or not COLUMN_NAME_RE.match(table):
        raise ValueError("CDC schema apply target dataset contains unsafe identifiers")
    return database, table


def _clickhouse_type(source_type: str, *, nullable: bool) -> str:
    base = _clickhouse_base_type(source_type)
    if not base:
        return ""
    return f"Nullable({base})" if nullable and not base.startswith("Nullable(") else base


def _clickhouse_base_type(source_type: str) -> str:
    value = source_type.strip().lower()
    decimal = re.match(r"(?:decimal|numeric)\((\d+),\s*(\d+)\)", value)
    if decimal:
        return f"Decimal({decimal.group(1)},{decimal.group(2)})"
    if value.startswith(("nvarchar", "varchar", "nchar", "char", "text", "ntext")):
        return "String"
    if value in {"int", "integer"}:
        return "Int32"
    if value == "bigint":
        return "Int64"
    if value == "smallint":
        return "Int16"
    if value == "tinyint":
        return "UInt8"
    if value in {"bit", "bool", "boolean"}:
        return "Bool"
    if value in {"float", "double", "real"}:
        return "Float64"
    if value == "date":
        return "Date"
    if value.startswith(("datetime", "datetime2", "smalldatetime")):
        return "DateTime64(3, 'UTC')"
    return ""


def _backfill_sql(*, qualified_table: str, column: str, payload_key: str, clickhouse_type: str) -> str:
    expression = _json_expression(clickhouse_type=clickhouse_type, payload_key=payload_key)
    return (
        f"ALTER TABLE {qualified_table} UPDATE {quote_identifier(column)} = {expression} "
        f"WHERE JSONExtractRaw(`dpone_cdc_payload_json`, {quote_literal(payload_key)}) != ''"
    )


def _json_expression(*, clickhouse_type: str, payload_key: str) -> str:
    literal = quote_literal(payload_key)
    if "String" in clickhouse_type:
        return f"JSONExtractString(`dpone_cdc_payload_json`, {literal})"
    if "Bool" in clickhouse_type:
        return f"JSONExtractBool(`dpone_cdc_payload_json`, {literal})"
    if "Decimal" in clickhouse_type:
        scale = re.search(r"Decimal\(\d+,\s*(\d+)\)", clickhouse_type)
        return f"toDecimal128OrNull(JSONExtractString(`dpone_cdc_payload_json`, {literal}), {scale.group(1) if scale else 0})"
    if "Float" in clickhouse_type:
        return f"toFloat64OrNull(JSONExtractRaw(`dpone_cdc_payload_json`, {literal}))"
    if "Int" in clickhouse_type or "UInt" in clickhouse_type:
        target = clickhouse_type.replace("Nullable(", "").rstrip(")")
        return f"to{target}OrNull(JSONExtractRaw(`dpone_cdc_payload_json`, {literal}))"
    if "DateTime" in clickhouse_type:
        return f"parseDateTime64BestEffortOrNull(JSONExtractString(`dpone_cdc_payload_json`, {literal}), 3, 'UTC')"
    if "Date" in clickhouse_type:
        return f"toDateOrNull(JSONExtractString(`dpone_cdc_payload_json`, {literal}))"
    return f"JSONExtractString(`dpone_cdc_payload_json`, {literal})"


__all__ = ["ClickHouseCdcSchemaDdlPlanner"]
