"""Credential-free source schema discovery evidence."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.ops.routes.bootstrap_models import DiscoveredColumn, DiscoveredTable, SourceDiscoveryReport
from dpone.ops.routes.bootstrap_policy import next_actions_for_blockers, status_from_blockers


class SourceDiscoveryService:
    """Build source discovery evidence from exported schema JSON."""

    def discover(
        self,
        *,
        output_dir: str | Path,
        source: str,
        schema_json: str | Path,
        dataset: str = "",
    ) -> SourceDiscoveryReport:
        directory = Path(output_dir)
        payload = _read_payload(Path(schema_json))
        tables = _tables(payload)
        blockers = _blockers(payload=payload, tables=tables)
        warnings = _warnings(tables)
        status = status_from_blockers(blockers, warnings)
        report = SourceDiscoveryReport(
            source=_normalize(source),
            dataset=dataset,
            passed=status != "blocked",
            status=status,
            score=_score(tables=tables, blockers=blockers),
            tables=tables,
            blockers=blockers,
            warnings=warnings,
            next_actions=next_actions_for_blockers(
                blockers,
                ready_action="Use this discovery artifact with route-bootstrap.",
            ),
            output_dir=str(directory),
            json_path=str(directory / "source_discovery.json"),
            markdown_path=str(directory / "source_discovery.md"),
        )
        report.write()
        return report


def _read_payload(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"blockers": ["source_discovery.schema_json_missing"]}
    except json.JSONDecodeError:
        return {"blockers": ["source_discovery.invalid_json"]}
    return value if isinstance(value, Mapping) else {"blockers": ["source_discovery.invalid_shape"]}


def _tables(payload: Mapping[str, Any]) -> tuple[DiscoveredTable, ...]:
    raw_tables = payload.get("tables")
    if isinstance(raw_tables, Mapping):
        raw_values: Sequence[Any] = tuple(raw_tables.values())
    elif isinstance(raw_tables, Sequence) and not isinstance(raw_tables, str | bytes):
        raw_values = raw_tables
    else:
        raw_values = (payload,) if payload.get("columns") else ()
    tables: list[DiscoveredTable] = []
    for raw_table in raw_values:
        if not isinstance(raw_table, Mapping):
            continue
        columns = tuple(_column(raw_column) for raw_column in _raw_columns(raw_table))
        tables.append(
            DiscoveredTable(
                schema=str(raw_table.get("schema") or raw_table.get("table_schema") or ""),
                name=str(raw_table.get("name") or raw_table.get("table") or raw_table.get("table_name") or ""),
                row_count=_optional_int(raw_table.get("row_count") or raw_table.get("estimated_rows")),
                columns=tuple(column for column in columns if column.name),
            )
        )
    return tuple(table for table in tables if table.name)


def _raw_columns(raw_table: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_columns = raw_table.get("columns")
    if isinstance(raw_columns, Mapping):
        return tuple(
            {"name": name, **(value if isinstance(value, Mapping) else {"type": value})}
            for name, value in raw_columns.items()
        )
    if isinstance(raw_columns, Sequence) and not isinstance(raw_columns, str | bytes):
        return tuple(column for column in raw_columns if isinstance(column, Mapping))
    return tuple()


def _column(raw: Mapping[str, Any]) -> DiscoveredColumn:
    name = str(raw.get("name") or raw.get("column") or raw.get("column_name") or "")
    data_type = str(raw.get("type") or raw.get("data_type") or raw.get("source_type") or "unknown")
    nullable = bool(raw.get("nullable", raw.get("is_nullable", True)))
    kind = _kind(data_type)
    risk_level, risk = _risk(data_type=data_type, nullable=nullable)
    return DiscoveredColumn(
        name=name,
        data_type=data_type,
        nullable=nullable,
        kind=kind,
        key_candidate=bool(raw.get("primary_key")) or _key_candidate(name=name, nullable=nullable),
        cursor_candidate=bool(raw.get("cursor")) or _cursor_candidate(name),
        risk_level=risk_level,
        risk=risk,
    )


def _kind(data_type: str) -> str:
    value = data_type.lower()
    if "int" in value or "number" in value or "numeric" in value or "decimal" in value:
        return "integer" if "int" in value and "point" not in value else "decimal"
    if "date" in value or "time" in value:
        return "temporal"
    if "bool" in value or value == "bit":
        return "boolean"
    if "json" in value or "variant" in value or "object" in value:
        return "semi_structured"
    if "char" in value or "text" in value or "string" in value:
        return "string"
    if "binary" in value or "blob" in value:
        return "binary"
    return "unknown"


def _risk(data_type: str, nullable: bool) -> tuple[str, str]:
    value = data_type.lower()
    if value in {"unknown", ""}:
        return "warning", "column type is unknown"
    if "max" in value or "text" in value or "blob" in value:
        return "warning", "unbounded source type needs explicit physical contract"
    if nullable:
        return "ready", "nullable column"
    return "ready", "low risk"


def _key_candidate(*, name: str, nullable: bool) -> bool:
    value = name.lower()
    return not nullable and (value == "id" or value.endswith("_id") or value.endswith("id"))


def _cursor_candidate(name: str) -> bool:
    value = name.lower()
    return value in {"updated_at", "modified_at", "created_at", "xmin", "lsn"} or "version" in value


def _blockers(*, payload: Mapping[str, Any], tables: tuple[DiscoveredTable, ...]) -> tuple[str, ...]:
    blockers = (
        [str(item) for item in payload.get("blockers", ()) if str(item)]
        if isinstance(payload.get("blockers"), list | tuple)
        else []
    )
    if not tables:
        blockers.append("source_discovery.tables_missing")
    for table in tables:
        if not table.columns:
            blockers.append(f"table.{table.qualified_name}.columns_missing")
    return tuple(dict.fromkeys(blockers))


def _warnings(tables: tuple[DiscoveredTable, ...]) -> tuple[str, ...]:
    warnings: list[str] = []
    for table in tables:
        if not table.primary_key_candidates:
            warnings.append(f"table.{table.qualified_name}.primary_key_candidate_missing")
        if not table.cursor_candidates:
            warnings.append(f"table.{table.qualified_name}.cursor_candidate_missing")
        warnings.extend(
            f"column.{table.qualified_name}.{column.name}.{column.risk}"
            for column in table.columns
            if column.risk_level == "warning"
        )
    return tuple(dict.fromkeys(warnings))


def _score(*, tables: tuple[DiscoveredTable, ...], blockers: tuple[str, ...]) -> float:
    if blockers:
        return 0.0
    columns = [column for table in tables for column in table.columns]
    if not columns:
        return 0.0
    safe = sum(1 for column in columns if column.risk_level == "ready")
    return round((safe / len(columns)) * 100.0, 2)


def _optional_int(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _normalize(value: str) -> str:
    return value.strip().lower().replace("-", "_")
