"""ClickHouse access enforcement dialect."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any


class ClickHouseAccessDialect:
    """Renders ClickHouse column grants, row policies and masked views."""

    sink_type = "clickhouse"

    def render_operations(
        self,
        *,
        target: Mapping[str, Any],
        requirements: Sequence[Mapping[str, Any]],
        options: Any,
        target_connection: Mapping[str, Any],
    ) -> tuple[tuple[dict[str, Any], ...], tuple[str, ...], tuple[str, ...]]:
        del target_connection
        blockers = _target_blockers(target)
        if blockers:
            return (), tuple(blockers), ()
        operations: list[dict[str, Any]] = []
        operations.extend(_grant_operations(target, requirements, options))
        operations.extend(_row_policy_operations(target, requirements, options))
        operations.extend(_mask_operations(target, requirements, options))
        return tuple(operations), (), ()


def _grant_operations(target: Mapping[str, Any], requirements: Sequence[Mapping[str, Any]], options: Any) -> list[dict]:
    by_subject: dict[str, set[str]] = {}
    for item in requirements:
        if item.get("type") == "column_grant":
            by_subject.setdefault(str(item.get("subject")), set()).add(str(item.get("column")))
    operations: list[dict] = []
    for subject, columns in sorted(by_subject.items()):
        role = _role(subject, options)
        columns_sql = ", ".join(_quote(column) for column in sorted(columns))
        operations.append(
            _operation(
                name=f"grant_select_{_slug(subject)}",
                sql=f"GRANT SELECT({columns_sql}) ON {_table(target)} TO {_quote(role)}",
                subject=subject,
                requirement_type="column_grant",
            )
        )
    return operations


def _row_policy_operations(
    target: Mapping[str, Any], requirements: Sequence[Mapping[str, Any]], options: Any
) -> list[dict]:
    operations: list[dict] = []
    for item in requirements:
        if item.get("type") != "row_filter":
            continue
        subject = str(item.get("subject"))
        name = f"{_role(subject, options)}_{_short_hash(item.get('row_filter'))}"
        sql = (
            f"CREATE ROW POLICY IF NOT EXISTS {_quote(name)} ON {_table(target)} "
            f"FOR SELECT USING {item.get('row_filter')} TO {_quote(_role(subject, options))}"
        )
        operations.append(
            _operation(
                name=f"row_policy_{_slug(subject)}_{_slug(str(item.get('column')))}",
                sql=sql,
                subject=subject,
                column=str(item.get("column")),
                requirement_type="row_filter",
            )
        )
    return operations


def _mask_operations(target: Mapping[str, Any], requirements: Sequence[Mapping[str, Any]], options: Any) -> list[dict]:
    by_subject: dict[str, list[Mapping[str, Any]]] = {}
    grant_columns: dict[str, set[str]] = {}
    for item in requirements:
        subject = str(item.get("subject"))
        if item.get("type") == "mask":
            by_subject.setdefault(subject, []).append(item)
        if item.get("type") == "column_grant":
            grant_columns.setdefault(subject, set()).add(str(item.get("column")))
    operations: list[dict] = []
    for subject, masks in sorted(by_subject.items()):
        columns = sorted(grant_columns.get(subject, {str(item.get("column")) for item in masks}))
        select_sql = ", ".join(_projection(column, masks) for column in columns)
        view = _masked_view_name(target, subject, options)
        sql = f"CREATE OR REPLACE VIEW {view} AS SELECT {select_sql} FROM {_table(target)}"
        operations.append(
            _operation(
                name=f"masked_view_{_slug(subject)}",
                sql=sql,
                subject=subject,
                requirement_type="mask",
            )
        )
    return operations


def _projection(column: str, masks: Sequence[Mapping[str, Any]]) -> str:
    mask = next((item for item in masks if item.get("column") == column), None)
    quoted = _quote(column)
    if not mask:
        return quoted
    if str(mask.get("masking")) == "hash":
        return f"hex(SHA256(toString({quoted}))) AS {quoted}"
    return f"NULL AS {quoted}"


def _operation(
    *,
    name: str,
    sql: str,
    subject: str,
    requirement_type: str,
    column: str | None = None,
) -> dict[str, Any]:
    payload = {
        "name": name,
        "operation_type": "sql",
        "requirement_type": requirement_type,
        "subject": subject,
        "sql": sql,
    }
    if column:
        payload["column"] = column
    return payload


def _target_blockers(target: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if not target.get("database"):
        blockers.append("data_product_access_enforcement.clickhouse_database_required")
    if not target.get("table"):
        blockers.append("data_product_access_enforcement.clickhouse_table_required")
    return blockers


def _masked_view_name(target: Mapping[str, Any], subject: str, options: Any) -> str:
    template = str(options.clickhouse.get("masked_view_naming") or "{database}.{table}__masked__{subject_hash}")
    raw = template.format(
        database=target.get("database"),
        table=target.get("table"),
        subject_hash=_short_hash(subject),
    )
    if "." in raw:
        database, table = raw.rsplit(".", 1)
        return f"{_quote(database)}.{_quote(table)}"
    return _quote(raw)


def _table(target: Mapping[str, Any]) -> str:
    return f"{_quote(str(target.get('database')))}.{_quote(str(target.get('table')))}"


def _role(subject: str, options: Any) -> str:
    return f"{options.clickhouse.get('role_prefix', 'dpone_')}{_slug(subject)}"


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _slug(raw: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", raw).strip("_").lower() or "subject"


def _short_hash(raw: object) -> str:
    return hashlib.sha256(str(raw).encode("utf-8")).hexdigest()[:12]


__all__ = ["ClickHouseAccessDialect"]
