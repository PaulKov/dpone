"""Exact immutable-trigger verification shared by MSSQL safety catalogs."""

from __future__ import annotations

import re
from typing import Any

from dpone.runtime.support.mssql_identifier_integrity import require_case_unambiguous_identifiers


def require_exact_table_trigger_set(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    table: str,
    triggers: frozenset[str],
    error_code: str,
) -> None:
    """Reject missing, extra, or case-ambiguous DML triggers on one table."""

    prefix = f"{connector.quote_identifier(database)}." if database else ""
    rows = connector.get_records(
        "SELECT tr.name AS trigger_name "
        f"FROM {prefix}sys.triggers AS tr "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = tr.parent_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ?",
        (schema, table),
        as_dict=True,
    )
    actual = frozenset(
        require_case_unambiguous_identifiers(
            (row.get("trigger_name") for row in rows),
            error_code=f"{error_code}_identifier_case_ambiguity:{database}.{schema}.{table}",
        )
    )
    if actual != triggers:
        raise RuntimeError(f"{error_code}:{database}.{schema}.{table}")


def require_exact_immutable_trigger(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    table: str,
    trigger: str,
    throw_token: str,
    error_code: str,
) -> None:
    """Require one enabled INSTEAD OF UPDATE/DELETE single-THROW body."""

    prefix = f"{connector.quote_identifier(database)}." if database else ""
    rows = connector.get_records(
        "SELECT tr.name AS trigger_name, tr.is_disabled, tr.is_instead_of_trigger, "
        "tr.is_not_for_replication, "
        "sm.definition AS trigger_definition, te.type_desc AS event_type "
        f"FROM {prefix}sys.triggers AS tr "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = tr.parent_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        f"INNER JOIN {prefix}sys.sql_modules AS sm ON sm.object_id = tr.object_id "
        f"INNER JOIN {prefix}sys.trigger_events AS te ON te.object_id = tr.object_id "
        "WHERE s.name = ? AND t.name = ? AND tr.name = ?",
        (schema, table, trigger),
        as_dict=True,
    )
    events = {str(row.get("event_type") or "").upper() for row in rows}
    expected_body = f"THROW51000,'{throw_token}',1"
    valid = bool(
        rows
        and events == {"DELETE", "UPDATE"}
        and all(not bool(row.get("is_disabled")) for row in rows)
        and all(not bool(row.get("is_not_for_replication")) for row in rows)
        and all(bool(row.get("is_instead_of_trigger")) for row in rows)
        and all(_canonical_trigger_body(str(row.get("trigger_definition") or "")) == expected_body for row in rows)
    )
    if not valid:
        raise RuntimeError(f"{error_code}:{database}.{schema}.{table}")


def _canonical_trigger_body(definition: str) -> str:
    uncommented = _without_sql_comments(definition)
    matches = tuple(re.finditer(r"\bAS\b", uncommented, flags=re.IGNORECASE))
    body = uncommented[matches[-1].end() :] if matches else uncommented
    return re.sub(r"\s+", "", body).rstrip(";").upper()


def _without_sql_comments(value: str) -> str:
    output: list[str] = []
    index = 0
    state = "plain"
    block_depth = 0
    while index < len(value):
        current = value[index]
        following = value[index + 1] if index + 1 < len(value) else ""
        if state == "line_comment":
            if current in "\r\n":
                state = "plain"
                output.append(current)
            index += 1
            continue
        if state == "block_comment":
            if current == "/" and following == "*":
                block_depth += 1
                index += 2
            elif current == "*" and following == "/":
                block_depth -= 1
                index += 2
                if block_depth == 0:
                    state = "plain"
            else:
                index += 1
            continue
        if state in {"single_quote", "double_quote", "bracket"}:
            output.append(current)
            closing = {"single_quote": "'", "double_quote": '"', "bracket": "]"}[state]
            if current == closing and following == closing:
                output.append(following)
                index += 2
                continue
            if current == closing:
                state = "plain"
            index += 1
            continue
        if current == "-" and following == "-":
            state = "line_comment"
            index += 2
        elif current == "/" and following == "*":
            state = "block_comment"
            block_depth = 1
            index += 2
        else:
            output.append(current)
            if current == "'":
                state = "single_quote"
            elif current == '"':
                state = "double_quote"
            elif current == "[":
                state = "bracket"
            index += 1
    return "".join(output)


__all__ = ["require_exact_immutable_trigger", "require_exact_table_trigger_set"]
