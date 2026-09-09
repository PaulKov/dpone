"""Exact mutable governance-trigger checks for the generic MSSQL catalog."""

from __future__ import annotations

from typing import Any

from dpone.runtime.state.mssql_generic_operation_trigger import canonical_trigger_body


def require_governance_trigger(
    connector: Any,
    *,
    database: str,
    schema: str,
    table: str,
    trigger: str,
    body: str,
    error_label: str,
) -> None:
    """Require one enabled AFTER UPDATE/DELETE trigger with the exact body."""

    prefix = f"{connector.quote_identifier(database)}."
    rows = connector.get_records(
        "SELECT tr.is_disabled, tr.is_instead_of_trigger, sm.definition, te.type_desc AS event_type "
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
    bodies = {canonical_trigger_body(str(row.get("definition") or "")) for row in rows}
    expected = canonical_trigger_body(body)
    valid = bool(
        rows
        and events == {"DELETE", "UPDATE"}
        and all(not bool(row.get("is_disabled")) for row in rows)
        and all(not bool(row.get("is_instead_of_trigger")) for row in rows)
        and bodies == {expected}
    )
    if not valid:
        raise RuntimeError(f"mssql_generic_transaction_{error_label}_trigger:{database}.{schema}.{table}")


__all__ = ["require_governance_trigger"]
