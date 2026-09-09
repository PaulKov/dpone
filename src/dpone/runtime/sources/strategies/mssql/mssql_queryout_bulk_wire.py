"""Bulk-wire route helpers for MSSQL queryout exports."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def resolve_bulk_wire_contract(load_config: Any, schema: list[tuple[str, str]], sink_connector: Any) -> Any | None:
    """Return the supported ClickHouse bulk-wire contract for this export."""

    if sink_connector is None or sink_connector.__class__.__name__ != "ClickHouseConnector":
        return None
    contract = (
        import_module("dpone.runtime.bulk_wire")
        .BulkWirePlanner()
        .plan(
            source_type="mssql",
            sink_type="clickhouse",
            schema=schema,
            source_options=getattr(load_config, "options", {}) or {},
            sink_options=getattr(load_config, "options", {}) or {},
        )
    )
    return (
        contract
        if contract.selected_route in {"typed_raw_direct", "typed_binary_row_stream", "typed_binary_bcp_native"}
        else None
    )


def bulk_wire_field_terminator(contract: Any | None) -> str | None:
    if contract is None or contract.input_format != "CustomSeparated":
        return None
    return contract.delimiter_profile.field_delimiter


def bulk_wire_row_terminator(contract: Any | None) -> str | None:
    if contract is None or contract.input_format != "CustomSeparated":
        return None
    return contract.delimiter_profile.source_row_terminator or contract.delimiter_profile.row_after_delimiter


__all__ = ["bulk_wire_field_terminator", "bulk_wire_row_terminator", "resolve_bulk_wire_contract"]
