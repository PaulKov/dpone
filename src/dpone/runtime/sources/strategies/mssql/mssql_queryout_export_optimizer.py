"""MSSQL queryout source export optimizer adapter."""

from __future__ import annotations

from typing import Any

from dpone.runtime.sources.strategies.mssql.mssql_export_optimizer_runtime import resolve_mssql_export_provider
from dpone.runtime.sources.strategies.mssql.mssql_odbc_array_artifacts import build_odbc_array_rowbinary_artifact
from dpone.runtime.sources.strategies.mssql.mssql_queryout_bulk_wire import resolve_bulk_wire_contract


def artifact_from_export_optimizer(
    *,
    connector: Any,
    sink_connector: Any,
    load_config: Any,
    query: str,
    schema: list[tuple[str, str]],
    current_default: str,
) -> Any | None:
    """Return an optimizer-selected artifact, or None when the default path should run."""

    selected = resolve_mssql_export_provider(
        connector=connector,
        load_config=load_config,
        query=query,
        schema=schema,
        current_default=current_default,
    )
    if selected == "mssql_odbc_array" and _is_clickhouse_sink(sink_connector):
        bulk_wire_contract = resolve_bulk_wire_contract(load_config, schema, sink_connector)
        if bulk_wire_contract is None:
            return None
        return build_odbc_array_rowbinary_artifact(
            connector=connector,
            load_config=load_config,
            query=query,
            schema=schema,
            provider_id=selected,
            bulk_wire_contract=bulk_wire_contract,
        )
    return None


def _is_clickhouse_sink(connector: Any) -> bool:
    return connector is not None and connector.__class__.__name__.endswith("ClickHouseConnector")


__all__ = ["artifact_from_export_optimizer"]
