"""Connector-neutral bulk-path projection for managed execution plans."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness import managed_native_transfer_plan as native_transfer_plan
from dpone.readiness.managed_native_projection import native_transfer_bulk_wire


def managed_bulk_path(raw: Mapping[str, Any], source: str, sink: str, export_format: str) -> str:
    """Return the stable public bulk-path label for one resolved route."""

    if source == "postgres" and sink == "mssql":
        return "postgres_copy_to_mssql_bcp"
    if source == "mssql" and sink == "clickhouse":
        if native_transfer_plan.requests_object_storage_pull(raw):
            return "mssql_parquet_object_storage_to_clickhouse_pull"
        bulk_wire = native_transfer_bulk_wire(raw, source, sink)
        if (
            str(bulk_wire.get("selected_route")) == "typed_binary_bcp_native"
            and str(bulk_wire.get("input_format")) == "Native"
        ):
            return "mssql_bcp_native_to_clickhouse_native"
        return {
            "typed_raw_direct": "mssql_bcp_queryout_to_clickhouse_typed_wire",
            "typed_binary_bcp_native": "mssql_bcp_native_to_clickhouse_rowbinary",
            "driver": "mssql_driver_to_clickhouse_python",
        }.get(str(bulk_wire.get("selected_route")), "mssql_bcp_queryout_to_clickhouse_direct_tsv")
    if sink == "kafka":
        return "producer_batch"
    if source == "kafka":
        return "bounded_kafka_batch"
    if sink == "mssql":
        return "mssql_bcp_import"
    if sink == "clickhouse":
        return "clickhouse_native_or_http_bulk"
    if export_format == "binary":
        return "native_binary_export"
    return "streaming_rows"


__all__ = ["managed_bulk_path"]
