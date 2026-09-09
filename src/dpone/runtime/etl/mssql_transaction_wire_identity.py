"""Canonical wire-codec projection for MSSQL transaction route identity."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec


def mssql_codec_contract(options: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the route-identity codec projection for PostgreSQL-to-MSSQL loads."""

    if not is_postgres_mssql_wire_route(options):
        return None
    codec = BulkTextCodec()
    return {
        "codec_id": codec.codec_id,
        "codec_version": codec.codec_version,
        "marker_prefix": codec.marker_prefix,
        "empty_string_marker": codec.empty_string_marker,
        "field_terminator": codec.field_terminator,
        "row_terminator": codec.row_terminator,
    }


def is_postgres_mssql_wire_route(options: Mapping[str, Any]) -> bool:
    """Return whether route identity requires the PostgreSQL-to-MSSQL wire codec."""

    return (
        str(options.get("source_type") or "").lower() == "postgres"
        and str(options.get("sink_type") or "").lower() == "mssql"
    )


__all__ = ["is_postgres_mssql_wire_route", "mssql_codec_contract"]
