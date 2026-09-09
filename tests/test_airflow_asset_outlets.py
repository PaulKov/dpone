"""Tests for Airflow pack outlet parsing helpers."""

from __future__ import annotations

import logging

import pytest


def test_outlet_uris_from_pack_ignores_invalid_entries() -> None:
    from dpone_airflow_pack.asset_outlets import outlet_uris_from_pack

    pack = {
        "airflow": {
            "execution": {
                "outlets": [
                    "clickhouse://analytics/orders",
                    42,
                    " ",
                    {"uri": "postgres://analytics/users"},
                    {"uri": "   "},
                    {"provenance": "manual"},
                ]
            }
        }
    }

    assert outlet_uris_from_pack(pack) == (
        "clickhouse://analytics/orders",
        "postgres://analytics/users",
    )


def test_outlet_uris_from_pack_prefers_mapping_uri_field() -> None:
    from dpone_airflow_pack.asset_outlets import outlet_uris_from_pack

    pack = {
        "airflow": {
            "execution": {
                "outlets": [
                    {"uri": "s3://analytics/events", "provenance": {"source": "asset_graph"}},
                ]
            }
        }
    }

    assert outlet_uris_from_pack(pack) == ("s3://analytics/events",)


def test_build_asset_outlets_skips_provider_value_error(caplog: pytest.LogCaptureFixture) -> None:
    from dpone_airflow_pack.asset_outlets import build_asset_outlets

    class _StrictAsset:
        def __init__(self, uri: str) -> None:
            if uri.startswith("mssql://") and uri.count("/") < 5:
                raise ValueError(
                    "URI format mssql:// must contain database, schema, "
                    "and table/view names with optional instance name"
                )
            self.uri = uri

    import dpone_airflow_pack.asset_outlets as asset_outlets

    original = asset_outlets._asset_class
    asset_outlets._asset_class = lambda: _StrictAsset
    try:
        with caplog.at_level(logging.WARNING):
            built = build_asset_outlets(
                (
                    "mssql://assortment_planning/example_forecast",
                    "mssql://sql-prod.internal:1433/dwh_example/assortment_planning/example_forecast",
                )
            )
    finally:
        asset_outlets._asset_class = original

    assert [item.uri for item in built] == [
        "mssql://sql-prod.internal:1433/dwh_example/assortment_planning/example_forecast"
    ]
    assert "DPONE_MSSQL_ASSET_URI_PARSE_REJECTED" in caplog.text
    assert "assortment_planning/example_forecast" not in caplog.text


def test_build_asset_outlets_does_not_swallow_unexpected_errors() -> None:
    from dpone_airflow_pack.asset_outlets import build_asset_outlets

    class _Boom:
        def __init__(self, uri: str) -> None:
            raise RuntimeError("unexpected")

    import dpone_airflow_pack.asset_outlets as asset_outlets

    original = asset_outlets._asset_class
    asset_outlets._asset_class = lambda: _Boom
    try:
        with pytest.raises(RuntimeError, match="unexpected"):
            build_asset_outlets(("mssql://sql-prod.internal:1433/DWH/dbo/orders",))
    finally:
        asset_outlets._asset_class = original
