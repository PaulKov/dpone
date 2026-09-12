"""Shared MSSQL Asset URI codec: named instance + closed canonical form."""

from __future__ import annotations

import pytest
from dpone_airflow_pack.mssql_asset_uri_codec import (
    canonicalize_mssql_asset_uri,
    canonicalize_mssql_asset_uri_parts,
    parse_mssql_asset_uri,
    require_canonical_mssql_asset_uri,
)


def test_three_and_four_segment_shapes_round_trip() -> None:
    three = "mssql://fixture01.invalid:1433/DWH/dbo/orders"
    four = "mssql://fixture01.invalid:1433/prod01/DWH/dbo/orders"
    assert require_canonical_mssql_asset_uri(three) == three
    assert require_canonical_mssql_asset_uri(four) == four
    parsed_four = parse_mssql_asset_uri(four)
    assert parsed_four.instance == "prod01"
    assert canonicalize_mssql_asset_uri(four) == four


def test_named_instance_is_lowercased_in_canonical_form() -> None:
    uri = canonicalize_mssql_asset_uri_parts(
        host="FIXTure01.Invalid",
        port=1433,
        instance="PROD01",
        database="DWH",
        schema="dbo",
        table="orders",
    )
    assert uri == "mssql://fixture01.invalid:1433/prod01/DWH/dbo/orders"


def test_closed_form_requires_explicit_port_and_rejects_userinfo() -> None:
    with pytest.raises(ValueError, match="explicit port"):
        parse_mssql_asset_uri("mssql://fixture01.invalid/DWH/dbo/orders", require_explicit_port=True)
    with pytest.raises(ValueError, match="credentials"):
        parse_mssql_asset_uri("mssql://user:pass@fixture01.invalid:1433/DWH/dbo/orders")
    with pytest.raises(ValueError, match="query or fragment"):
        parse_mssql_asset_uri("mssql://fixture01.invalid:1433/DWH/dbo/orders?x=1")


def test_canonicalize_parse_equality_is_required_for_closed_uri() -> None:
    with pytest.raises(ValueError, match="closed canonical"):
        require_canonical_mssql_asset_uri("mssql://FIXTure01.Invalid:1433/DWH/dbo/orders")
