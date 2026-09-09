"""Entry-level projection parser: relation equality + schema bounds."""

from __future__ import annotations

from pathlib import Path

import pytest
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.mssql_asset_ref_codec import require_asset_ref_sha256
from dpone_airflow_pack.mssql_asset_uri_codec import quote_mssql_path_segment
from dpone_airflow_pack.mssql_outlet_projection_codes import PROJECTION_INVALID, PROJECTION_MISMATCH
from dpone_airflow_pack.mssql_outlet_projection_contract import parse_mssql_asset_outlet_projection
from dpone_airflow_pack.mssql_outlet_projection_entries import (
    MAX_PROJECTION_ENTRIES,
    MAX_WORKLOAD_IDS_PER_ENTRY,
    parse_projection_entry,
)

_ASSET_REF = {
    "engine": "mssql",
    "connection_ref": "mssql_marts",
    "database": "DWH",
    "schema": "dbo",
    "table": "orders",
}


def _entry(*, uri: str = "mssql://sql-prod.internal:1433/DWH/dbo/orders", **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "asset_ref": dict(_ASSET_REF),
        "asset_ref_sha256": require_asset_ref_sha256(_ASSET_REF),
        "registry_connection_ref": "mssql_writer",
        "resolved_binding": {"registry_connection_ref": "mssql_writer"},
        "workload_ids": ["demo"],
        "uri": uri,
    }
    body.update(overrides)
    return body


def test_parse_projection_entry_accepts_matching_relation_from_authority_host() -> None:
    digest, uri = parse_projection_entry(
        _entry(uri="mssql://sql-build-plane.internal:1444/inst01/DWH/dbo/orders"),
        index=0,
        path=None,
    )
    assert digest == require_asset_ref_sha256(_ASSET_REF)
    assert uri == "mssql://sql-build-plane.internal:1444/inst01/DWH/dbo/orders"


@pytest.mark.parametrize(
    ("uri", "code"),
    [
        ("mssql://sql-prod.internal:1433/OTHER/dbo/orders", PROJECTION_MISMATCH),
        ("mssql://sql-prod.internal:1433/DWH/other/orders", PROJECTION_MISMATCH),
        ("mssql://sql-prod.internal:1433/DWH/dbo/other", PROJECTION_MISMATCH),
    ],
)
def test_parse_projection_entry_relation_mismatch(uri: str, code: str) -> None:
    with pytest.raises(InitFetchProviderError) as exc:
        parse_projection_entry(_entry(uri=uri), index=0, path=Path("projection.json"))
    assert exc.value.code == code
    assert "relation mismatches" in str(exc.value)


def test_parse_projection_entry_noncanonical_percent_encoding_is_invalid() -> None:
    # Lowercase hex is not the closed canonical form (quote emits uppercase).
    assert quote_mssql_path_segment("ord/ers") == "ord%2Fers"
    uri = "mssql://sql-prod.internal:1433/DWH/dbo/ord%2fers"
    with pytest.raises(InitFetchProviderError) as exc:
        parse_projection_entry(_entry(uri=uri), index=0, path=None)
    assert exc.value.code == PROJECTION_INVALID


def test_parse_entries_enforces_schema_max_items() -> None:
    entry = _entry()
    projection = {
        "schema": "dpone.mssql-asset-outlet-projection.v1",
        "environment": "prod",
        "binding_set_ref": "sha256:" + "b" * 64,
        "connection_registry_ref": "sha256:" + "c" * 64,
        "entries": [entry] * (MAX_PROJECTION_ENTRIES + 1),
        "projection_sha256": "sha256:" + "d" * 64,
    }
    with pytest.raises(InitFetchProviderError) as exc:
        parse_mssql_asset_outlet_projection(projection)
    assert exc.value.code == PROJECTION_INVALID
    assert str(MAX_PROJECTION_ENTRIES) in str(exc.value)


def test_parse_projection_entry_enforces_workload_ids_max_items() -> None:
    with pytest.raises(InitFetchProviderError) as exc:
        parse_projection_entry(
            _entry(workload_ids=[f"w{i}" for i in range(MAX_WORKLOAD_IDS_PER_ENTRY + 1)]),
            index=0,
            path=None,
        )
    assert exc.value.code == PROJECTION_INVALID
    assert str(MAX_WORKLOAD_IDS_PER_ENTRY) in str(exc.value)
