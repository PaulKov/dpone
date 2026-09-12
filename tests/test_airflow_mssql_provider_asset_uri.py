"""Real microsoft-mssql provider cell for AIP-60 Asset URI acceptance.

Installed on Airflow pack-compat matrix cells with
``apache-airflow-providers-microsoft-mssql==4.7.0`` (Airflow >= 2.11 only).

Airflow 2.x emits a UserWarning for non-AIP-60 URIs; Airflow 3.x raises
``ValueError``. Both are fail-closed signals for dpone build/check.
"""

from __future__ import annotations

import importlib.util
import warnings

import pytest


def _mssql_provider_available() -> bool:
    try:
        return importlib.util.find_spec("airflow.providers.microsoft.mssql") is not None
    except ModuleNotFoundError:
        return False


pytestmark = pytest.mark.skipif(
    not _mssql_provider_available(),
    reason="apache-airflow-providers-microsoft-mssql not installed in this environment",
)


def _asset(uri: str) -> object:
    """Build Asset (AF3) or Dataset (AF2). Do not swallow provider URI errors."""

    try:
        from airflow.sdk import Asset
    except ImportError:
        Asset = None  # type: ignore[misc, assignment]
    if Asset is not None:
        return Asset(uri)
    from airflow.datasets import Dataset

    return Dataset(uri)


def _asset_uri(asset: object) -> str:
    for attr in ("uri", "name"):
        value = getattr(asset, attr, None)
        if isinstance(value, str) and value:
            return value
    raise AssertionError(f"Asset/Dataset has no uri/name: {asset!r}")


@pytest.mark.parametrize(
    "uri",
    [
        "mssql://fixture01.invalid:1433/dwh_example/dbo/orders",
        "mssql://fixture01.invalid:1444/dwh_example/dbo/orders",
        "mssql://fixture01.invalid:1433/MSSQL01/dwh_example/dbo/orders",
        "mssql://fixture01.invalid:1433/dwh_example/assortment%20planning/supply%2Fforecast",
    ],
)
def test_microsoft_mssql_provider_accepts_canonical_uris(uri: str) -> None:
    assert _asset_uri(_asset(uri)) == uri


@pytest.mark.parametrize(
    "uri",
    [
        "mssql://dbo/orders",
        "mssql://dwh_example/dbo/orders",
    ],
)
def test_microsoft_mssql_provider_flags_compact_uris(uri: str) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            _asset(uri)
        except ValueError as exc:
            assert "mssql://" in str(exc).lower() or "aip-60" in str(exc).lower() or "database" in str(exc).lower()
            return
    assert any("AIP-60" in str(item.message) or "mssql://" in str(item.message) for item in caught), caught


def test_producer_consumer_equality_on_canonical_form() -> None:
    producer = "mssql://fixture01.invalid:1433/dwh_example/dbo/orders"
    consumer = "mssql://fixture01.invalid:1433/dwh_example/dbo/orders"
    assert _asset_uri(_asset(producer)) == _asset_uri(_asset(consumer))
