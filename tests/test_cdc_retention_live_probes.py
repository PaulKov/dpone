from __future__ import annotations

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc.retention_probes import MssqlCdcRetentionProbe, MssqlChangeTrackingRetentionProbe


class _Connector:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def get_records(
        self,
        query: str,
        params: tuple[object, ...] | None = None,
        *,
        as_dict: bool = False,
    ) -> list[dict[str, object]]:
        assert as_dict is True
        self.calls.append((query, params or tuple()))
        return self.rows


def test_mssql_change_tracking_probe_reads_min_valid_and_current_versions() -> None:
    connector = _Connector([{"min_valid_version": 10, "current_version": 42}])

    bounds = MssqlChangeTrackingRetentionProbe(
        connector=connector,
        source_schema="dbo",
        source_table="orders",
    ).read_bounds()

    query, params = connector.calls[0]
    assert bounds.backend == CDCBackend.MSSQL_CHANGE_TRACKING
    assert bounds.min_available_offset == "10"
    assert bounds.high_watermark == "42"
    assert "CHANGE_TRACKING_MIN_VALID_VERSION" in query
    assert "CHANGE_TRACKING_CURRENT_VERSION" in query
    assert params == ("[dbo].[orders]",)


def test_mssql_cdc_probe_reads_min_and_max_lsn() -> None:
    connector = _Connector([{"min_lsn": "0x0000000000000000000A", "max_lsn": "0x0000000000000000000F"}])

    bounds = MssqlCdcRetentionProbe(connector=connector, capture_instance="dbo_orders").read_bounds()

    query, params = connector.calls[0]
    assert bounds.backend == CDCBackend.MSSQL_CDC
    assert bounds.min_available_offset == "0x0000000000000000000A"
    assert bounds.high_watermark == "0x0000000000000000000F"
    assert "sys.fn_cdc_get_min_lsn" in query
    assert "sys.fn_cdc_get_max_lsn" in query
    assert params == ("dbo_orders",)
