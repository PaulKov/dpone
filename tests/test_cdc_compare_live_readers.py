from __future__ import annotations

from dpone.runtime.cdc.compare_readers import ClickHouseCdcLogCompareReader, MssqlCdcCompareReader


class _Connector:
    database = "analytics"

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, object | None]] = []

    def get_records(self, query: str, params: object | None = None, as_dict: bool = False) -> list[dict[str, object]]:
        del as_dict
        self.calls.append((query, params))
        return self.rows


def test_mssql_cdc_compare_reader_reads_ordered_source_rows_with_quoted_columns() -> None:
    connector = _Connector([{"order_id": 1, "status": "paid"}])
    reader = MssqlCdcCompareReader(
        connector=connector,
        source_schema="dbo",
        source_table="orders",
        columns=("order_id", "status"),
        unique_key=("order_id",),
        max_rows=100,
    )

    rows = reader.read_rows()
    query, params = connector.calls[0]

    assert rows[0].payload == {"order_id": 1, "status": "paid"}
    assert "SELECT TOP (?) [order_id], [status]" in query
    assert "FROM [dbo].[orders]" in query
    assert "ORDER BY [order_id]" in query
    assert params == (100,)


def test_clickhouse_cdc_log_compare_reader_reads_latest_cdc_payloads() -> None:
    connector = _Connector(
        [
            {
                "dpone_cdc_payload_json": '{"order_id":1,"status":"paid"}',
                "dpone_cdc_deleted": 0,
                "dpone_cdc_position": "42",
            }
        ]
    )
    reader = ClickHouseCdcLogCompareReader(
        connector=connector,
        cdc_dataset="analytics.orders_cdc",
        stream_id="mssql_to_clickhouse__cdc__dbo_orders__analytics_orders_cdc",
        unique_key=("order_id",),
        max_rows=100,
    )

    rows = reader.read_rows()
    query, _params = connector.calls[0]

    assert rows[0].payload == {"order_id": 1, "status": "paid"}
    assert rows[0].deleted is False
    assert "row_number() OVER" in query
    assert "PARTITION BY dpone_cdc_unique_key_hash" in query
    assert "dpone_cdc_stream_id = 'mssql_to_clickhouse__cdc__dbo_orders__analytics_orders_cdc'" in query
    assert "LIMIT 100" in query
