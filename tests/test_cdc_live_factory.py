from __future__ import annotations

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.live_factory import CdcRuntimeLiveAdapterFactory, SqlCdcOffsetStoreAdapter
from dpone.runtime.cdc.mssql import MSSQLCDCReader, MSSQLChangeTrackingReader
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream


class _Storage:
    def __init__(self) -> None:
        self.saved: list[tuple[str, str, str, CDCOffset]] = []
        self.loaded: list[tuple[str, str, str, CDCBackend]] = []
        self.offset = CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x10", snapshot_complete=True)

    def load_offset(
        self,
        pipeline_name: str,
        source_schema: str,
        source_table: str,
        backend: CDCBackend,
    ) -> CDCOffset | None:
        self.loaded.append((pipeline_name, source_schema, source_table, backend))
        return self.offset

    def save_offset(self, pipeline_name: str, source_schema: str, source_table: str, offset: CDCOffset) -> None:
        self.saved.append((pipeline_name, source_schema, source_table, offset))


def _stream(*, backend: CDCBackend = CDCBackend.MSSQL_CDC) -> CdcRuntimeStream:
    return CdcRuntimeStream(
        pipeline_name="orders-cdc",
        source="mssql",
        sink="clickhouse",
        backend=backend,
        source_schema="dbo",
        source_table="orders",
        target_dataset="analytics.orders_cdc",
        unique_key=("order_id",),
    )


def test_sql_cdc_offset_store_adapter_uses_runtime_stream_identity() -> None:
    storage = _Storage()
    adapter = SqlCdcOffsetStoreAdapter(storage)
    stream = _stream()
    offset = CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x11", snapshot_complete=True)

    assert adapter.load_offset(stream) == storage.offset
    adapter.save_offset(stream, offset)

    assert storage.loaded == [("orders-cdc", "dbo", "orders", CDCBackend.MSSQL_CDC)]
    assert storage.saved == [("orders-cdc", "dbo", "orders", offset)]


def test_live_adapter_factory_builds_mssql_cdc_reader_from_stream() -> None:
    connector = object()
    reader = CdcRuntimeLiveAdapterFactory().mssql_reader(
        connector=connector,
        stream=_stream(),
        capture_instance="dbo_orders_capture",
    )

    assert isinstance(reader, MSSQLCDCReader)
    assert reader.connector is connector
    assert reader.config.source_schema == "dbo"
    assert reader.config.source_table == "orders"
    assert reader.config.resolved_capture_instance == "dbo_orders_capture"


def test_live_adapter_factory_builds_change_tracking_reader_with_unique_key_fallback() -> None:
    connector = object()
    reader = CdcRuntimeLiveAdapterFactory().mssql_reader(
        connector=connector,
        stream=_stream(backend=CDCBackend.MSSQL_CHANGE_TRACKING),
    )

    assert isinstance(reader, MSSQLChangeTrackingReader)
    assert reader.connector is connector
    assert reader.config.primary_keys == ("order_id",)
