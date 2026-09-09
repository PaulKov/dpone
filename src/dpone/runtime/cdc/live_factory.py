"""Factories that compose live CDC runtime adapters without route branches."""

from __future__ import annotations

from typing import Any

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.live_adapters import ClickHouseCdcSinkApplier
from dpone.runtime.cdc.mssql import (
    MSSQLCDCReader,
    MSSQLChangeTrackingReader,
    MSSQLChangeTrackingReaderConfig,
    MSSQLTableCDCReaderConfig,
)
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream


class SqlCdcOffsetStoreAdapter:
    """Adapt SQL-backed CDC offset storage to the runtime orchestrator port."""

    def __init__(self, storage: Any) -> None:
        self._storage = storage

    def load_offset(self, stream: CdcRuntimeStream) -> CDCOffset | None:
        return self._storage.load_offset(
            stream.pipeline_name,
            stream.source_schema,
            stream.source_table,
            stream.backend,
        )

    def save_offset(self, stream: CdcRuntimeStream, offset: CDCOffset) -> None:
        self._storage.save_offset(stream.pipeline_name, stream.source_schema, stream.source_table, offset)


class CdcRuntimeLiveAdapterFactory:
    """Build live CDC readers, offset stores, and sink appliers from connectors."""

    def mssql_reader(
        self,
        *,
        connector: Any,
        stream: CdcRuntimeStream,
        capture_instance: str | None = None,
        change_tracking_keys: tuple[str, ...] = tuple(),
    ) -> MSSQLCDCReader | MSSQLChangeTrackingReader:
        if stream.backend == CDCBackend.MSSQL_CDC:
            return MSSQLCDCReader(
                connector,
                MSSQLTableCDCReaderConfig(
                    source_schema=stream.source_schema,
                    source_table=stream.source_table,
                    capture_instance=capture_instance,
                ),
            )
        if stream.backend == CDCBackend.MSSQL_CHANGE_TRACKING:
            return MSSQLChangeTrackingReader(
                connector,
                MSSQLChangeTrackingReaderConfig(
                    source_schema=stream.source_schema,
                    source_table=stream.source_table,
                    primary_keys=change_tracking_keys or stream.unique_key,
                ),
            )
        raise ValueError(f"Unsupported MSSQL live CDC backend: {stream.backend.value}")

    def mssql_offset_store(
        self,
        *,
        connector: Any,
        schema: str = "etl_state",
        table: str = "etl_cdc_offset",
    ) -> SqlCdcOffsetStoreAdapter:
        from dpone.runtime.state.cdc import MSSQLCDCOffsetStorage

        return SqlCdcOffsetStoreAdapter(MSSQLCDCOffsetStorage(connector=connector, schema=schema, table=table))

    def clickhouse_sink_applier(self, *, connector: Any) -> ClickHouseCdcSinkApplier:
        return ClickHouseCdcSinkApplier(connector)


__all__ = [
    "CdcRuntimeLiveAdapterFactory",
    "SqlCdcOffsetStoreAdapter",
]
