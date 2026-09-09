"""MSSQL source export benchmark runners."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from dpone.runtime.export_optimizer_models import ExportProviderProbe
from dpone.runtime.support.mssql_bulk import BcpOptions

Clock = Callable[[], float]


class MssqlBcpProbeRunner:
    """Probe one MSSQL BCP queryout mode with a bounded row count."""

    def __init__(
        self,
        *,
        provider_id: str,
        connector: Any,
        file_format: str,
        packet_sizes: tuple[int, ...],
        clock: Clock = time.monotonic,
    ) -> None:
        self.provider_id = provider_id
        self._connector = connector
        self._file_format = file_format
        self._packet_sizes = packet_sizes
        self._clock = clock

    def probe(self, request: Any) -> ExportProviderProbe:
        results = [self._probe_packet_size(request, packet_size) for packet_size in self._packet_sizes]
        return max(results, key=lambda probe: probe.rows_per_second or 0.0)

    def _probe_packet_size(self, request: Any, packet_size: int) -> ExportProviderProbe:
        request.work_dir.mkdir(parents=True, exist_ok=True)
        output_path = request.work_dir / f"dpone_export_probe_{uuid4().hex}.bcp"
        options = BcpOptions(
            bcp_path=getattr(self._connector, "bcp_path", "bcp"),
            file_format=self._file_format,
            packet_size=packet_size,
            timeout_seconds=request.max_probe_seconds,
            trust_server_certificate=getattr(self._connector, "trust_server_certificate", "no") == "yes",
            encode_text=False,
        )
        started = self._clock()
        try:
            rows = int(self._connector.bcp_queryout(_probe_query(request), str(output_path), options=options) or 0)
            duration = max(self._clock() - started, 0.000001)
            bytes_written = output_path.stat().st_size if output_path.exists() else 0
            return ExportProviderProbe(
                provider_id=self.provider_id,
                rows_per_second=round(rows / duration, 2),
                bytes_per_second=round(bytes_written / duration, 2),
                temp_bytes=bytes_written,
                reasons=(f"bcp_packet_size={packet_size}",),
                provider_version="bcp",
            )
        finally:
            output_path.unlink(missing_ok=True)


class MssqlOdbcArrayProbeRunner:
    """Probe pyodbc array fetch throughput without materializing all rows."""

    provider_id = "mssql_odbc_array"

    def __init__(self, *, connector: Any, fetch_size: int, clock: Clock = time.monotonic) -> None:
        self._connector = connector
        self._fetch_size = max(1, int(fetch_size))
        self._clock = clock

    def probe(self, request: Any) -> ExportProviderProbe:
        started = self._clock()
        first_row_at: float | None = None
        rows = 0
        bytes_read = 0
        for batch in self._connector.get_records_streaming(
            _probe_query(request),
            batch_size=self._fetch_size,
            as_dict=False,
        ):
            if batch and first_row_at is None:
                first_row_at = self._clock()
            rows += len(batch)
            bytes_read += _estimated_batch_bytes(batch)
        duration = max(self._clock() - started, 0.000001)
        return ExportProviderProbe(
            provider_id=self.provider_id,
            rows_per_second=round(rows / duration, 2),
            bytes_per_second=round(bytes_read / duration, 2),
            first_row_latency_ms=round((first_row_at - started) * 1000, 2) if first_row_at is not None else None,
            temp_bytes=0,
            reasons=(f"odbc_fetch_size={self._fetch_size}",),
            provider_version="pyodbc",
        )


def _probe_query(request: Any) -> str:
    return f"SELECT TOP ({request.probe_rows}) * FROM ({request.query}) AS dpone_export_probe"


def _estimated_batch_bytes(batch: list[Any]) -> int:
    return sum(_estimated_row_bytes(row) for row in batch)


def _estimated_row_bytes(row: Any) -> int:
    values = row.values() if isinstance(row, dict) else row
    return sum(0 if value is None else len(str(value).encode("utf-8")) for value in values)


__all__ = ["MssqlBcpProbeRunner", "MssqlOdbcArrayProbeRunner"]
