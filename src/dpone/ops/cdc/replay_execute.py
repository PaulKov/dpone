"""Ops facade for CDC replay execution."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc.live_adapters import ClickHouseCdcSinkApplier
from dpone.runtime.cdc.local_runtime import LocalCdcSinkApplier
from dpone.runtime.cdc.replay_execution import CdcReplayExecutionReport, CdcReplayExecutionService
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream
from dpone.runtime.credentials.config import CredentialsSource
from dpone.runtime.credentials.factory import BaseFactory

ConnectorFactory = Callable[[str, CredentialsSource, str | None, str | None], Any]


class CdcReplayExecutionOpsService:
    """Compose local or live sink appliers for quarantine replay."""

    def __init__(self, *, sink_connector_factory: ConnectorFactory | None = None) -> None:
        self._sink_connector_factory = sink_connector_factory or _create_clickhouse_connector

    def execute(
        self,
        *,
        output_dir: str | Path,
        quarantine_json: str | Path,
        source: str,
        sink: str,
        backend: str,
        pipeline_name: str,
        source_schema: str,
        source_table: str,
        target_dataset: str,
        unique_key: tuple[str, ...],
        mode: str = "local",
        sink_connection_id: str | None = None,
        credentials_source: str = CredentialsSource.ENVIRONMENT.value,
        credentials_mount_point: str | None = None,
        credentials_path: str | None = None,
        max_events: int | None = None,
    ) -> CdcReplayExecutionReport:
        stream = CdcRuntimeStream(
            pipeline_name=pipeline_name,
            source=source,
            sink=sink,
            backend=CDCBackend(backend),
            source_schema=source_schema,
            source_table=source_table,
            target_dataset=target_dataset,
            unique_key=unique_key,
        )
        if mode == "local":
            return CdcReplayExecutionService(sink_applier=LocalCdcSinkApplier(Path(output_dir) / "sink")).execute(
                output_dir=output_dir, quarantine_json=quarantine_json, stream=stream, max_events=max_events
            )
        if mode == "live":
            if sink != "clickhouse":
                raise ValueError("live CDC replay currently supports sink=clickhouse")
            connector = self._sink_connector_factory(
                _required_text(sink_connection_id, "--sink-connection-id"),
                CredentialsSource(credentials_source),
                credentials_mount_point,
                credentials_path,
            )
            try:
                return CdcReplayExecutionService(sink_applier=ClickHouseCdcSinkApplier(connector)).execute(
                    output_dir=output_dir,
                    quarantine_json=quarantine_json,
                    stream=stream,
                    max_events=max_events,
                )
            finally:
                _close(connector)
        raise ValueError(f"Unsupported CDC replay mode: {mode}")


def _create_clickhouse_connector(
    connection_id: str,
    credentials_source: CredentialsSource,
    mount_point: str | None,
    path: str | None,
) -> Any:
    return BaseFactory._create_clickhouse_connector(
        connection_id,
        credentials_source,
        mount_point=mount_point,
        path=path,
    )


def _required_text(value: str | None, flag: str) -> str:
    if not value:
        raise ValueError(f"{flag} is required for live CDC replay mode")
    return value


def _close(connector: Any) -> None:
    close = getattr(connector, "close", None)
    if callable(close):
        close()


__all__ = ["CdcReplayExecutionOpsService"]
