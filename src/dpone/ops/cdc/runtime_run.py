"""Ops facade for credential-free CDC runtime runs."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc.live_factory import CdcRuntimeLiveAdapterFactory
from dpone.runtime.cdc.local_runtime import FileCdcOffsetStore, JsonCdcBatchReader, LocalCdcSinkApplier
from dpone.runtime.cdc.poison_models import PoisonMode
from dpone.runtime.cdc.runtime_models import CdcRuntimePolicy, CdcRuntimeRunReport, CdcRuntimeStream
from dpone.runtime.cdc.runtime_orchestrator import CdcRuntimeOrchestrator
from dpone.runtime.credentials.config import CredentialsSource
from dpone.runtime.credentials.factory import BaseFactory

ConnectorFactory = Callable[[str, CredentialsSource, str | None, str | None], Any]


class CdcRuntimeRunService:
    """Compose local or live CDC runtime adapters for CLI and CI execution."""

    def __init__(
        self,
        *,
        orchestrator: CdcRuntimeOrchestrator | None = None,
        live_factory: CdcRuntimeLiveAdapterFactory | None = None,
        source_connector_factory: ConnectorFactory | None = None,
        sink_connector_factory: ConnectorFactory | None = None,
    ) -> None:
        self._orchestrator = orchestrator or CdcRuntimeOrchestrator()
        self._live_factory = live_factory or CdcRuntimeLiveAdapterFactory()
        self._source_connector_factory = source_connector_factory or _create_mssql_connector
        self._sink_connector_factory = sink_connector_factory or _create_clickhouse_connector

    def run(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        backend: str,
        pipeline_name: str,
        source_schema: str,
        source_table: str,
        target_dataset: str,
        unique_key: tuple[str, ...],
        mode: str = "local",
        events_json: str | Path | None = None,
        checkpoint_json: str | Path | None = None,
        source_connection_id: str | None = None,
        sink_connection_id: str | None = None,
        credentials_source: str = CredentialsSource.ENVIRONMENT.value,
        credentials_mount_point: str | None = None,
        credentials_path: str | None = None,
        state_schema: str = "etl_state",
        state_table: str = "etl_cdc_offset",
        capture_instance: str | None = None,
        change_tracking_keys: tuple[str, ...] = tuple(),
        max_changes: int = 10000,
        poison_mode: str = "fail_closed",
    ) -> CdcRuntimeRunReport:
        cdc_backend = CDCBackend(backend)
        stream = CdcRuntimeStream(
            pipeline_name=pipeline_name,
            source=source,
            sink=sink,
            backend=cdc_backend,
            source_schema=source_schema,
            source_table=source_table,
            target_dataset=target_dataset,
            unique_key=unique_key,
        )
        if mode == "local":
            return self._run_local(
                output_dir=output_dir,
                stream=stream,
                events_json=_required_path(events_json, "--events-json"),
                checkpoint_json=_required_path(checkpoint_json, "--checkpoint-json"),
                max_changes=max_changes,
                poison_mode=poison_mode,
            )
        if mode == "live":
            return self._run_live(
                output_dir=output_dir,
                stream=stream,
                source_connection_id=_required_text(source_connection_id, "--source-connection-id"),
                sink_connection_id=_required_text(sink_connection_id, "--sink-connection-id"),
                credentials_source=CredentialsSource(credentials_source),
                credentials_mount_point=credentials_mount_point,
                credentials_path=credentials_path,
                state_schema=state_schema,
                state_table=state_table,
                capture_instance=capture_instance,
                change_tracking_keys=change_tracking_keys,
                max_changes=max_changes,
                poison_mode=poison_mode,
            )
        raise ValueError(f"Unsupported CDC runtime mode: {mode}")

    def _run_local(
        self,
        *,
        output_dir: str | Path,
        stream: CdcRuntimeStream,
        events_json: str | Path,
        checkpoint_json: str | Path,
        max_changes: int,
        poison_mode: str,
    ) -> CdcRuntimeRunReport:
        directory = Path(output_dir)
        return self._orchestrator.run_once(
            stream=stream,
            reader=JsonCdcBatchReader(
                path=events_json,
                backend=stream.backend,
                source_schema=stream.source_schema,
                source_table=stream.source_table,
            ),
            offset_store=FileCdcOffsetStore(checkpoint_json),
            sink_applier=LocalCdcSinkApplier(directory / "sink"),
            output_dir=directory,
            policy=CdcRuntimePolicy(max_changes=max_changes, poison_mode=_poison_mode(poison_mode)),
        )

    def _run_live(
        self,
        *,
        output_dir: str | Path,
        stream: CdcRuntimeStream,
        source_connection_id: str,
        sink_connection_id: str,
        credentials_source: CredentialsSource,
        credentials_mount_point: str | None,
        credentials_path: str | None,
        state_schema: str,
        state_table: str,
        capture_instance: str | None,
        change_tracking_keys: tuple[str, ...],
        max_changes: int,
        poison_mode: str,
    ) -> CdcRuntimeRunReport:
        if stream.source != "mssql" or stream.sink != "clickhouse":
            raise ValueError("live CDC runtime currently supports source=mssql and sink=clickhouse")
        source_connector = self._source_connector_factory(
            source_connection_id,
            credentials_source,
            credentials_mount_point,
            credentials_path,
        )
        sink_connector = self._sink_connector_factory(
            sink_connection_id,
            credentials_source,
            credentials_mount_point,
            credentials_path,
        )
        try:
            return self._orchestrator.run_once(
                stream=stream,
                reader=self._live_factory.mssql_reader(
                    connector=source_connector,
                    stream=stream,
                    capture_instance=capture_instance,
                    change_tracking_keys=change_tracking_keys,
                ),
                offset_store=self._live_factory.mssql_offset_store(
                    connector=source_connector,
                    schema=state_schema,
                    table=state_table,
                ),
                sink_applier=self._live_factory.clickhouse_sink_applier(connector=sink_connector),
                output_dir=Path(output_dir),
                policy=CdcRuntimePolicy(max_changes=max_changes, poison_mode=_poison_mode(poison_mode)),
            )
        finally:
            _close(source_connector)
            _close(sink_connector)


def _create_mssql_connector(
    connection_id: str,
    credentials_source: CredentialsSource,
    mount_point: str | None,
    path: str | None,
) -> Any:
    return BaseFactory._create_mssql_connector(
        connection_id,
        credentials_source,
        autocommit=True,
        mount_point=mount_point,
        path=path,
    )


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


def _required_path(value: str | Path | None, flag: str) -> str | Path:
    if value is None:
        raise ValueError(f"{flag} is required for local CDC runtime mode")
    return value


def _poison_mode(value: str) -> PoisonMode:
    if value not in {"fail_closed", "quarantine_and_continue"}:
        raise ValueError("poison_mode must be fail_closed or quarantine_and_continue")
    return cast(PoisonMode, value)


def _required_text(value: str | None, flag: str) -> str:
    if not value:
        raise ValueError(f"{flag} is required for live CDC runtime mode")
    return value


def _close(connector: Any) -> None:
    close = getattr(connector, "close", None)
    if callable(close):
        close()
