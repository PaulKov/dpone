"""Ops facades for CDC retention checks and resync execution."""

from __future__ import annotations

from pathlib import Path

from dpone.ops.cdc.retention_adapters import mssql_retention_probe, static_retention_probe
from dpone.ops.cdc.runtime_adapters import (
    ConnectorFactory,
    CredentialsSource,
    build_offset,
    build_stream,
    clickhouse_sink_applier,
    close_connector,
    create_clickhouse_connector,
    create_mssql_connector,
    local_sink_applier,
    required_text,
)
from dpone.runtime.cdc.resync import CdcResyncExecutionService
from dpone.runtime.cdc.retention import CdcResyncPlanner, CdcRetentionGapService, CdcRetentionPolicy
from dpone.runtime.cdc.retention_models import (
    CdcResyncExecutionReport,
    CdcResyncPlanReport,
    CdcRetentionReport,
)


class CdcRetentionCheckOpsService:
    """Compose local or live retention probes for CDC retention reports."""

    def __init__(
        self,
        *,
        retention_service: CdcRetentionGapService | None = None,
        source_connector_factory: ConnectorFactory | None = None,
    ) -> None:
        self._retention_service = retention_service or CdcRetentionGapService(policy=CdcRetentionPolicy())
        self._source_connector_factory = source_connector_factory or create_mssql_connector

    def evaluate(
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
        committed_offset: str | None = None,
        mode: str = "local",
        min_available_offset: str | None = None,
        high_watermark: str | None = None,
        current_offset: str | None = None,
        retention_seconds: int | None = None,
        source_connection_id: str | None = None,
        credentials_source: str = CredentialsSource.ENVIRONMENT.value,
        credentials_mount_point: str | None = None,
        credentials_path: str | None = None,
        capture_instance: str | None = None,
    ) -> CdcRetentionReport:
        stream = build_stream(
            source=source,
            sink=sink,
            backend=backend,
            pipeline_name=pipeline_name,
            source_schema=source_schema,
            source_table=source_table,
            target_dataset=target_dataset,
            unique_key=unique_key,
        )
        offset = build_offset(stream.backend, committed_offset)
        if mode == "local":
            return self._retention_service.evaluate(
                output_dir=output_dir,
                stream=stream,
                committed_offset=offset,
                probe=static_retention_probe(
                    backend=stream.backend,
                    min_available_offset=min_available_offset,
                    high_watermark=high_watermark,
                    current_offset=current_offset,
                    retention_seconds=retention_seconds,
                ),
            )
        if mode == "live":
            if source != "mssql":
                raise ValueError("live CDC retention checks currently support source=mssql")
            connector = self._source_connector_factory(
                required_text(source_connection_id, "--source-connection-id"),
                CredentialsSource(credentials_source),
                credentials_mount_point,
                credentials_path,
            )
            try:
                return self._retention_service.evaluate(
                    output_dir=output_dir,
                    stream=stream,
                    committed_offset=offset,
                    probe=mssql_retention_probe(
                        connector=connector,
                        backend=stream.backend,
                        source_schema=source_schema,
                        source_table=source_table,
                        capture_instance=capture_instance,
                    ),
                )
            finally:
                close_connector(connector)
        raise ValueError(f"Unsupported CDC retention mode: {mode}")


class CdcResyncPlanOpsService:
    """Build resync plans from retention reports and snapshot row artifacts."""

    def __init__(self, *, planner: CdcResyncPlanner | None = None) -> None:
        self._planner = planner or CdcResyncPlanner()

    def plan(
        self,
        *,
        output_dir: str | Path,
        retention_report_json: str | Path,
        rows_json: str | Path,
        source: str,
        sink: str,
        backend: str,
        pipeline_name: str,
        source_schema: str,
        source_table: str,
        target_dataset: str,
        unique_key: tuple[str, ...],
        max_rows: int | None = None,
    ) -> CdcResyncPlanReport:
        return self._planner.plan_from_file(
            output_dir=output_dir,
            stream=build_stream(
                source=source,
                sink=sink,
                backend=backend,
                pipeline_name=pipeline_name,
                source_schema=source_schema,
                source_table=source_table,
                target_dataset=target_dataset,
                unique_key=unique_key,
            ),
            retention_report_json=retention_report_json,
            rows_json=rows_json,
            max_rows=max_rows,
        )


class CdcResyncExecutionOpsService:
    """Compose local or live sink appliers for CDC resync execution."""

    def __init__(self, *, sink_connector_factory: ConnectorFactory | None = None) -> None:
        self._sink_connector_factory = sink_connector_factory or create_clickhouse_connector

    def execute(
        self,
        *,
        output_dir: str | Path,
        resync_plan_json: str | Path,
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
        max_actions: int | None = None,
    ) -> CdcResyncExecutionReport:
        stream = build_stream(
            source=source,
            sink=sink,
            backend=backend,
            pipeline_name=pipeline_name,
            source_schema=source_schema,
            source_table=source_table,
            target_dataset=target_dataset,
            unique_key=unique_key,
        )
        if mode == "local":
            return CdcResyncExecutionService(sink_applier=local_sink_applier(output_dir)).execute(
                output_dir=output_dir,
                resync_plan_json=resync_plan_json,
                stream=stream,
                max_actions=max_actions,
            )
        if mode == "live":
            if sink != "clickhouse":
                raise ValueError("live CDC resync currently supports sink=clickhouse")
            connector = self._sink_connector_factory(
                required_text(sink_connection_id, "--sink-connection-id"),
                CredentialsSource(credentials_source),
                credentials_mount_point,
                credentials_path,
            )
            try:
                return CdcResyncExecutionService(sink_applier=clickhouse_sink_applier(connector)).execute(
                    output_dir=output_dir,
                    resync_plan_json=resync_plan_json,
                    stream=stream,
                    max_actions=max_actions,
                )
            finally:
                close_connector(connector)
        raise ValueError(f"Unsupported CDC resync mode: {mode}")


__all__ = [
    "CdcResyncExecutionOpsService",
    "CdcResyncPlanOpsService",
    "CdcRetentionCheckOpsService",
]
