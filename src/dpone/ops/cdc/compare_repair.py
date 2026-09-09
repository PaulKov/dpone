"""Ops facades for CDC compare and repair."""

from __future__ import annotations

from pathlib import Path

from dpone.ops.cdc.compare_adapters import (
    clickhouse_compare_reader,
    local_compare_reader,
    local_target_compare_reader,
    mssql_compare_reader,
)
from dpone.ops.cdc.runtime_adapters import (
    ConnectorFactory,
    CredentialsSource,
    build_stream,
    clickhouse_sink_applier,
    close_connector,
    create_clickhouse_connector,
    create_mssql_connector,
    local_sink_applier,
    required_text,
)
from dpone.runtime.cdc.compare import CdcCompareRepairService
from dpone.runtime.cdc.compare_models import CdcCompareRepairReport
from dpone.runtime.cdc.repair import CdcRepairExecutionReport, CdcRepairExecutionService


class CdcCompareRepairOpsService:
    """Compose local or live compare readers for CDC compare repair reports."""

    def __init__(
        self,
        *,
        compare_service: CdcCompareRepairService | None = None,
        source_connector_factory: ConnectorFactory | None = None,
        sink_connector_factory: ConnectorFactory | None = None,
    ) -> None:
        self._compare_service = compare_service or CdcCompareRepairService()
        self._source_connector_factory = source_connector_factory or create_mssql_connector
        self._sink_connector_factory = sink_connector_factory or create_clickhouse_connector

    def compare(
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
        columns: tuple[str, ...] = tuple(),
        mode: str = "local",
        source_rows_json: str | Path | None = None,
        target_rows_json: str | Path | None = None,
        source_connection_id: str | None = None,
        sink_connection_id: str | None = None,
        credentials_source: str = CredentialsSource.ENVIRONMENT.value,
        credentials_mount_point: str | None = None,
        credentials_path: str | None = None,
        max_rows: int = 10000,
        max_diffs: int = 1000,
    ) -> CdcCompareRepairReport:
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
            return self._compare_service.compare(
                output_dir=output_dir,
                stream=stream,
                source_reader=local_compare_reader(source_rows_json, unique_key=unique_key),
                target_reader=local_target_compare_reader(target_rows_json, unique_key=unique_key),
                max_diffs=max_diffs,
            )
        if mode == "live":
            if source != "mssql" or sink != "clickhouse":
                raise ValueError("live CDC compare currently supports source=mssql and sink=clickhouse")
            source_connector = self._source_connector_factory(
                required_text(
                    source_connection_id,
                    "--source-connection-id",
                    mode="live CDC compare/repair mode",
                ),
                CredentialsSource(credentials_source),
                credentials_mount_point,
                credentials_path,
            )
            sink_connector = self._sink_connector_factory(
                required_text(
                    sink_connection_id,
                    "--sink-connection-id",
                    mode="live CDC compare/repair mode",
                ),
                CredentialsSource(credentials_source),
                credentials_mount_point,
                credentials_path,
            )
            try:
                return self._compare_service.compare(
                    output_dir=output_dir,
                    stream=stream,
                    source_reader=mssql_compare_reader(
                        connector=source_connector,
                        source_schema=source_schema,
                        source_table=source_table,
                        columns=columns,
                        unique_key=unique_key,
                        max_rows=max_rows,
                    ),
                    target_reader=clickhouse_compare_reader(
                        connector=sink_connector,
                        cdc_dataset=target_dataset,
                        stream_id=stream.stream_id,
                        unique_key=unique_key,
                        max_rows=max_rows,
                    ),
                    max_diffs=max_diffs,
                )
            finally:
                close_connector(source_connector)
                close_connector(sink_connector)
        raise ValueError(f"Unsupported CDC compare mode: {mode}")


class CdcRepairExecutionOpsService:
    """Compose local or live sink appliers for CDC repair execution."""

    def __init__(self, *, sink_connector_factory: ConnectorFactory | None = None) -> None:
        self._sink_connector_factory = sink_connector_factory or create_clickhouse_connector

    def execute(
        self,
        *,
        output_dir: str | Path,
        repair_plan_json: str | Path,
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
    ) -> CdcRepairExecutionReport:
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
            return CdcRepairExecutionService(sink_applier=local_sink_applier(output_dir)).execute(
                output_dir=output_dir,
                repair_plan_json=repair_plan_json,
                stream=stream,
                max_actions=max_actions,
            )
        if mode == "live":
            if sink != "clickhouse":
                raise ValueError("live CDC repair currently supports sink=clickhouse")
            connector = self._sink_connector_factory(
                required_text(
                    sink_connection_id,
                    "--sink-connection-id",
                    mode="live CDC compare/repair mode",
                ),
                CredentialsSource(credentials_source),
                credentials_mount_point,
                credentials_path,
            )
            try:
                return CdcRepairExecutionService(sink_applier=clickhouse_sink_applier(connector)).execute(
                    output_dir=output_dir,
                    repair_plan_json=repair_plan_json,
                    stream=stream,
                    max_actions=max_actions,
                )
            finally:
                close_connector(connector)
        raise ValueError(f"Unsupported CDC repair mode: {mode}")


__all__ = ["CdcCompareRepairOpsService", "CdcRepairExecutionOpsService"]
