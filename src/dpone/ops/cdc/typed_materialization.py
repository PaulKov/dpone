"""Ops facade for typed ClickHouse CDC serving materialization."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

from dpone.runtime.cdc.typed_materialization import (
    ClickHouseCdcTypedColumn,
    ClickHouseCdcTypedMaterializationPlan,
    ClickHouseCdcTypedMaterializationPolicy,
    ClickHouseCdcTypedMaterializationReport,
    ClickHouseCdcTypedMaterializationService,
    ClickHouseCdcTypedQualityPolicy,
    DeleteMode,
)
from dpone.runtime.cdc.typed_materialization_quality import SchemaDriftMode

ConnectorFactory = Callable[[str, Any, str | None, str | None], Any]


class CdcTypedMaterializationService:
    """Compose ClickHouse credentials and typed materialization for CLI use."""

    def __init__(self, *, sink_connector_factory: ConnectorFactory | None = None) -> None:
        self._sink_connector_factory = sink_connector_factory or _create_clickhouse_connector

    def materialize(
        self,
        *,
        output_dir: str | Path,
        cdc_dataset: str,
        target_dataset: str,
        unique_key: Sequence[str],
        columns: Sequence[str | ClickHouseCdcTypedColumn],
        sink_connection_id: str,
        credentials_source: str = "env",
        credentials_mount_point: str | None = None,
        credentials_path: str | None = None,
        delete_mode: str = "exclude_deleted",
        fail_on_parse_errors: bool = False,
        max_parse_error_ratio: float = 0.0,
        quarantine_dataset: str | None = None,
        schema_drift_mode: str = "warn_additive",
        quality_sample_limit: int = 100,
    ) -> ClickHouseCdcTypedMaterializationReport:
        connector = self._sink_connector_factory(
            sink_connection_id,
            _credentials_source(credentials_source),
            credentials_mount_point,
            credentials_path,
        )
        try:
            plan = ClickHouseCdcTypedMaterializationPlan.from_datasets(
                cdc_dataset=cdc_dataset,
                target_dataset=target_dataset,
                unique_key=unique_key,
                columns=_columns(columns),
                default_database=str(connector.database),
            )
            policy = ClickHouseCdcTypedMaterializationPolicy(
                delete_mode=_delete_mode(delete_mode),
                quality=ClickHouseCdcTypedQualityPolicy(
                    fail_on_parse_errors=fail_on_parse_errors,
                    max_parse_error_ratio=max_parse_error_ratio,
                    quarantine_dataset=quarantine_dataset,
                    schema_drift_mode=_schema_drift_mode(schema_drift_mode),
                    sample_limit=quality_sample_limit,
                ),
            )
            return ClickHouseCdcTypedMaterializationService(connector).materialize(
                plan=plan,
                policy=policy,
                output_dir=output_dir,
            )
        finally:
            _close(connector)


def _columns(values: Sequence[str | ClickHouseCdcTypedColumn]) -> tuple[ClickHouseCdcTypedColumn, ...]:
    return tuple(
        value if isinstance(value, ClickHouseCdcTypedColumn) else ClickHouseCdcTypedColumn.from_cli(value)
        for value in values
    )


def _create_clickhouse_connector(
    connection_id: str,
    credentials_source: Any,
    mount_point: str | None,
    path: str | None,
) -> Any:
    from dpone.runtime.credentials.factory import BaseFactory

    return BaseFactory._create_clickhouse_connector(
        connection_id,
        credentials_source,
        mount_point=cast(str, mount_point),
        path=cast(str, path),
    )


def _close(connector: Any) -> None:
    close = getattr(connector, "close", None)
    if callable(close):
        close()


def _delete_mode(value: str) -> DeleteMode:
    if value not in {"exclude_deleted", "tombstone"}:
        raise ValueError("delete_mode must be exclude_deleted or tombstone")
    return cast(DeleteMode, value)


def _schema_drift_mode(value: str) -> SchemaDriftMode:
    if value not in {"off", "warn_additive", "strict"}:
        raise ValueError("schema_drift_mode must be off, warn_additive, or strict")
    return cast(SchemaDriftMode, value)


def _credentials_source(value: str) -> Any:
    from dpone.runtime.credentials.config import CredentialsSource

    return CredentialsSource(value)
