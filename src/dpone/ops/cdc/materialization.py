"""Ops facade for ClickHouse CDC serving materialization."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

from dpone.runtime.cdc.materialization import (
    ClickHouseCdcMaterializationPlan,
    ClickHouseCdcMaterializationPolicy,
    ClickHouseCdcMaterializationReport,
    ClickHouseCdcMaterializationService,
    DeleteMode,
)

ConnectorFactory = Callable[[str, Any, str | None, str | None], Any]


class CdcMaterializationService:
    """Compose ClickHouse credentials and runtime materialization for CLI use."""

    def __init__(self, *, sink_connector_factory: ConnectorFactory | None = None) -> None:
        self._sink_connector_factory = sink_connector_factory or _create_clickhouse_connector

    def materialize(
        self,
        *,
        output_dir: str | Path,
        cdc_dataset: str,
        target_dataset: str,
        unique_key: Sequence[str],
        sink_connection_id: str,
        credentials_source: str = "env",
        credentials_mount_point: str | None = None,
        credentials_path: str | None = None,
        delete_mode: str = "exclude_deleted",
    ) -> ClickHouseCdcMaterializationReport:
        connector = self._sink_connector_factory(
            sink_connection_id,
            _credentials_source(credentials_source),
            credentials_mount_point,
            credentials_path,
        )
        try:
            plan = ClickHouseCdcMaterializationPlan.from_datasets(
                cdc_dataset=cdc_dataset,
                target_dataset=target_dataset,
                unique_key=unique_key,
                default_database=str(connector.database),
            )
            policy = ClickHouseCdcMaterializationPolicy(delete_mode=_delete_mode(delete_mode))
            return ClickHouseCdcMaterializationService(connector).materialize(
                plan=plan,
                policy=policy,
                output_dir=output_dir,
            )
        finally:
            _close(connector)


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


def _credentials_source(value: str) -> Any:
    from dpone.runtime.credentials.config import CredentialsSource

    return CredentialsSource(value)
