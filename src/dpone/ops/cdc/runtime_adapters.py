"""Shared runtime adapter wiring for CDC ops facades."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.live_adapters import ClickHouseCdcSinkApplier
from dpone.runtime.cdc.local_runtime import LocalCdcSinkApplier
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream
from dpone.runtime.credentials.config import CredentialsSource
from dpone.runtime.credentials.factory import BaseFactory

ConnectorFactory = Callable[[str, CredentialsSource, str | None, str | None], Any]


def build_stream(
    *,
    source: str,
    sink: str,
    backend: str,
    pipeline_name: str,
    source_schema: str,
    source_table: str,
    target_dataset: str,
    unique_key: tuple[str, ...],
) -> CdcRuntimeStream:
    return CdcRuntimeStream(
        pipeline_name=pipeline_name,
        source=source,
        sink=sink,
        backend=CDCBackend(backend),
        source_schema=source_schema,
        source_table=source_table,
        target_dataset=target_dataset,
        unique_key=unique_key,
    )


def build_offset(backend: CDCBackend, token: str | None) -> CDCOffset | None:
    if token is None:
        return None
    return CDCOffset(backend=backend, token=token, snapshot_complete=True)


def create_mssql_connector(
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


def create_clickhouse_connector(
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


def local_sink_applier(output_dir: str | Path) -> LocalCdcSinkApplier:
    return LocalCdcSinkApplier(Path(output_dir) / "sink")


def clickhouse_sink_applier(connector: Any) -> ClickHouseCdcSinkApplier:
    return ClickHouseCdcSinkApplier(connector)


def required_text(value: str | None, flag: str, *, mode: str | None = None) -> str:
    if not value:
        suffix = f" for {mode}" if mode else ""
        raise ValueError(f"{flag} is required{suffix}")
    return value


def required_path(value: str | Path | None, flag: str, *, mode: str) -> str | Path:
    if value is None:
        raise ValueError(f"{flag} is required for {mode}")
    return value


def close_connector(connector: Any) -> None:
    close = getattr(connector, "close", None)
    if callable(close):
        close()


__all__ = [
    "CDCBackend",
    "CDCOffset",
    "ConnectorFactory",
    "CredentialsSource",
    "build_offset",
    "build_stream",
    "clickhouse_sink_applier",
    "close_connector",
    "create_clickhouse_connector",
    "create_mssql_connector",
    "local_sink_applier",
    "required_path",
    "required_text",
]
