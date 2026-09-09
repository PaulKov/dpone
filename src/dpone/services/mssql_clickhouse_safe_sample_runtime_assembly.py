"""Assembly helpers for the MSSQL -> ClickHouse certified safe-sample runtime path."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any, Protocol

from dpone.services.mssql_clickhouse_safe_sample_copier import (
    build_mssql_clickhouse_safe_sample_copier_from_pipeline_source,
)
from dpone.services.mssql_clickhouse_safe_sample_execution import (
    ClickHouseSafeSampleWriterFactory,
    CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor,
    MssqlSafeSampleReaderFactory,
    RuntimeCredentialResolver,
)
from dpone.services.safe_sample_data_copier_registry import SafeSampleDataCopierRegistry

MSSQL_CLICKHOUSE_SAFE_SAMPLE_CERTIFICATION_ID = "mssql_clickhouse_incremental_merge_airflow_kpo"


class MssqlSafeSampleSqlClientFactory(Protocol):
    """Build an MSSQL SQL client from resolved runtime credentials."""

    def create(self, credentials: Any) -> Any:
        """Return a client compatible with ``MssqlSafeSampleSqlReader``."""


class ClickHouseSafeSampleSqlClientFactory(Protocol):
    """Build a ClickHouse SQL client from resolved runtime credentials."""

    def create(self, credentials: Any) -> Any:
        """Return a client compatible with ``ClickHouseSafeSampleSqlWriter``."""


class MssqlSafeSampleSqlReaderPortFactory:
    """Adapt an MSSQL SQL-client factory into the safe-sample reader port."""

    def __init__(self, *, client_factory: MssqlSafeSampleSqlClientFactory) -> None:
        self._client_factory = client_factory

    def create(self, credentials: Any) -> Any:
        reader_module = importlib.import_module("dpone.services.mssql_safe_sample_sql_reader")
        return reader_module.MssqlSafeSampleSqlReader(client=self._client_factory.create(credentials))


class ClickHouseSafeSampleSqlWriterPortFactory:
    """Adapt a ClickHouse SQL-client factory into the safe-sample writer port."""

    def __init__(self, *, client_factory: ClickHouseSafeSampleSqlClientFactory) -> None:
        self._client_factory = client_factory

    def create(self, credentials: Any) -> Any:
        writer_module = importlib.import_module("dpone.services.clickhouse_safe_sample_sql_writer")
        return writer_module.ClickHouseSafeSampleSqlWriter(client=self._client_factory.create(credentials))


def build_mssql_clickhouse_safe_sample_registry_from_pipeline_source(
    pipeline_source: Mapping[str, Any],
    *,
    process_name: str | None = None,
    credential_resolver: RuntimeCredentialResolver,
    reader_factory: MssqlSafeSampleReaderFactory,
    writer_factory: ClickHouseSafeSampleWriterFactory,
) -> SafeSampleDataCopierRegistry:
    """Build a registry-backed certified copier from source config and runtime ports.

    The helper intentionally accepts a structural credential resolver and
    structural reader/writer factories. That keeps service-layer assembly free of
    runtime imports while allowing the runtime pod to inject
    ``BindingCredentialResolver`` and concrete connector ports.
    """

    executor = CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor(
        credential_resolver=credential_resolver,
        reader_factory=reader_factory,
        writer_factory=writer_factory,
    )
    copier = build_mssql_clickhouse_safe_sample_copier_from_pipeline_source(
        pipeline_source,
        process_name=process_name,
        executor=executor,
    )
    return SafeSampleDataCopierRegistry.with_copiers({MSSQL_CLICKHOUSE_SAFE_SAMPLE_CERTIFICATION_ID: copier})


def build_mssql_clickhouse_safe_sample_sql_registry_from_pipeline_source(
    pipeline_source: Mapping[str, Any],
    *,
    process_name: str | None = None,
    credential_resolver: RuntimeCredentialResolver,
    mssql_client_factory: MssqlSafeSampleSqlClientFactory,
    clickhouse_client_factory: ClickHouseSafeSampleSqlClientFactory,
) -> SafeSampleDataCopierRegistry:
    """Build a certified copier registry from runtime credential and SQL-client factories."""

    return build_mssql_clickhouse_safe_sample_registry_from_pipeline_source(
        pipeline_source,
        process_name=process_name,
        credential_resolver=credential_resolver,
        reader_factory=MssqlSafeSampleSqlReaderPortFactory(client_factory=mssql_client_factory),
        writer_factory=ClickHouseSafeSampleSqlWriterPortFactory(client_factory=clickhouse_client_factory),
    )


__all__ = [
    "ClickHouseSafeSampleSqlClientFactory",
    "ClickHouseSafeSampleSqlWriterPortFactory",
    "MSSQL_CLICKHOUSE_SAFE_SAMPLE_CERTIFICATION_ID",
    "MssqlSafeSampleSqlClientFactory",
    "MssqlSafeSampleSqlReaderPortFactory",
    "build_mssql_clickhouse_safe_sample_registry_from_pipeline_source",
    "build_mssql_clickhouse_safe_sample_sql_registry_from_pipeline_source",
]
