"""ClickHouse connector facade.

This module keeps the public ``ClickHouseConnector`` API stable while delegating
implementation-heavy concerns to smaller runtime services.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.connector_logging import ETLLogger


import os
from collections.abc import Iterable, Iterator
from typing import Any

from dpone.runtime.connector_logging import etl_logger
from dpone.runtime.connectors.base import AbstractConnector
from dpone.runtime.connectors.clickhouse_gcs_export import ClickHouseGCSExportService
from dpone.runtime.connectors.clickhouse_http_client import create_clickhouse_http_client
from dpone.runtime.connectors.clickhouse_incremental_export import ClickHouseIncrementalExportService
from dpone.runtime.connectors.clickhouse_partitioning import ClickHousePartitionService
from dpone.runtime.connectors.clickhouse_query_ops import ClickHouseQueryOps

_GCS_EXPORT_DELEGATES = {
    "_build_export_sql": "_build_export_sql",
}

_PARTITION_DELEGATES = {
    "_parse_date_value": "_parse_date_value",
    "generate_date_partitions": "generate_date_partitions",
    "discover_nonempty_partitions": "discover_nonempty_partitions",
    "count_rows_by_date": "count_rows_by_date",
}

_INCREMENTAL_EXPORT_DELEGATES = {
    "export_partitions": "export_partitions",
    "_determine_partition_type": "_determine_partition_type",
    "_build_select_clause": "_build_select_clause",
    "_build_partition_query": "_build_partition_query",
    "export_incremental_with_cleanup": "export_incremental_with_cleanup",
}

_HTTP_DRIVERS = {"http", "https", "http_tls", "clickhouse_connect", "connect"}


class ClickHouseConnector(AbstractConnector):
    """Connector for ClickHouse based on ``clickhouse-driver``.

    Public API is preserved for backward compatibility. Heavy logic is delegated to
    dedicated helper services to keep this facade small and readable.
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        application_name: str = "dpone-clickhouse",
        secure: bool = False,
        compression: bool = True,
        connect_timeout: int = 10,
        send_receive_timeout: int = 3600,
        settings: dict[str, Any] | None = None,
        driver: str = "native",
        ca_cert: str | None = None,
        gcs_hmac_key: str | None = None,
        gcs_hmac_secret: str | None = None,
        gcs_hmac_vault_path: str | None = None,
        logger: ETLLogger | None = None,
    ):
        self.host = host
        self.driver = _normalize_driver(driver)
        self.port = port or _default_port(driver=self.driver, secure=secure)
        self.database = database or "default"
        self.user = user or "default"
        self.password = password or ""
        self.application_name = application_name
        self.secure = secure
        self.compression = compression
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.ca_cert = ca_cert
        self.gcs_hmac_key = gcs_hmac_key
        self.gcs_hmac_secret = gcs_hmac_secret
        self.gcs_hmac_vault_path = gcs_hmac_vault_path
        self.logger = logger or etl_logger

        default_settings = {
            "output_format_parquet_compression_method": "snappy",
            "allow_experimental_analyzer": 1,
            "max_threads": int(os.getenv("CH_MAX_THREADS", 4)),
            "max_memory_usage": int(os.getenv("CH_MAX_MEMORY", 8 * 1024 * 1024 * 1024)),
            "max_bytes_before_external_sort": int(os.getenv("CH_EXT_SORT", 2 * 1024 * 1024 * 1024)),
            "max_bytes_before_external_group_by": int(os.getenv("CH_EXT_GBY", 2 * 1024 * 1024 * 1024)),
            "optimize_read_in_order": 1,
        }
        self.settings = {**default_settings, **(settings or {})}
        self._client: Any | None = None

        self._query_ops = ClickHouseQueryOps(self)
        self._gcs_export = ClickHouseGCSExportService(self)
        self._partition_service = ClickHousePartitionService(self)
        self._incremental_export = ClickHouseIncrementalExportService(self)

    def __getattr__(self, name: str) -> Any:
        if name in _GCS_EXPORT_DELEGATES:
            return getattr(self._gcs_export, _GCS_EXPORT_DELEGATES[name])
        if name in _PARTITION_DELEGATES:
            return getattr(self._partition_service, _PARTITION_DELEGATES[name])
        if name in _INCREMENTAL_EXPORT_DELEGATES:
            return getattr(self._incremental_export, _INCREMENTAL_EXPORT_DELEGATES[name])
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

    @property
    def connection(self):
        """Lazy creation of the configured ClickHouse client."""
        if self._client is None:
            if self.driver in _HTTP_DRIVERS:
                self._client = create_clickhouse_http_client(
                    host=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password,
                    database=self.database,
                    secure=self.secure,
                    connect_timeout=self.connect_timeout,
                    send_receive_timeout=self.send_receive_timeout,
                    ca_cert=self.ca_cert,
                    settings=self.settings or None,
                )
                return self._client
            from clickhouse_driver import Client

            self._client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                secure=self.secure,
                compression=self.compression,
                connect_timeout=self.connect_timeout,
                send_receive_timeout=self.send_receive_timeout,
                client_name=self.application_name,
                settings=self.settings or None,
            )
        return self._client

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        return self._query_ops.execute_query(query, params)

    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        return self._query_ops.get_records(query, params, as_dict=as_dict)

    def begin(self) -> None:
        pass

    def commit_transaction(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def get_records_iterator(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
    ) -> Iterator[Any]:
        return self._query_ops.get_records_iterator(query, params)

    def import_from_csv(
        self,
        table_schema: str,
        table_name: str,
        csv_file: str,
        delimiter: str = ",",
        batch_rows: int = 100_000,
    ) -> None:
        return self._query_ops.import_from_csv(
            table_schema=table_schema,
            table_name=table_name,
            csv_file=csv_file,
            delimiter=delimiter,
            batch_rows=batch_rows,
        )

    def query_max_column(self, table_name: str, column_name: str) -> Any:
        return self._query_ops.query_max_column(table_name, column_name)

    def get_max_column_value(self, schema: str, table: str, column: str) -> Any | None:
        """Return MAX(column) for watermark incremental extract against ClickHouse targets."""

        database = schema or self.database
        query = f"SELECT max(`{column}`) AS max_val FROM `{database}`.`{table}`"
        rows = self.get_records(query, as_dict=True)
        if not rows:
            return None
        return rows[0].get("max_val")

    def describe_result_columns(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
    ) -> list[str]:
        return self._query_ops.describe_result_columns(query, params)

    def get_records_streaming(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        batch_size: int = 10000,
        as_dict: bool = False,
    ):
        return self._query_ops.get_records_streaming(
            query,
            params,
            batch_size=batch_size,
            as_dict=as_dict,
        )

    def export_to_gcs(
        self,
        query: str,
        gcs_uri: str,
        *,
        format: str = "parquet",
        chunk_rows: int | None = None,
        file_prefix: str = "data",
        extra_settings: dict[str, Any] | None = None,
    ) -> str:
        return self._gcs_export.export_to_gcs(
            query,
            gcs_uri,
            format=format,
            chunk_rows=chunk_rows,
            file_prefix=file_prefix,
            extra_settings=extra_settings,
        )

    def clone_for_partition(self, partition_index: int) -> ClickHouseConnector:
        """Return an isolated connector for one parallel partition worker."""
        return ClickHouseConnector(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            application_name=f"{self.application_name}-partition-{partition_index}",
            secure=self.secure,
            compression=self.compression,
            connect_timeout=self.connect_timeout,
            send_receive_timeout=self.send_receive_timeout,
            settings=dict(self.settings or {}),
            gcs_hmac_key=self.gcs_hmac_key,
            gcs_hmac_secret=self.gcs_hmac_secret,
            gcs_hmac_vault_path=self.gcs_hmac_vault_path,
            driver=self.driver,
            ca_cert=self.ca_cert,
            logger=self.logger,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.disconnect()
            self._client = None

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int | None = None,
        offset: int | None = None,
    ) -> str:
        return self._query_ops.build_select_query(schema, table, columns, limit=limit, offset=offset)


def _normalize_driver(driver: str | None) -> str:
    value = str(driver or "native").strip().lower().replace("-", "_")
    return value or "native"


def _default_port(*, driver: str, secure: bool) -> int:
    if driver in _HTTP_DRIVERS:
        return 8443 if secure else 8123
    return 9440 if secure else 9000
