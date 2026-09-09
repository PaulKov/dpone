"""BigQuery backend methods shared by the public state factory."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.runtime.state.factory_bigquery import create_bigquery_run_state_storage, create_bigquery_xmin_state_storage

if TYPE_CHECKING:  # pragma: no cover
    from dpone.runtime.state.run_state import RunStateStorage
    from dpone.runtime.state.xmin_storage import XMinStateStorage


class BigQueryStateFactoryMixin:
    """Construct BigQuery connectors and state stores through an inheriting factory."""

    @classmethod
    def create_bigquery_connector(
        cls,
        connection_id: str | None,
        credentials_source: str = "airflow",
        mount_point: str | None = None,
        path: str | None = None,
        proxy_enable: bool = False,
        proxy_mount_point: str | None = None,
        proxy_path: str = "network/proxy/gcp/current",
    ):
        """Создает общий BigQueryConnector.

        Args:
            connection_id: ID подключения для получения SA креденшиалов
            credentials_source: источник креденшиалов ('airflow' или 'vault')
            mount_point: Vault mount point для SA
            path: Vault path для SA
            proxy_enable: включить proxy для всех BQ подключений
            proxy_mount_point: Vault mount point для прокси креденшиалов
            proxy_path: Vault path для прокси креденшиалов
        """

        from dpone.runtime.credentials.config import CredentialsSource, require_connection_id
        from dpone.runtime.credentials.factory import BaseFactory

        return BaseFactory._create_bigquery_connector(
            connection_id=require_connection_id(connection_id, backend="BigQuery"),
            credentials_source=CredentialsSource(credentials_source),
            mount_point=mount_point,
            path=path,
            proxy_enable=proxy_enable,
            proxy_mount_point=proxy_mount_point,
            proxy_path=proxy_path,
        )

    @classmethod
    def create_bigquery_kafka_offset_state_storage(
        cls,
        connection_id: str | None = None,
        bigquery_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "etl_kafka_offsets",
        schema: str = "etl_state",
        mount_point: str | None = None,
        path: str | None = None,
        proxy_enable: bool = False,
    ):
        from dpone.runtime.state.kafka import BigQueryKafkaOffsetStateStorage

        connector = bigquery_connector or cls.create_bigquery_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
            proxy_enable=proxy_enable,
        )
        return BigQueryKafkaOffsetStateStorage(connector=connector, dataset=schema, table=state_table)

    @classmethod
    def create_bigquery_load_audit_storage(
        cls,
        connection_id: str | None = None,
        bigquery_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "__dpone__loads",
        schema: str = "etl_state",
        mount_point: str | None = None,
        path: str | None = None,
        proxy_enable: bool = False,
    ):
        """Creates BigQuery-backed canonical load audit storage."""

        from dpone.runtime.state.load_audit import BigQueryLoadAuditStorage

        connector = bigquery_connector or cls.create_bigquery_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
            proxy_enable=proxy_enable,
        )
        return BigQueryLoadAuditStorage(connector=connector, dataset=schema, table=state_table)

    @classmethod
    def create_run_state_storage(
        cls,
        connection_id: str | None = None,
        bigquery_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "etl_run_state",
        schema: str = "etl_state",
        mount_point: str | None = None,
        path: str | None = None,
        proxy_enable: bool = False,
    ) -> RunStateStorage:
        """Создает хранилище состояния выполнения ETL процессов.

        Параметры:
            connection_id: ID подключения Airflow (используется если bigquery_connector = None)
            bigquery_connector: готовый BigQueryConnector (переиспользуемый)
            credentials_source: источник креденшиалов
            state_table: имя таблицы состояния
            schema: датасет BigQuery
            mount_point: Vault mount point
            path: Vault path
            proxy_enable: включить proxy
        """

        return create_bigquery_run_state_storage(
            bigquery_connector=bigquery_connector,
            connector_factory=cls.create_bigquery_connector,
            connection_id=connection_id,
            credentials_source=credentials_source,
            state_table=state_table,
            schema=schema,
            mount_point=mount_point,
            path=path,
            proxy_enable=proxy_enable,
        )

    @classmethod
    def create_xmin_state_storage(
        cls,
        connection_id: str | None = None,
        bigquery_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "etl_xmin_state",
        schema: str = "etl_state",
        mount_point: str | None = None,
        path: str | None = None,
        proxy_enable: bool = False,
    ) -> XMinStateStorage:
        """Создает хранилище состояния xmin.

        Параметры:
            connection_id: ID подключения Airflow (используется если bigquery_connector = None)
            bigquery_connector: готовый BigQueryConnector (переиспользуемый)
            credentials_source: источник креденшиалов
            state_table: имя таблицы состояния
            schema: датасет BigQuery
            mount_point: Vault mount point
            path: Vault path
            proxy_enable: включить proxy
        """

        return create_bigquery_xmin_state_storage(
            bigquery_connector=bigquery_connector,
            connector_factory=cls.create_bigquery_connector,
            connection_id=connection_id,
            credentials_source=credentials_source,
            state_table=state_table,
            schema=schema,
            mount_point=mount_point,
            path=path,
            proxy_enable=proxy_enable,
        )


__all__ = ["BigQueryStateFactoryMixin"]
