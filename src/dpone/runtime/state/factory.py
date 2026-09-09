"""Фабрика для создания хранилищ состояния."""

from __future__ import annotations

from dpone.runtime.state.factory_bigquery_mixin import BigQueryStateFactoryMixin


class StateFactory(BigQueryStateFactoryMixin):
    @classmethod
    def create_mssql_connector(
        cls,
        connection_id: str | None,
        credentials_source: str = "airflow",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        """Creates a shared MSSQLConnector for state storage."""
        from dpone.runtime.credentials.config import CredentialsSource, require_connection_id
        from dpone.runtime.credentials.factory import BaseFactory

        return BaseFactory._create_mssql_connector(
            connection_id=require_connection_id(connection_id, backend="MSSQL"),
            credentials_source=CredentialsSource(credentials_source),
            mount_point=mount_point,
            path=path,
        )

    @classmethod
    def create_mssql_xmin_state_storage(
        cls,
        connection_id: str | None = None,
        mssql_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "etl_xmin_state",
        schema: str = "etl_state",
        database: str | None = None,
        receipt_table: str = "dpone_commit_receipt",
        repair_authority_table: str = "dpone_repair_authority",
        repair_consumption_table: str = "dpone_repair_authority_consumption",
        run_table: str = "etl_run_state",
        audit_table: str | None = None,
        atomicity: str = "after_target",
        provisioning: str = "runtime",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        """Creates MSSQL-backed xmin state storage."""
        from dpone.runtime.state.mssql import MSSQLXMinStateStorage

        connector = mssql_connector or cls.create_mssql_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
        )
        return MSSQLXMinStateStorage(
            connector=connector,
            schema=schema,
            table=state_table,
            database=database,
            receipt_table=receipt_table,
            repair_authority_table=repair_authority_table,
            repair_consumption_table=repair_consumption_table,
            run_table=run_table,
            audit_table=audit_table,
            atomicity=atomicity,
            provisioning=provisioning,
        )

    @classmethod
    def create_mssql_generic_transaction_state_storage(
        cls,
        *,
        mssql_connector,
        database: str,
        schema: str,
    ):
        """Create the narrow four-object state adapter for governed MSSQL loads."""

        from dpone.runtime.state.mssql_generic_transaction_storage import (
            MssqlGenericTransactionStateStorage,
        )

        return MssqlGenericTransactionStateStorage(
            mssql_connector,
            database=database,
            schema=schema,
        )

    @classmethod
    def create_mssql_kafka_offset_state_storage(
        cls,
        connection_id: str | None = None,
        mssql_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "etl_kafka_offsets",
        schema: str = "etl_state",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        from dpone.runtime.state.kafka import SQLKafkaOffsetStateStorage

        connector = mssql_connector or cls.create_mssql_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
        )
        return SQLKafkaOffsetStateStorage(connector=connector, schema=schema, table=state_table, dialect="mssql")

    @classmethod
    def create_mssql_run_state_storage(
        cls,
        connection_id: str | None = None,
        mssql_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "etl_run_state",
        schema: str = "etl_state",
        database: str | None = None,
        provisioning: str = "runtime",
        identity_policy: str = "legacy_compatible",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        """Creates MSSQL-backed run state storage."""
        from dpone.runtime.state.mssql import MSSQLRunStateStorage

        connector = mssql_connector or cls.create_mssql_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
        )
        return MSSQLRunStateStorage(
            connector=connector,
            schema=schema,
            table=state_table,
            database=database,
            provisioning=provisioning,
            identity_policy=identity_policy,
        )

    @classmethod
    def create_mssql_load_audit_storage(
        cls,
        connection_id: str | None = None,
        mssql_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "__dpone__loads",
        schema: str = "etl_state",
        database: str | None = None,
        provisioning: str = "runtime",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        """Creates MSSQL-backed canonical load audit storage."""
        from dpone.runtime.state.mssql import MSSQLLoadAuditStorage

        connector = mssql_connector or cls.create_mssql_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
        )
        return MSSQLLoadAuditStorage(
            connector=connector,
            schema=schema,
            table=state_table,
            database=database,
            provisioning=provisioning,
        )

    @classmethod
    def create_postgres_connector(
        cls,
        connection_id: str | None,
        credentials_source: str = "airflow",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        """Creates a shared PostgresConnector for state storage."""
        from dpone.runtime.credentials.config import CredentialsSource, require_connection_id
        from dpone.runtime.credentials.factory import BaseFactory

        return BaseFactory._create_postgres_connector(
            connection_id=require_connection_id(connection_id, backend="PostgreSQL"),
            credentials_source=CredentialsSource(credentials_source),
            mount_point=mount_point,
            path=path,
        )

    @classmethod
    def create_postgres_xmin_state_storage(
        cls,
        connection_id: str | None = None,
        postgres_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "etl_xmin_state",
        schema: str = "etl_state",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        """Creates PostgreSQL-backed xmin state storage."""
        from dpone.runtime.state.postgres import PostgresXMinStateStorage

        connector = postgres_connector or cls.create_postgres_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
        )
        return PostgresXMinStateStorage(connector=connector, schema=schema, table=state_table)

    @classmethod
    def create_postgres_kafka_offset_state_storage(
        cls,
        connection_id: str | None = None,
        postgres_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "etl_kafka_offsets",
        schema: str = "etl_state",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        from dpone.runtime.state.kafka import SQLKafkaOffsetStateStorage

        connector = postgres_connector or cls.create_postgres_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
        )
        return SQLKafkaOffsetStateStorage(connector=connector, schema=schema, table=state_table, dialect="postgres")

    @classmethod
    def create_postgres_run_state_storage(
        cls,
        connection_id: str | None = None,
        postgres_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "etl_run_state",
        schema: str = "etl_state",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        """Creates PostgreSQL-backed run state storage."""
        from dpone.runtime.state.postgres import PostgresRunStateStorage

        connector = postgres_connector or cls.create_postgres_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
        )
        return PostgresRunStateStorage(connector=connector, schema=schema, table=state_table)

    @classmethod
    def create_postgres_load_audit_storage(
        cls,
        connection_id: str | None = None,
        postgres_connector=None,
        credentials_source: str = "airflow",
        state_table: str = "__dpone__loads",
        schema: str = "etl_state",
        mount_point: str | None = None,
        path: str | None = None,
    ):
        """Creates PostgreSQL-backed canonical load audit storage."""
        from dpone.runtime.state.postgres import PostgresLoadAuditStorage

        connector = postgres_connector or cls.create_postgres_connector(
            connection_id=connection_id,
            credentials_source=credentials_source,
            mount_point=mount_point,
            path=path,
        )
        return PostgresLoadAuditStorage(connector=connector, schema=schema, table=state_table)
