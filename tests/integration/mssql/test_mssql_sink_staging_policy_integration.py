from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    QuietIntegrationLogger,
)

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sources.mssql import MSSQLSource

pytestmark = pytest.mark.integration_mssql


def _connector() -> MSSQLConnector:
    host = os.getenv("DPONE_IT_MSSQL_HOST")
    if not host:
        pytest.skip("DPONE_IT_MSSQL_HOST is not configured")
    connector = MSSQLConnector(
        host=host,
        port=int(os.getenv("DPONE_IT_MSSQL_PORT", "1433")),
        database=os.getenv("DPONE_IT_MSSQL_DATABASE", "dpone"),
        user=os.getenv("DPONE_IT_MSSQL_USER", "sa"),
        password=os.getenv("DPONE_IT_MSSQL_PASSWORD", ""),
        driver=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=os.getenv("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        bcp_path=os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
    )
    try:
        connector.execute_query("SELECT 1")
    except Exception as exc:
        connector.close()
        pytest.skip(f"MSSQL integration endpoint is unavailable: {exc}")
    return connector


def _load_config(schema: str, table: str, strategy: LoadStrategy, **kwargs) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="mssql-it",
        source_schema="dbo",
        source_table=table,
        target_schema=schema,
        target_table=table,
        staging_schema=schema,
        load_strategy=strategy,
        batch_size=2,
        **kwargs,
    )


def _rows(connector: MSSQLConnector, schema: str, table: str) -> list[tuple]:
    return connector.get_records(f"SELECT [id], [name] FROM [{schema}].[{table}] ORDER BY [id]")


def _leftover_tables(connector: MSSQLConnector, schema: str, table: str) -> list[str]:
    rows = connector.get_records(
        """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME LIKE ?
        ORDER BY TABLE_NAME
        """,
        (schema, f"{table}__dpone_%"),
    )
    return [str(row[0]) for row in rows]


def test_mssql_sink_full_refresh_merge_and_replace_use_verified_staging() -> None:
    connector = _connector()
    schema = f"it_stage_{uuid.uuid4().hex[:8]}"
    table = "orders"
    sink = MSSQLSink(connector)
    payload_schema = [("id", "int"), ("name", "nvarchar(100)")]

    schema_created = False
    try:
        connector.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        schema_created = True

        first = sink.load(
            _load_config(schema, table, LoadStrategy.FULL_REFRESH),
            LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]),
                schema=payload_schema,
            ),
        )
        assert first.inserted_rows == 2
        assert _rows(connector, schema, table) == [(1, "alpha"), (2, "beta")]

        refreshed = sink.load(
            _load_config(schema, table, LoadStrategy.FULL_REFRESH),
            LoadPayload(artifact=InMemoryRowsArtifact([{"id": 3, "name": "gamma"}]), schema=payload_schema),
        )
        assert refreshed.inserted_rows == 1
        assert _rows(connector, schema, table) == [(3, "gamma")]

        merged = sink.load(
            _load_config(schema, table, LoadStrategy.INCREMENTAL_MERGE, unique_key="id"),
            LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 3, "name": "gamma-updated"}, {"id": 4, "name": "delta"}]),
                schema=payload_schema,
            ),
        )
        assert merged.updated_rows == 1
        assert _rows(connector, schema, table) == [(3, "gamma-updated"), (4, "delta")]

        replaced = sink.load(
            _load_config(schema, table, LoadStrategy.REPLACE, custom_predicate="[id] = 4"),
            LoadPayload(artifact=InMemoryRowsArtifact([{"id": 5, "name": "epsilon"}]), schema=payload_schema),
        )
        assert replaced.replaced_rows == 1
        assert _rows(connector, schema, table) == [(3, "gamma-updated"), (5, "epsilon")]
        assert _leftover_tables(connector, schema, table) == []
    finally:
        if schema_created:
            connector.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
            for leftover in _leftover_tables(connector, schema, table):
                connector.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{leftover}]")
            connector.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        connector.close()


def test_standard_etl_replace_preserves_existing_target_physical_fingerprint(tmp_path: Path) -> None:
    """Existing-target REPLACE is scoped DELETE+INSERT, never an object swap."""

    source = _connector()
    target = _connector()
    suffix = uuid.uuid4().hex[:8]
    source_table = f"replace_source_{suffix}"
    target_table = f"replace_target_{suffix}"
    reference_table = f"replace_scope_{suffix}"
    logger = QuietIntegrationLogger()

    def fingerprint() -> dict[str, object]:
        object_id = target.get_records(
            "SELECT OBJECT_ID(?)",
            (f"dpone_it.{target_table}",),
        )[0][0]
        indexes = target.get_records(
            "SELECT i.name, i.type_desc, i.is_unique, i.filter_definition, p.data_compression_desc "
            "FROM sys.indexes AS i "
            "INNER JOIN sys.tables AS t ON t.object_id = i.object_id "
            "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "LEFT JOIN sys.partitions AS p ON p.object_id = i.object_id AND p.index_id = i.index_id "
            "WHERE s.name = N'dpone_it' AND t.name = ? AND i.index_id > 0 ORDER BY i.index_id",
            (target_table,),
        )
        checks = target.get_records(
            "SELECT cc.name, cc.definition, cc.is_disabled FROM sys.check_constraints AS cc "
            "INNER JOIN sys.tables AS t ON t.object_id = cc.parent_object_id "
            "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE s.name = N'dpone_it' AND t.name = ? ORDER BY cc.name",
            (target_table,),
        )
        foreign_keys = target.get_records(
            "SELECT fk.name, OBJECT_NAME(fk.referenced_object_id), fk.is_disabled "
            "FROM sys.foreign_keys AS fk "
            "INNER JOIN sys.tables AS t ON t.object_id = fk.parent_object_id "
            "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE s.name = N'dpone_it' AND t.name = ? ORDER BY fk.name",
            (target_table,),
        )
        permissions = target.get_records(
            "SELECT dp.state_desc, dp.permission_name, USER_NAME(dp.grantee_principal_id) "
            "FROM sys.database_permissions AS dp WHERE dp.major_id = OBJECT_ID(?) "
            "ORDER BY dp.permission_name, dp.state_desc",
            (f"dpone_it.{target_table}",),
        )
        return {
            "object_id": object_id,
            "indexes": indexes,
            "checks": checks,
            "foreign_keys": foreign_keys,
            "permissions": permissions,
        }

    try:
        target.execute_query(
            f"DROP TABLE IF EXISTS [dpone_it].[{target_table}]; "
            f"DROP TABLE IF EXISTS [dpone_it].[{source_table}]; "
            f"DROP TABLE IF EXISTS [dpone_it].[{reference_table}]"
        )
        target.execute_query(
            f"CREATE TABLE [dpone_it].[{reference_table}] ([scope_id] int NOT NULL PRIMARY KEY); "
            f"INSERT INTO [dpone_it].[{reference_table}] VALUES (1), (2); "
            f"CREATE TABLE [dpone_it].[{source_table}] ("
            "[id] int NOT NULL, [scope_id] int NOT NULL, [value] nvarchar(64) NOT NULL); "
            f"CREATE TABLE [dpone_it].[{target_table}] ("
            "[id] int NOT NULL, [scope_id] int NOT NULL, [value] nvarchar(64) NOT NULL, "
            f"CONSTRAINT [ck_{suffix}] CHECK ([id] > 0), "
            f"CONSTRAINT [fk_{suffix}] FOREIGN KEY ([scope_id]) "
            f"REFERENCES [dpone_it].[{reference_table}] ([scope_id])); "
            f"CREATE NONCLUSTERED INDEX [ix_{suffix}] ON [dpone_it].[{target_table}] ([value]); "
            f"GRANT SELECT ON OBJECT::[dpone_it].[{target_table}] TO public; "
            f"INSERT INTO [dpone_it].[{source_table}] VALUES (1, 1, N'new-scope-row'); "
            f"INSERT INTO [dpone_it].[{target_table}] VALUES "
            "(10, 1, N'old-scope-row'), (20, 2, N'outside-sentinel')"
        )
        before = fingerprint()
        config = LoadConfig(
            source_conn_id="mssql_source",
            target_conn_id="mssql_sink",
            source_schema="dpone_it",
            source_table=source_table,
            target_schema="dpone_it",
            target_table=target_table,
            staging_schema="staging",
            load_strategy=LoadStrategy.REPLACE,
            custom_predicate="[scope_id] = 1",
            options={
                "source_type": "mssql",
                "sink_type": "mssql",
                "work_dir": str(tmp_path),
                "batch_commit_mode": "whole",
                "technical_columns": "forbidden",
                "schema_evolution": False,
            },
        )
        processor = ETLProcessor(
            MSSQLSource(source, logger, sink_connector=target),
            MSSQLSink(target, logger=logger),
            etl_logger=logger,
        )

        result = processor.run(config, dag_id="DAG__integration__mssql__replace_preservation")

        assert result["status"] == "success"
        assert target.get_records(
            f"SELECT [id], [scope_id], [value] FROM [dpone_it].[{target_table}] ORDER BY [id]"
        ) == [(1, 1, "new-scope-row"), (20, 2, "outside-sentinel")]
        assert fingerprint() == before
        assert _leftover_tables(target, "dpone_it", target_table) == []
    finally:
        target.execute_query(f"DROP TABLE IF EXISTS [dpone_it].[{target_table}]")
        target.execute_query(f"DROP TABLE IF EXISTS [dpone_it].[{source_table}]")
        target.execute_query(f"DROP TABLE IF EXISTS [dpone_it].[{reference_table}]")
        source.close()
        target.close()
