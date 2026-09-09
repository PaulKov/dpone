from __future__ import annotations

from typing import Any

import pytest


def test_mssql_dbapi_safe_sample_client_executes_bounded_plan_and_closes_cursor() -> None:
    from dpone.runtime.safe_sample_sql_clients import MssqlDbApiSafeSampleSqlClient
    from dpone.services.mssql_safe_sample_sql_reader import MssqlSafeSampleReadPlan

    class Cursor:
        description = (("id",), ("email",))

        def __init__(self) -> None:
            self.executions: list[tuple[str, tuple[Any, ...]]] = []
            self.timeout: int | None = None
            self.closed = False

        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            self.executions.append((sql, params))

        def fetchall(self) -> list[tuple[Any, ...]]:
            return [(1, "alice@example.test")]

        def close(self) -> None:
            self.closed = True

    cursor = Cursor()

    class Connection:
        def cursor(self) -> Cursor:
            return cursor

    client = MssqlDbApiSafeSampleSqlClient(connection_provider=lambda: Connection())
    result = client.fetch(
        MssqlSafeSampleReadPlan(
            sql="SELECT TOP (@sample_rows) * FROM [dbo].[orders]",
            parameters={"sample_rows": 1000},
            sample_rows=1000,
            max_bytes=1024**3,
            timeout_seconds=60,
            source_read_only=True,
            source={"type": "mssql", "connection_ref": "mssql_dev"},
        )
    )

    assert cursor.executions == [
        (
            "DECLARE @sample_rows int = ?;\nSELECT TOP (@sample_rows) * FROM [dbo].[orders]",
            (1000,),
        )
    ]
    assert cursor.timeout == 60
    assert cursor.closed is True
    assert result["rows"] == [{"id": 1, "email": "alice@example.test"}]
    assert result["rows_read"] == 1
    assert result["bytes_read"] > 0
    assert result["diagnostics"] == {"client": "dbapi_mssql", "rows_returned": 1}


def test_clickhouse_connection_safe_sample_client_executes_insert_plan_without_serializing_rows() -> None:
    from dpone.runtime.safe_sample_sql_clients import ClickHouseConnectionSafeSampleSqlClient
    from dpone.services.clickhouse_safe_sample_sql_writer import ClickHouseSafeSampleInsertPlan

    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[dict[str, Any]]]] = []

        def execute(self, sql: str, rows: list[dict[str, Any]]) -> int:
            self.calls.append((sql, rows))
            return len(rows)

    connection = Connection()
    client = ClickHouseConnectionSafeSampleSqlClient(connection_provider=lambda: connection)
    plan = ClickHouseSafeSampleInsertPlan(
        sql="INSERT INTO `dpone_tmp`.`orders` (`email`, `id`) VALUES",
        rows=({"id": 1, "email": "alice@example.test"},),
        temporary_table={"schema": "dpone_tmp", "name": "orders"},
        sink={"type": "clickhouse", "connection_ref": "clickhouse_dev"},
    )

    result = client.insert(plan)

    assert connection.calls == [
        (
            "INSERT INTO `dpone_tmp`.`orders` (`email`, `id`) VALUES",
            [{"id": 1, "email": "alice@example.test"}],
        )
    ]
    assert result["rows_written"] == 1
    assert result["bytes_written"] > 0
    assert result["diagnostics"] == {"client": "clickhouse_connection", "rows_sent": 1}
    assert "alice@example.test" not in repr(result)


def test_clickhouse_connection_safe_sample_client_rejects_non_native_insert_before_io() -> None:
    from dpone.runtime.safe_sample_sql_clients import ClickHouseConnectionSafeSampleSqlClient
    from dpone.services.clickhouse_safe_sample_sql_writer import ClickHouseSafeSampleInsertPlan

    def unexpected_connection() -> object:
        raise AssertionError("invalid insert must fail before acquiring a connection")

    client = ClickHouseConnectionSafeSampleSqlClient(connection_provider=unexpected_connection)
    plan = ClickHouseSafeSampleInsertPlan(
        sql="INSERT INTO `dpone_tmp`.`orders` FORMAT JSONEachRow",
        rows=({"id": 1},),
        temporary_table={"schema": "dpone_tmp", "name": "orders"},
        sink={"type": "clickhouse", "connection_ref": "clickhouse_dev"},
    )

    with pytest.raises(ValueError, match="must end with VALUES"):
        client.insert(plan)


def test_clickhouse_connection_safe_sample_client_rejects_driver_row_count_mismatch() -> None:
    from dpone.runtime.safe_sample_sql_clients import ClickHouseConnectionSafeSampleSqlClient
    from dpone.services.clickhouse_safe_sample_sql_writer import ClickHouseSafeSampleInsertPlan

    class Connection:
        def execute(self, sql: str, rows: list[dict[str, Any]]) -> int:
            return 0

    client = ClickHouseConnectionSafeSampleSqlClient(connection_provider=lambda: Connection())
    plan = ClickHouseSafeSampleInsertPlan(
        sql="INSERT INTO `dpone_tmp`.`orders` (`id`) VALUES",
        rows=({"id": 1},),
        temporary_table={"schema": "dpone_tmp", "name": "orders"},
        sink={"type": "clickhouse", "connection_ref": "clickhouse_dev"},
    )

    with pytest.raises(RuntimeError, match="row count mismatch"):
        client.insert(plan)


@pytest.mark.parametrize("result", [None, [], True])
def test_clickhouse_connection_safe_sample_client_rejects_ambiguous_driver_acknowledgement(result: Any) -> None:
    from dpone.runtime.safe_sample_sql_clients import ClickHouseConnectionSafeSampleSqlClient
    from dpone.services.clickhouse_safe_sample_sql_writer import ClickHouseSafeSampleInsertPlan

    class Connection:
        def execute(self, sql: str, rows: list[dict[str, Any]]) -> Any:
            return result

    client = ClickHouseConnectionSafeSampleSqlClient(connection_provider=lambda: Connection())
    plan = ClickHouseSafeSampleInsertPlan(
        sql="INSERT INTO `dpone_tmp`.`orders` (`id`) VALUES",
        rows=({"id": 1},),
        temporary_table={"schema": "dpone_tmp", "name": "orders"},
        sink={"type": "clickhouse", "connection_ref": "clickhouse_dev"},
    )

    with pytest.raises(RuntimeError, match="integer inserted-row count"):
        client.insert(plan)


def test_clickhouse_connection_safe_sample_client_skips_empty_insert() -> None:
    from dpone.runtime.safe_sample_sql_clients import ClickHouseConnectionSafeSampleSqlClient
    from dpone.services.clickhouse_safe_sample_sql_writer import ClickHouseSafeSampleInsertPlan

    def unexpected_connection() -> object:
        raise AssertionError("empty batch must not acquire a connection")

    client = ClickHouseConnectionSafeSampleSqlClient(connection_provider=unexpected_connection)
    result = client.insert(
        ClickHouseSafeSampleInsertPlan(
            sql="INSERT INTO `dpone_tmp`.`orders` VALUES",
            rows=(),
            temporary_table={"schema": "dpone_tmp", "name": "orders"},
            sink={"type": "clickhouse", "connection_ref": "clickhouse_dev"},
        )
    )

    assert result["rows_written"] == 0
    assert result["diagnostics"] == {"client": "clickhouse_connection", "rows_sent": 0}


def test_safe_sample_credentials_factories_create_lazy_connector_clients_without_opening_connections() -> None:
    from dpone.runtime.credentials.config import CredentialsConfig
    from dpone.runtime.safe_sample_sql_clients import (
        ClickHouseCredentialsSafeSampleSqlClientFactory,
        MssqlCredentialsSafeSampleSqlClientFactory,
    )

    created: list[tuple[str, dict[str, Any]]] = []

    class LazyConnector:
        def __init__(self, connector_type: str, **kwargs: Any) -> None:
            created.append((connector_type, kwargs))
            self.connection_accesses = 0

        @property
        def connection(self) -> object:
            self.connection_accesses += 1
            raise AssertionError("connection must stay lazy during client factory create")

    mssql_client = MssqlCredentialsSafeSampleSqlClientFactory(
        connector_factory=lambda **kwargs: LazyConnector("mssql", **kwargs)
    ).create(
        CredentialsConfig(
            host="mssql.internal",
            port=1433,
            database="dwh",
            username="dpone",
            password="must-not-leak",
            driver="ODBC Driver 18 for SQL Server",
            query_timeout=30,
        )
    )
    clickhouse_client = ClickHouseCredentialsSafeSampleSqlClientFactory(
        connector_factory=lambda **kwargs: LazyConnector("clickhouse", **kwargs)
    ).create(
        CredentialsConfig(
            host="clickhouse.internal",
            port=9000,
            database="analytics",
            username="dpone",
            password="must-not-leak",
            secure=True,
        )
    )

    assert created == [
        (
            "mssql",
            {
                "host": "mssql.internal",
                "port": 1433,
                "database": "dwh",
                "user": "dpone",
                "password": "must-not-leak",
                "driver": "ODBC Driver 18 for SQL Server",
                "encrypt": "yes",
                "trust_server_certificate": "no",
                "connect_timeout": 10,
                "query_timeout": 30,
            },
        ),
        (
            "clickhouse",
            {
                "host": "clickhouse.internal",
                "port": 9000,
                "database": "analytics",
                "user": "dpone",
                "password": "must-not-leak",
                "secure": True,
                "compression": True,
                "connect_timeout": 10,
                "send_receive_timeout": 300,
                "settings": None,
            },
        ),
    ]
    assert mssql_client is not None
    assert clickhouse_client is not None
