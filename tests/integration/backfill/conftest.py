"""Fixtures for backfill integration tests.

PostgreSQL/ClickHouse fixtures are inherited from ``tests/integration/conftest.py``.
This module adds SQL Server and Kafka session fixtures with the same
skip-first UX: missing drivers or unreachable services produce a clear skip
reason locally while CI provides the full environment.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

import pytest

_TRUTHY = {"1", "true", "yes", "on"}


def require_integration() -> None:
    if str(os.environ.get("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in _TRUTHY:
        pytest.skip("Integration tests are disabled. Set DPONE_RUN_INTEGRATION=1 to enable them.")


@dataclass(frozen=True, slots=True)
class MSSQLIntegrationSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    driver: str
    connect_timeout: float = 60.0
    poll_interval: float = 2.0


def read_mssql_integration_settings(env: Mapping[str, str] | None = None) -> MSSQLIntegrationSettings:
    source = env or os.environ
    return MSSQLIntegrationSettings(
        host=source.get("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
        port=int(source.get("DPONE_IT_MSSQL_PORT", "51433")),
        database=source.get("DPONE_IT_MSSQL_DATABASE", "dpone_it"),
        user=source.get("DPONE_IT_MSSQL_USER", "sa"),
        password=source.get("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!"),
        driver=source.get("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        connect_timeout=float(source.get("DPONE_IT_MSSQL_CONNECT_TIMEOUT", "60")),
        poll_interval=float(source.get("DPONE_IT_MSSQL_POLL_INTERVAL", "2")),
    )


@pytest.fixture(scope="session")
def mssql_settings() -> MSSQLIntegrationSettings:
    return read_mssql_integration_settings()


@pytest.fixture(scope="session")
def mssql_connector(mssql_settings: MSSQLIntegrationSettings):
    require_integration()
    pyodbc = pytest.importorskip("pyodbc", reason="pyodbc is required for MSSQL backfill routes")
    if mssql_settings.driver not in pyodbc.drivers():
        pytest.skip(
            f"MSSQL ODBC driver {mssql_settings.driver!r} is not installed "
            f"(available: {pyodbc.drivers() or 'none'}); MSSQL backfill routes run in CI"
        )
    from dpone.runtime.connectors.mssql import MSSQLConnector

    bootstrap_kwargs = dict(
        host=mssql_settings.host,
        port=mssql_settings.port,
        user=mssql_settings.user,
        password=mssql_settings.password,
        driver=mssql_settings.driver,
        trust_server_certificate="yes",
    )
    deadline = time.monotonic() + mssql_settings.connect_timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            bootstrap = MSSQLConnector(database="master", **bootstrap_kwargs)
            bootstrap.execute_query(
                f"IF DB_ID('{mssql_settings.database}') IS NULL CREATE DATABASE [{mssql_settings.database}]"
            )
            bootstrap.close()
            break
        except Exception as exc:  # pragma: no cover - external service bootstrap
            last_error = exc
            time.sleep(mssql_settings.poll_interval)
    else:
        pytest.skip(f"MSSQL integration service is not reachable: {last_error}")

    connector = MSSQLConnector(database=mssql_settings.database, **bootstrap_kwargs)
    try:
        yield connector
    finally:
        connector.close()


@pytest.fixture
def mssql_schema(mssql_connector) -> Iterator[str]:
    schema = f"it_{uuid.uuid4().hex[:10]}"
    mssql_connector.execute_query(f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]')")
    try:
        yield schema
    finally:
        for row in mssql_connector.get_records(
            "SELECT t.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = ?",
            params=(schema,),
        ):
            mssql_connector.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{row[0]}]")
        mssql_connector.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")


@pytest.fixture(scope="session")
def kafka_connector():
    require_integration()
    pytest.importorskip("confluent_kafka", reason="confluent-kafka is required for Kafka backfill routes")
    from dpone.runtime.connectors.kafka import KafkaConnector

    bootstrap = os.getenv("DPONE_IT_KAFKA_BOOTSTRAP") or os.getenv("DPONE_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:59092")
    connector = KafkaConnector(bootstrap_servers=bootstrap, client_id="dpone-backfill-it")
    try:
        producer = connector.create_producer(options={"socket.timeout.ms": 5000})
        metadata = producer.list_topics(timeout=10)
        if not metadata.brokers:
            pytest.skip(f"Kafka broker at {bootstrap} reported no brokers")
    except Exception as exc:  # pragma: no cover - external service bootstrap
        pytest.skip(f"Kafka integration service is not reachable at {bootstrap}: {exc}")
    return connector
