from __future__ import annotations

import sys
from types import SimpleNamespace

from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.credentials.airflow_env import parse_airflow_connection_uri
from dpone.runtime.credentials.config import CredentialsConfig, CredentialsSource
from dpone.runtime.credentials.factory import BaseFactory
from dpone.runtime.credentials.providers import AirflowCredentialsProvider


def test_mssql_connector_includes_allowlisted_odbc_options() -> None:
    connector = MSSQLConnector(
        host="sql",
        port=1433,
        database="dwh",
        user="u",
        password="p",
        odbc_options={
            "multi_subnet_failover": "yes",
            "application_intent": "ReadOnly",
            "transparent_network_ip_resolution": "no",
            "PWD": "must-not-override-password",
        },
    )

    connection_string = connector._connection_string()

    assert "MultiSubnetFailover=Yes" in connection_string
    assert "ApplicationIntent=ReadOnly" in connection_string
    assert "TransparentNetworkIPResolution=No" in connection_string
    assert "must-not-override-password" not in connection_string


def test_mssql_connector_passes_connect_timeout_to_pyodbc(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class FakePyodbc:
        @staticmethod
        def connect(connection_string, **kwargs):
            calls.append({"connection_string": connection_string, **kwargs})
            return SimpleNamespace(timeout=0)

    monkeypatch.setitem(sys.modules, "pyodbc", FakePyodbc)
    connector = MSSQLConnector(
        host="sql",
        port=1433,
        database="dwh",
        user="u",
        password="p",
        connect_timeout=7,
    )

    assert connector.connection is not None
    assert calls[0]["timeout"] == 7


def test_mssql_factory_passes_airflow_extras_to_odbc_and_timeout() -> None:
    class Manager:
        def get_credentials(self, connection_name, source, mount_point=None, path=None):
            del connection_name, source, mount_point, path
            return CredentialsConfig(
                host="sql.example.com",
                port=1433,
                database="dwh",
                username="etl",
                password="secret",
                additional_params={
                    "multi_subnet_failover": "yes",
                    "connect_timeout": "5",
                    "application_intent": "ReadOnly",
                    "unsafe": "ignored",
                },
            )

    original_manager = BaseFactory.manager
    BaseFactory.manager = Manager()
    try:
        connector = BaseFactory._create_mssql_connector("mssql-demo", CredentialsSource.AIRFLOW)
    finally:
        BaseFactory.manager = original_manager

    connection_string = connector._connection_string()
    assert "MultiSubnetFailover=Yes" in connection_string
    assert "ApplicationIntent=ReadOnly" in connection_string
    assert "unsafe=ignored" not in connection_string
    assert connector.connect_timeout == 5


def test_airflow_mssql_uri_parser_maps_connect_timeout_and_raw_extras() -> None:
    creds = parse_airflow_connection_uri(
        "mssql://etl:secret@sql.example.com:1433/DWH"
        "?connect_timeout=5&multi_subnet_failover=yes&trust_server_certificate=yes"
    )

    assert creds.connect_timeout == 5
    assert creds.trust_server_certificate == "yes"
    assert creds.additional_params == {
        "connect_timeout": "5",
        "multi_subnet_failover": "yes",
        "trust_server_certificate": "yes",
    }


def test_airflow_mssql_uri_parser_accepts_odbc_style_extra_names() -> None:
    creds = parse_airflow_connection_uri(
        "mssql://etl:secret@sql.example.com:1433/DWH"
        "?Encrypt=yes&TrustServerCertificate=yes&LoginTimeout=7&QueryTimeout=30"
    )

    assert creds.encrypt == "yes"
    assert creds.trust_server_certificate == "yes"
    assert creds.connect_timeout == 7
    assert creds.query_timeout == 30
    assert creds.additional_params == {
        "Encrypt": "yes",
        "TrustServerCertificate": "yes",
        "LoginTimeout": "7",
        "QueryTimeout": "30",
    }


def test_airflow_mssql_uri_parser_accepts_sqlalchemy_dialect_schemes() -> None:
    pymssql = parse_airflow_connection_uri(
        "mssql+pymssql://etl:secret@sql.example.com:1433/dwh_example"
        "?trust_server_certificate=yes&multi_subnet_failover=yes&connect_timeout=5"
    )
    pyodbc = parse_airflow_connection_uri(
        "mssql+pyodbc://etl:secret@sql.example.com:1433/dwh_example"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )

    assert pymssql.host == "sql.example.com"
    assert pymssql.database == "dwh_example"
    assert pymssql.trust_server_certificate == "yes"
    assert pymssql.connect_timeout == 5
    assert pymssql.additional_params["multi_subnet_failover"] == "yes"
    assert pyodbc.driver == "ODBC Driver 18 for SQL Server"
    assert pyodbc.trust_server_certificate == "yes"


def test_airflow_basehook_mssql_provider_maps_connect_timeout_and_raw_extras() -> None:
    creds = AirflowCredentialsProvider._from_mssql(
        SimpleNamespace(
            conn_type="mssql",
            host="sql.example.com",
            port=1433,
            schema="DWH",
            login="etl",
            password="secret",
            extra='{"connect_timeout":5,"multi_subnet_failover":"yes","trust_server_certificate":"yes"}',
        )
    )

    assert creds.connect_timeout == 5
    assert creds.trust_server_certificate == "yes"
    assert creds.additional_params == {
        "connect_timeout": 5,
        "multi_subnet_failover": "yes",
        "trust_server_certificate": "yes",
    }


def test_airflow_basehook_mssql_provider_accepts_odbc_style_extra_names() -> None:
    creds = AirflowCredentialsProvider._from_mssql(
        SimpleNamespace(
            conn_type="mssql",
            host="sql.example.com",
            port=1433,
            schema="DWH",
            login="etl",
            password="secret",
            extra='{"Encrypt":"yes","TrustServerCertificate":"yes","LoginTimeout":7,"QueryTimeout":30}',
        )
    )

    assert creds.encrypt == "yes"
    assert creds.trust_server_certificate == "yes"
    assert creds.connect_timeout == 7
    assert creds.query_timeout == 30
    assert creds.additional_params == {
        "Encrypt": "yes",
        "TrustServerCertificate": "yes",
        "LoginTimeout": 7,
        "QueryTimeout": 30,
    }
