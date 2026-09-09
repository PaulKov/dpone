"""Registry catalog wins over leftover Airflow Connection URI identity."""

from __future__ import annotations

from dpone.runtime.credentials.airflow_uri_registry import credentials_from_airflow_connection_uri


def test_uri_without_registry_keeps_leftover_catalog() -> None:
    parsed = credentials_from_airflow_connection_uri("mssql://etl:secret@192.0.2.211:1433/analytics_staging")

    assert parsed.host == "192.0.2.211"
    assert parsed.port == 1433
    assert parsed.username == "etl"
    assert parsed.password == "secret"
    assert parsed.database == "analytics_staging"


def test_registry_database_and_schema_replace_leftover_uri_catalog() -> None:
    parsed = credentials_from_airflow_connection_uri(
        "mssql://etl:secret@192.0.2.211:1433/analytics_staging",
        connection={"database": "Example_System", "schema": "dbo"},
    )

    assert parsed.host == "192.0.2.211"
    assert parsed.port == 1433
    assert parsed.username == "etl"
    assert parsed.password == "secret"
    assert parsed.database == "Example_System"
    assert parsed.schema == "dbo"
