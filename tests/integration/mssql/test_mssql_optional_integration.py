from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.connectors.mssql_bulk import DelimitedBulkFile
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.support.bulk_text_codec import BulkTextCodec

pytestmark = pytest.mark.integration_mssql


def _connector() -> MSSQLConnector:
    host = os.getenv("DPONE_IT_MSSQL_HOST")
    if not host:
        pytest.skip("DPONE_IT_MSSQL_HOST is not configured")
    return MSSQLConnector(
        host=host,
        port=int(os.getenv("DPONE_IT_MSSQL_PORT", "1433")),
        database=os.getenv("DPONE_IT_MSSQL_DATABASE", "dpone"),
        user=os.getenv("DPONE_IT_MSSQL_USER", "sa"),
        password=os.getenv("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!"),
        driver=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=os.getenv("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        bcp_path=os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
    )


def test_mssql_bcp_import_roundtrip(tmp_path: Path) -> None:
    connector = _connector()
    file_path: str | None = None
    try:
        connector.execute_query("IF SCHEMA_ID('it') IS NULL EXEC('CREATE SCHEMA [it]')")
        connector.execute_query("DROP TABLE IF EXISTS [it].[bulk_roundtrip]")
        connector.execute_query("CREATE TABLE [it].[bulk_roundtrip] ([id] int NOT NULL, [name] nvarchar(100) NULL)")

        writer = DelimitedBulkFile(directory=str(tmp_path))
        file_path, expected_rows = writer.write_rows(
            [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
            ["id", "name"],
        )

        copied_rows = connector.bcp_import("it", "bulk_roundtrip", file_path)
        rows = connector.get_records("SELECT [id], [name] FROM [it].[bulk_roundtrip] ORDER BY [id]")

        assert copied_rows == expected_rows
        assert rows == [(1, "alpha"), (2, "beta")]
    finally:
        if file_path:
            Path(file_path).unlink(missing_ok=True)
        connector.execute_query("DROP TABLE IF EXISTS [it].[bulk_roundtrip]")
        connector.close()


def test_mssql_verified_empty_payload_has_receipt_without_bcp(tmp_path: Path) -> None:
    """A complete empty snapshot has cryptographic evidence, not fake BCP success."""

    connector = _connector()
    manager = MSSQLStagingManager(connector)
    config = SimpleNamespace(
        staging_schema="it",
        target_schema="it",
        staging_table="empty_payload_staging",
        target_table="empty_payload_target",
        staging_database=None,
        options={},
        reconciliation=False,
        batch_size=1000,
    )
    path = tmp_path / "empty.bcp"
    path.write_bytes(b"")
    file_artifact = FileExportArtifact(
        str(path),
        ["id"],
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=0,
    )
    staging = None
    try:
        connector.execute_query("IF SCHEMA_ID('it') IS NULL EXEC('CREATE SCHEMA [it]')")
        connector.execute_query("DROP TABLE IF EXISTS [it].[empty_payload_staging]")
        staging = manager.create(config, [("id", "int")])

        def unexpected_bcp(*_args, **_kwargs):
            raise AssertionError("zero-row evidence must not invoke bcp")

        connector.bcp_import = unexpected_bcp
        assert manager.load_from_file(staging, file_artifact) == 0
        evidence = staging.consumed_payload_evidence.require_complete(native=False)
        assert evidence.declared_rows == evidence.actual_raw_rows == 0
        assert manager.count_rows("it", "empty_payload_staging") == 0
    finally:
        if staging is not None:
            staging.cleanup()
        connector.close()


def test_mssql_real_bcp_reject_cleans_staging_and_preserves_target(tmp_path: Path) -> None:
    """A vendor-rejected/partial file never becomes an accepted payload."""

    connector = _connector()
    manager = MSSQLStagingManager(connector)
    error_file = tmp_path / "rejected.err"
    config = SimpleNamespace(
        staging_schema="it",
        target_schema="it",
        staging_table="rejected_payload_staging",
        target_table="bcp_fault_target",
        staging_database=None,
        options={
            "allow_unsafe_raw_mssql_bulk_files": True,
            "__dpone_mssql_native_staging": True,
            "__dpone_mssql_native_column_types": {"id": "int"},
            "bulk": {"mode": "bcp", "bcp": {"error_file": str(error_file)}},
        },
        reconciliation=False,
        batch_size=1000,
    )
    path = tmp_path / "partial.bcp"
    path.write_bytes(b"1\nnot-an-integer\n")
    file_artifact = FileExportArtifact(
        str(path),
        ["id"],
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=2,
    )
    try:
        connector.execute_query("IF SCHEMA_ID('it') IS NULL EXEC('CREATE SCHEMA [it]')")
        connector.execute_query("DROP TABLE IF EXISTS [it].[rejected_payload_staging]")
        connector.execute_query("DROP TABLE IF EXISTS [it].[bcp_fault_target]")
        connector.execute_query(
            "CREATE TABLE [it].[bcp_fault_target] ([id] int NOT NULL PRIMARY KEY, [value] nvarchar(64) NOT NULL)"
        )
        connector.execute_query("INSERT INTO [it].[bcp_fault_target] ([id], [value]) VALUES (7, N'before-image')")
        before = connector.get_records("SELECT [id], [value] FROM [it].[bcp_fault_target]")

        with pytest.raises(ArtifactIntegrityError) as raised:
            file_artifact.materialize(manager, config, [("id", "int")])

        assert raised.value.code == "artifact_integrity.bcp_rejected_rows"
        assert connector.get_records("SELECT [id], [value] FROM [it].[bcp_fault_target]") == before
        assert not connector.table_exists("it", "rejected_payload_staging")
        assert error_file.exists() and error_file.stat().st_size > 0
    finally:
        connector.execute_query("DROP TABLE IF EXISTS [it].[rejected_payload_staging]")
        connector.execute_query("DROP TABLE IF EXISTS [it].[bcp_fault_target]")
        connector.close()
