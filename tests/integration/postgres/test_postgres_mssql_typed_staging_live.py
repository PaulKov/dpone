"""Real SQL Server proof for one-pass typed snapshot staging."""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.incremental_snapshot import DELTA_HASH_COLUMN
from dpone.runtime.sinks.staging_managers.mssql_typed_file import MssqlTypedFileIngestor
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from tests.integration.postgres.postgres_live_support import (
    ensure_mssql_database_and_schemas,
    mssql_connector,
    postgres_mssql_enabled,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.skipif(
        not postgres_mssql_enabled(), reason="PostgreSQL + MSSQL integration credentials are not configured"
    ),
]

_TABLE = "typed_snapshot_staging_live"
_DIRECT_TABLE = "typed_initial_business_prefix_live"
_COLUMNS = (
    "id",
    "amount",
    "price",
    "active",
    "business_date",
    "created_at",
    "event_at",
    "clock",
    "payload",
    "label",
    DELTA_HASH_COLUMN,
)
_TYPES = {
    "id": "uniqueidentifier",
    "amount": "int",
    "price": "decimal(12,4)",
    "active": "bit",
    "business_date": "date",
    "created_at": "datetime2(6)",
    "event_at": "datetimeoffset(6)",
    "clock": "time(6)",
    "payload": "varbinary(16)",
    "label": "nvarchar(64)",
    DELTA_HASH_COLUMN: "char(64)",
}


def test_typed_snapshot_staging_preserves_native_values_via_real_bcp(tmp_path: Path) -> None:
    """Bulk-load native values without raw text staging or TRY_CONVERT scans."""

    database = ensure_mssql_database_and_schemas()
    connector = mssql_connector(database=database)
    codec = BulkTextCodec()
    qualified = f"[{database}].[staging].[{_TABLE}]"
    try:
        connector.execute_query(f"DROP TABLE IF EXISTS {qualified}")
        connector.execute_query(_create_table_sql(database))
        artifact = _artifact(
            tmp_path,
            codec,
            rows=(
                (
                    "550e8400-e29b-41d4-a716-446655440000",
                    "7",
                    "123.4500",
                    "1",
                    "2026-08-20",
                    "2026-08-20 10:11:12.123456",
                    "2026-08-20 10:11:12.123456+03:00",
                    "10:11:12.123456",
                    "deadbeef",
                    codec.encode("Привет\tмир"),
                ),
                (
                    "550e8400-e29b-41d4-a716-446655440001",
                    "8",
                    "0.0001",
                    "0",
                    "2026-08-21",
                    "2026-08-21 00:00:00.000000",
                    "2026-08-21 00:00:00.000000+00:00",
                    "00:00:00.000000",
                    "",
                    codec.encode(""),
                ),
            ),
        )
        staging = StagingTableArtifact(
            database=database,
            schema="staging",
            table=_TABLE,
            columns=_COLUMNS,
            staging_manager=SimpleNamespace(),
            column_types=dict(_TYPES),
            bulk_options=BulkOptionsResolver.resolve({"bulk": {"bcp": {"batch_size": 1}}}),
            typed_file_ingestion=True,
        )

        inserted = MssqlTypedFileIngestor(connector, SimpleNamespace()).ingest(staging, artifact)

        assert inserted == 2
        assert staging.typed_transport == "mssql.bcp.length_prefixed_utf8.v1"
        assert staging.typed_batch_count == 2
        assert connector.get_records(_readback_sql(qualified)) == [
            (
                "550E8400-E29B-41D4-A716-446655440000",
                7,
                "123.4500",
                True,
                "2026-08-20",
                "2026-08-20T10:11:12.123456",
                "2026-08-20T07:11:12.123456Z",
                "10:11:12.123456",
                "DEADBEEF",
                "Привет\tмир",
            ),
            (
                "550E8400-E29B-41D4-A716-446655440001",
                8,
                "0.0001",
                False,
                "2026-08-21",
                "2026-08-21T00:00:00",
                "2026-08-21T00:00:00Z",
                "00:00:00.000000",
                None,
                "",
            ),
        ]
    finally:
        connector.execute_query(f"DROP TABLE IF EXISTS {qualified}")
        connector.close()


def test_typed_initial_staging_skips_nullable_framework_suffix_via_real_bcp(tmp_path: Path) -> None:
    """A business-only host file must bind the native prefix without a raw table."""

    database = ensure_mssql_database_and_schemas()
    connector = mssql_connector(database=database)
    codec = BulkTextCodec()
    qualified = f"[{database}].[staging].[{_DIRECT_TABLE}]"
    path = tmp_path / "typed_initial_business_prefix.bcp"
    path.write_text(
        "550e8400-e29b-41d4-a716-446655440000\t" + codec.encode("Привет\tмир") + "\n",
        encoding="utf-8",
    )
    artifact = FileExportArtifact(
        str(path),
        ("id", "label"),
        format="mssql-delimited",
        bulk_text_codec=codec,
        rows_exported=1,
    )
    try:
        connector.execute_query(f"DROP TABLE IF EXISTS {qualified}")
        connector.execute_query(
            f"""
            CREATE TABLE {qualified} (
                [id] uniqueidentifier NOT NULL,
                [label] nvarchar(64) NULL,
                [__dpone__row_hash] varchar(64) NULL,
                [__dpone__deleted_at] datetime2(7) NULL
            )
            """
        )
        staging = StagingTableArtifact(
            database=database,
            schema="staging",
            table=_DIRECT_TABLE,
            columns=("id", "label", "__dpone__row_hash", "__dpone__deleted_at"),
            staging_manager=SimpleNamespace(),
            column_types={
                "id": "uniqueidentifier",
                "label": "nvarchar(64)",
                "__dpone__row_hash": "varchar(64)",
                "__dpone__deleted_at": "datetime2(7)",
            },
            wire_schema=(("id", "uuid"), ("label", "text")),
            bulk_options=BulkOptionsResolver.resolve({}),
            typed_file_ingestion=True,
            typed_file_row_hash_validation=False,
            typed_file_deferred_native_evidence=True,
            direct_native_staging=True,
        )

        inserted = MssqlTypedFileIngestor(connector, SimpleNamespace()).ingest(staging, artifact)

        assert inserted == 1
        assert connector.get_records(
            f"SELECT CONVERT(varchar(36), [id]), [label], [__dpone__row_hash], [__dpone__deleted_at] FROM {qualified}"
        ) == [("550E8400-E29B-41D4-A716-446655440000", "Привет\tмир", None, None)]
        assert staging.consumed_payload_evidence.require_complete(native=False).actual_raw_rows == 1
    finally:
        connector.execute_query(f"DROP TABLE IF EXISTS {qualified}")
        connector.close()


def _artifact(tmp_path: Path, codec: BulkTextCodec, *, rows: tuple[tuple[str, ...], ...]) -> FileExportArtifact:
    path = tmp_path / "typed_snapshot_staging.bcp"
    with path.open("wb") as handle:
        for row in rows:
            payload = "\t".join(row)
            digest = hashlib.sha256(payload.encode("utf-8").decode("utf-8").encode("utf-16le")).hexdigest()
            handle.write(f"{payload}\t{digest}\n".encode())
    return FileExportArtifact(
        str(path),
        _COLUMNS,
        format="mssql-delimited",
        bulk_text_codec=codec,
        rows_exported=len(rows),
    )


def _create_table_sql(database: str) -> str:
    return f"""
        CREATE TABLE [{database}].[staging].[{_TABLE}] (
            [id] uniqueidentifier NOT NULL,
            [amount] int NOT NULL,
            [price] decimal(12,4) NOT NULL,
            [active] bit NOT NULL,
            [business_date] date NOT NULL,
            [created_at] datetime2(6) NOT NULL,
            [event_at] datetimeoffset(6) NOT NULL,
            [clock] time(6) NOT NULL,
            [payload] varbinary(16) NULL,
            [label] nvarchar(64) NULL,
            [{DELTA_HASH_COLUMN}] char(64) NOT NULL
        )
    """


def _readback_sql(qualified: str) -> str:
    return f"""
        SELECT
            CONVERT(varchar(36), [id]),
            [amount],
            CONVERT(varchar(32), [price]),
            [active],
            CONVERT(varchar(10), [business_date], 23),
            CONVERT(varchar(27), [created_at], 126),
            CONVERT(varchar(40), [event_at], 127),
            CONVERT(varchar(24), [clock]),
            CONVERT(varchar(max), [payload], 2),
            [label]
        FROM {qualified}
        ORDER BY [amount]
    """
