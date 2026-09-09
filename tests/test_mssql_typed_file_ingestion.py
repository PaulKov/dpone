from __future__ import annotations

import hashlib
import io
import struct
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.incremental_snapshot import DELTA_HASH_COLUMN
from dpone.runtime.sinks.staging_managers.mssql_typed_bcp import write_length_prefixed_row
from dpone.runtime.sinks.staging_managers.mssql_typed_file import MssqlTypedFileIngestor
from dpone.runtime.sinks.staging_managers.mssql_typed_values import (
    MssqlTypedValueDecoder,
    MssqlTypedValueError,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_typed_config import (
    typed_snapshot_staging_config,
)
from dpone.runtime.support.bulk_text_codec import BulkTextCodec


@pytest.mark.parametrize(
    ("raw", "dtype", "expected"),
    [
        (b"7", "int", 7),
        (b"1", "bit", True),
        (b"123.4500", "decimal(10,4)", Decimal("123.4500")),
        (b"99999999999999999999999999999999999999", "decimal(38,0)", Decimal("9" * 38)),
        (b"550e8400-e29b-41d4-a716-446655440000", "uniqueidentifier", "550e8400-e29b-41d4-a716-446655440000"),
        (b"2026-08-20", "date", date(2026, 8, 20)),
        (b"2026-08-20 10:11:12.123456", "datetime2(6)", datetime(2026, 8, 20, 10, 11, 12, 123456)),
        (b"10:11:12.123456", "time(6)", "10:11:12.123456"),
        (b"deadbeef", "varbinary(max)", bytes.fromhex("deadbeef")),
    ],
)
def test_typed_value_decoder_returns_native_dbapi_values(raw: bytes, dtype: str, expected: object) -> None:
    assert MssqlTypedValueDecoder(BulkTextCodec()).decode(raw, column="value", target_type=dtype) == expected


def test_typed_value_decoder_preserves_null_empty_and_codec_controls() -> None:
    codec = BulkTextCodec()
    decoder = MssqlTypedValueDecoder(codec)
    value = "a\tb\r\nc\x1d"

    assert decoder.decode(b"", column="value", target_type="nvarchar(max)") is None
    assert decoder.decode(codec.encode("").encode(), column="value", target_type="nvarchar(max)") == ""
    assert decoder.decode(codec.encode(value).encode(), column="value", target_type="nvarchar(max)") == value


@pytest.mark.parametrize(
    ("raw", "dtype", "code"),
    [
        (b"256", "tinyint", "value_out_of_range"),
        (b"1.234", "decimal(5,2)", "value_out_of_range"),
        (b"-0", "float(53)", "value_lossy"),
        (b"xyz", "varbinary(max)", "binary_hex_invalid"),
        ("яя".encode(), "varchar(10)", "ansi_text_unverifiable"),
        (b"2026-08-20 10:11:12.123456", "datetime2(3)", "value_lossy"),
        (b"2026-08-20 10:11:12.001000", "datetime", "value_lossy"),
        (b"2026-08-20 10:11:12", "smalldatetime", "value_lossy"),
        (b"5e-324", "float(53)", "value_lossy"),
        (b"aa", "binary(2)", "fixed_width_padding_loss"),
        (b"x", "nchar(2)", "fixed_width_padding_loss"),
        (b"<x />", "xml", "target_type_unsupported"),
    ],
)
def test_typed_value_decoder_fails_closed_on_lossy_values(raw: bytes, dtype: str, code: str) -> None:
    with pytest.raises(MssqlTypedValueError, match=code):
        MssqlTypedValueDecoder(BulkTextCodec()).decode(raw, column="value", target_type=dtype)


def test_datetimeoffset_preserves_one_canonical_bulk_value() -> None:
    decoder = MssqlTypedValueDecoder(BulkTextCodec())

    value = decoder.decode(
        b"2026-08-20 10:11:12.123456+00:00",
        column="created_at",
        target_type="datetimeoffset(6)",
    )

    assert value == "2026-08-20 10:11:12.123456+00:00"


def test_time_preserves_fractional_seconds_for_bulk_conversion() -> None:
    decoder = MssqlTypedValueDecoder(BulkTextCodec())

    value = decoder.decode(b"10:11:12.123456", column="clock", target_type="time(6)")

    assert value == "10:11:12.123456"


def test_length_prefixed_bcp_framing_distinguishes_null_empty_and_controls() -> None:
    handle = io.BytesIO()

    written = write_length_prefixed_row(handle, (None, "", "a\tb\r\n", b"\x00\xff"))

    payload = handle.getvalue()
    assert written == len(payload)
    assert payload == (
        struct.pack("<i", -1)
        + struct.pack("<i", 0)
        + struct.pack("<i", 5)
        + b"a\tb\r\n"
        + struct.pack("<i", 4)
        + b"00ff"
    )


class _TypedConnector:
    trust_server_certificate = "yes"
    bcp_path = "bcp"

    def __init__(self) -> None:
        self.imported_rows = 0
        self.calls: list[tuple[bytes, str]] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    @staticmethod
    def qualified_name(schema: str, table: str, *, database: str | None = None) -> str:
        return f"[{database}].[{schema}].[{table}]"

    def bcp_import_format(self, schema, table, file_path, format_path, *, options, database=None) -> int:
        del schema, table, options, database
        data = Path(file_path).read_bytes()
        format_text = Path(format_path).read_text()
        field_count = int(format_text.splitlines()[1])
        offset = 0
        fields = 0
        while offset < len(data):
            length = struct.unpack_from("<i", data, offset)[0]
            offset += 4
            if length >= 0:
                offset += length
            fields += 1
        assert offset == len(data)
        assert fields % field_count == 0
        self.imported_rows = fields // field_count
        self.calls.append((data, format_text))
        return self.imported_rows

    def get_records(self, query, params=None, as_dict=False):
        del params
        assert "COUNT_BIG" in str(query)
        return [{"row_count": self.imported_rows}] if as_dict else [(self.imported_rows,)]


class _Logger:
    def __init__(self) -> None:
        self.events = []

    def log_etl_progress(self, event, values) -> None:
        self.events.append((event, values))


def _artifact(tmp_path: Path, rows: list[tuple[str, ...]]) -> FileExportArtifact:
    path = tmp_path / "delta.bcp"
    lines = []
    for row in rows:
        payload = "\t".join(row)
        digest = hashlib.sha256(payload.encode().decode("utf-8").encode("utf-16le")).hexdigest()
        lines.append(payload + "\t" + digest + "\n")
    path.write_text("".join(lines), encoding="utf-8")
    return FileExportArtifact(
        str(path),
        ("id", "amount", "created_at", DELTA_HASH_COLUMN),
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=len(rows),
    )


def _business_artifact(tmp_path: Path, rows: list[tuple[str, ...]]) -> FileExportArtifact:
    path = tmp_path / "business.bcp"
    path.write_text("".join("\t".join(row) + "\n" for row in rows), encoding="utf-8")
    return FileExportArtifact(
        str(path),
        ("id", "amount", "created_at"),
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=len(rows),
    )


def _staging(connector: _TypedConnector, *, batch_size: int = 2) -> StagingTableArtifact:
    return StagingTableArtifact(
        database="DWH_Dev",
        schema="sample_metrics",
        table="delta_native",
        columns=("id", "amount", "created_at", DELTA_HASH_COLUMN),
        staging_manager=SimpleNamespace(),
        column_types={
            "id": "uniqueidentifier",
            "amount": "int",
            "created_at": "datetimeoffset(6)",
            DELTA_HASH_COLUMN: "char(64)",
        },
        bulk_options=BulkOptionsResolver.resolve({"bulk": {"bcp": {"batch_size": batch_size}}}),
        typed_file_ingestion=True,
    )


def test_typed_file_ingestor_reads_once_and_builds_one_length_prefixed_bulk_file(tmp_path: Path) -> None:
    connector = _TypedConnector()
    logger = _Logger()
    artifact = _artifact(
        tmp_path,
        [
            (f"00000000-0000-0000-0000-{index:012d}", str(index), "2026-08-20 10:11:12.123456+00:00")
            for index in range(1, 4)
        ],
    )
    staging = _staging(connector)

    inserted = MssqlTypedFileIngestor(connector, logger).ingest(staging, artifact)

    assert inserted == 3
    assert len(connector.calls) == 1
    binary, format_text = connector.calls[0]
    assert binary
    assert "SQLCHAR\t4" in format_text
    assert '"\\t"' not in format_text
    assert '"\\n"' not in format_text
    assert staging.typed_transport == "mssql.bcp.length_prefixed_utf8.v1"
    assert staging.typed_batch_count == 2
    assert staging.consumed_payload_evidence.require_complete(native=True).actual_native_rows == 3
    assert logger.events[0][0] == "MSSQL_TYPED_STAGING_COMPLETE"


def test_direct_native_ingestor_accepts_receipted_business_prefix_and_defers_native_evidence(
    tmp_path: Path,
) -> None:
    connector = _TypedConnector()
    artifact = _business_artifact(
        tmp_path,
        [
            (f"00000000-0000-0000-0000-{index:012d}", str(index), "2026-08-20 10:11:12.123456+00:00")
            for index in range(1, 4)
        ],
    )
    staging = _staging(connector)
    staging.columns = (*artifact.columns, "__dpone__row_hash", "__dpone__deleted_at")
    staging.column_types.update(
        {
            "__dpone__row_hash": "varchar(64)",
            "__dpone__deleted_at": "datetime2(7)",
        }
    )
    staging.target_column_types = dict(staging.column_types)
    staging.wire_schema = tuple((column, staging.column_types[column]) for column in artifact.columns)
    staging.typed_file_row_hash_validation = False
    staging.typed_file_deferred_native_evidence = True

    inserted = MssqlTypedFileIngestor(connector, _Logger()).ingest(staging, artifact)

    assert inserted == 3
    assert len(connector.calls) == 1
    _binary, format_text = connector.calls[0]
    assert int(format_text.splitlines()[1]) == len(artifact.columns)
    assert staging.consumed_payload_evidence.require_complete(native=False).actual_raw_rows == 3
    assert staging.consumed_payload_evidence.actual_native_rows is None


def test_typed_file_ingestor_rejects_row_checksum_before_binding(tmp_path: Path) -> None:
    connector = _TypedConnector()
    artifact = _artifact(
        tmp_path,
        [("00000000-0000-0000-0000-000000000001", "1", "2026-08-20 10:11:12.123456+00:00")],
    )
    data = Path(artifact.file_path).read_bytes()
    Path(artifact.file_path).write_bytes(data.replace(b"\t1\t", b"\t2\t"))
    # Re-capture only the outer byte receipt to isolate the independent row
    # checksum proof, exactly as a corrupt producer would present it.
    artifact = FileExportArtifact(
        artifact.file_path,
        artifact.columns,
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=1,
    )

    with pytest.raises(ArtifactIntegrityError, match="delta_checksum_mismatch"):
        MssqlTypedFileIngestor(connector, _Logger()).ingest(_staging(connector), artifact)

    assert connector.calls == []


def test_typed_file_ingestor_rejects_overwidth_override_before_bcp_or_target_work(tmp_path: Path) -> None:
    """Bounded text is decoded and length-checked before native BCP starts."""

    connector = _TypedConnector()
    artifact = _artifact(
        tmp_path,
        [("four", "1", "2026-08-20 10:11:12.123456+00:00")],
    )
    staging = _staging(connector)
    staging.column_types["id"] = "nvarchar(3)"

    with pytest.raises(MssqlTypedValueError, match="mssql_typed_ingest.value_too_long:id"):
        MssqlTypedFileIngestor(connector, _Logger()).ingest(staging, artifact)

    assert connector.calls == []
    assert connector.imported_rows == 0
    assert staging.consumed_payload_evidence is None


def test_typed_snapshot_config_issues_exact_native_staging_contract() -> None:
    config = LoadConfig(
        source_conn_id="pg",
        target_conn_id="ms",
        source_schema="public",
        source_table="metrics_value",
        target_database="DWH_Dev",
        target_schema="sample_metrics",
        target_table="metrics_value",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["guid"],
        options={
            "schema_contract": {
                "columns": {
                    "guid": {"nullable": False},
                    "amount": {"nullable": False},
                }
            }
        },
    )
    schema = (("guid", "uniqueidentifier"), ("amount", "int"), (DELTA_HASH_COLUMN, "char(64)"))

    resolved = typed_snapshot_staging_config(config, schema, unique_key=("guid",))

    assert resolved.options["__dpone_mssql_typed_file_staging_v1"] is True
    assert resolved.options["__dpone_snapshot_native_staging"] is True
    assert resolved.options["__dpone_snapshot_native_column_types"] == dict(schema)
    assert resolved.options["__dpone_snapshot_native_not_null_columns"] == [
        DELTA_HASH_COLUMN,
        "amount",
        "guid",
    ]
