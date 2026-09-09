from __future__ import annotations

import struct
import uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pytest

from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.native_acceleration import NativeAccelerationRegistry
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder
from dpone.runtime.support.temporal_fidelity import TemporalFidelityPolicy
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy

dpone_native_accel = pytest.importorskip("dpone_native_accel")


def test_native_accel_provider_declares_certified_mssql_clickhouse_native_backend() -> None:
    capabilities = dpone_native_accel.capabilities()
    backends = capabilities["backends"]

    assert callable(dpone_native_accel.transcode)
    assert callable(dpone_native_accel.transcode_batches)
    assert capabilities["schema_version"] == "dpone.native_transfer.acceleration.v1"
    assert backends
    assert backends[0]["backend_id"] == "mssql_bcp_native_to_clickhouse_native"
    assert backends[0]["certified"] is True
    assert "int" in backends[0]["supported_types"]
    assert "time" in backends[0]["supported_types"]
    assert "datetimeoffset" in backends[0]["supported_types"]


def test_native_accel_provider_matches_python_reference_for_clickhouse_native(tmp_path: Path) -> None:
    reference_artifact = _artifact(tmp_path, "reference.bcp")
    accelerated_artifact = _artifact(tmp_path, "accelerated.bcp")
    accelerated_artifact._estimated_rows = 1
    schema = [("id", "int nullable")]
    target_schema = [("id", "Nullable(Int32)")]

    reference = NativeWireTranscoder(
        registry=NativeAccelerationRegistry(module_loader=lambda: None)
    ).to_clickhouse_binary(
        reference_artifact,
        schema,
        clickhouse_schema=target_schema,
    )
    accelerated = NativeWireTranscoder().to_clickhouse_binary(
        accelerated_artifact,
        schema,
        clickhouse_schema=target_schema,
    )

    assert b"".join(accelerated.iter_bytes()) == b"".join(reference.iter_bytes())
    assert accelerated.native_acceleration_evidence.selected_backend == "native_accelerated"
    assert accelerated.native_acceleration_evidence.rows == 2


def test_native_accel_provider_matches_python_reference_for_unicode_text(tmp_path: Path) -> None:
    reference_artifact = _text_artifact(tmp_path, "reference-text.bcp")
    accelerated_artifact = _text_artifact(tmp_path, "accelerated-text.bcp")
    schema = [("payload", "nvarchar(100) nullable")]
    target_schema = [("payload", "Nullable(String)")]

    reference = NativeWireTranscoder(
        registry=NativeAccelerationRegistry(module_loader=lambda: None)
    ).to_clickhouse_binary(
        reference_artifact,
        schema,
        clickhouse_schema=target_schema,
    )
    accelerated = NativeWireTranscoder().to_clickhouse_binary(
        accelerated_artifact,
        schema,
        clickhouse_schema=target_schema,
    )

    assert b"".join(accelerated.iter_bytes()) == b"".join(reference.iter_bytes())
    assert accelerated.native_acceleration_evidence.selected_backend == "native_accelerated"


def test_native_accel_provider_matches_python_reference_for_temporal_decimal_uuid_binary(tmp_path: Path) -> None:
    reference_artifact = _mixed_artifact(tmp_path, "reference-mixed.bcp")
    accelerated_artifact = _mixed_artifact(tmp_path, "accelerated-mixed.bcp")
    schema = [
        ("amount", "decimal(10,2) nullable"),
        ("d", "date nullable"),
        ("ts", "datetime2(7) nullable"),
        ("guid", "uniqueidentifier nullable"),
        ("raw", "varbinary(10) nullable"),
        ("ratio", "float"),
    ]
    target_schema = [
        ("amount", "Nullable(Decimal(10, 2))"),
        ("d", "Nullable(Date)"),
        ("ts", "Nullable(DateTime64(7))"),
        ("guid", "Nullable(UUID)"),
        ("raw", "Nullable(String)"),
        ("ratio", "Float64"),
    ]
    type_policy = MssqlClickHouseTypePolicy(binary_encoding="hex")

    reference = NativeWireTranscoder(
        registry=NativeAccelerationRegistry(module_loader=lambda: None)
    ).to_clickhouse_binary(
        reference_artifact,
        schema,
        clickhouse_schema=target_schema,
        type_policy=type_policy,
    )
    accelerated = NativeWireTranscoder().to_clickhouse_binary(
        accelerated_artifact,
        schema,
        clickhouse_schema=target_schema,
        type_policy=type_policy,
    )

    assert b"".join(accelerated.iter_bytes()) == b"".join(reference.iter_bytes())
    assert accelerated.native_acceleration_evidence.selected_backend == "native_accelerated"


def test_native_accel_provider_matches_reference_for_all_temporal_target_families(tmp_path: Path) -> None:
    reference_artifact = _legacy_temporal_artifact(
        tmp_path,
        "reference-legacy-temporal.bcp",
        acceleration_mode="auto",
    )
    accelerated_artifact = _legacy_temporal_artifact(
        tmp_path,
        "accelerated-legacy-temporal.bcp",
        acceleration_mode="required",
    )
    schema = [
        ("d", "date nullable"),
        ("d32", "date nullable"),
        ("dt", "datetime2(0) nullable"),
        ("dt64", "datetime2(7) nullable"),
        ("legacy_dt", "datetime nullable"),
        ("rounded_dt", "smalldatetime nullable"),
        ("time_seconds", "time(7) nullable"),
        ("fixed", "char(4) nullable"),
    ]
    target_schema = [
        ("d", "Nullable(Date)"),
        ("d32", "Nullable(Date32)"),
        ("dt", "Nullable(DateTime)"),
        ("dt64", "Nullable(DateTime64(7))"),
        ("legacy_dt", "Nullable(DateTime)"),
        ("rounded_dt", "Nullable(DateTime64(0))"),
        ("time_seconds", "Nullable(UInt32)"),
        ("fixed", "Nullable(FixedString(4))"),
    ]

    reference = NativeWireTranscoder(
        registry=NativeAccelerationRegistry(module_loader=lambda: None)
    ).to_clickhouse_binary(
        reference_artifact,
        schema,
        clickhouse_schema=target_schema,
    )
    accelerated = NativeWireTranscoder().to_clickhouse_binary(
        accelerated_artifact,
        schema,
        clickhouse_schema=target_schema,
    )

    assert b"".join(accelerated.iter_bytes()) == b"".join(reference.iter_bytes())
    assert accelerated.native_acceleration_evidence.selected_backend == "native_accelerated"
    assert accelerated.native_acceleration_evidence.rows == 2


@pytest.mark.parametrize(
    ("target_type", "policy"),
    [
        ("Nullable(DateTime64(7, 'UTC'))", MssqlClickHouseTypePolicy()),
        (
            "Nullable(String)",
            MssqlClickHouseTypePolicy(temporal=TemporalFidelityPolicy(offset_timestamp_mode="preserve_text")),
        ),
    ],
)
def test_native_accel_provider_matches_reference_for_datetimeoffset(
    tmp_path: Path,
    target_type: str,
    policy: MssqlClickHouseTypePolicy,
) -> None:
    reference_artifact = _datetimeoffset_artifact(tmp_path, "reference-datetimeoffset.bcp", "auto")
    accelerated_artifact = _datetimeoffset_artifact(tmp_path, "accelerated-datetimeoffset.bcp", "required")
    schema = [("offset_at", "datetimeoffset(7) nullable")]
    target_schema = [("offset_at", target_type)]

    reference = NativeWireTranscoder(
        registry=NativeAccelerationRegistry(module_loader=lambda: None)
    ).to_clickhouse_binary(reference_artifact, schema, clickhouse_schema=target_schema, type_policy=policy)
    accelerated = NativeWireTranscoder().to_clickhouse_binary(
        accelerated_artifact,
        schema,
        clickhouse_schema=target_schema,
        type_policy=policy,
    )

    assert b"".join(accelerated.iter_bytes()) == b"".join(reference.iter_bytes())
    assert accelerated.native_acceleration_evidence.selected_backend == "native_accelerated"


@pytest.mark.parametrize(
    "target_type",
    [
        "LowCardinality(Nullable(String))",
        "Nullable(LowCardinality(FixedString(2)))",
    ],
)
def test_native_and_reference_encoders_fail_closed_for_low_cardinality(
    tmp_path: Path,
    target_type: str,
) -> None:
    schema = [("payload", "nvarchar(100) nullable")]
    target_schema = [("payload", target_type)]
    reference_artifact = _text_artifact(tmp_path, "reference-low-cardinality.bcp")
    accelerated_artifact = _text_artifact(tmp_path, "accelerated-low-cardinality.bcp")

    reference = NativeWireTranscoder(
        registry=NativeAccelerationRegistry(module_loader=lambda: None)
    ).to_clickhouse_binary(reference_artifact, schema, clickhouse_schema=target_schema)
    accelerated = NativeWireTranscoder().to_clickhouse_binary(
        accelerated_artifact,
        schema,
        clickhouse_schema=target_schema,
    )

    with pytest.raises(ValueError, match="LowCardinality Native encoding is not supported"):
        b"".join(reference.iter_bytes())
    with pytest.raises(ValueError, match="native_acceleration_unsupported_clickhouse_type"):
        b"".join(accelerated.iter_bytes())


@pytest.mark.parametrize(
    "target_type",
    ["Array(String)", "Enum8('red' = 1, 'green' = 2)"],
)
def test_native_accel_provider_fails_closed_for_unsupported_complex_targets(
    tmp_path: Path,
    target_type: str,
) -> None:
    from dpone_native_accel._mssql_clickhouse_native import ColumnLayout, MssqlBcpClickHouseNativeBackend

    path = tmp_path / "unsupported-complex.bcp"
    value = "ab".encode("utf-16le")
    path.write_bytes(len(value).to_bytes(2, byteorder="little", signed=True) + value)
    backend = MssqlBcpClickHouseNativeBackend(
        artifact_path=path,
        columns=[
            ColumnLayout(
                name="value",
                storage_type="nvarchar",
                clickhouse_type=target_type,
                nullable=False,
                prefix_width=2,
                fixed_length=None,
                precision=None,
                scale=None,
                encoding="utf-16le",
            )
        ],
        block_rows=1,
        block_bytes=None,
    )

    with pytest.raises(ValueError, match="native_acceleration_unsupported_clickhouse_type"):
        list(backend.iter_blocks())


def test_native_accel_provider_rejects_date_and_datetime_out_of_range() -> None:
    """Date/DateTime must fail loud on UInt16/UInt32 overflow — never wrap."""

    from dpone_native_accel._mssql_clickhouse_native import ColumnLayout, _encode_date, _encode_datetime

    with pytest.raises(struct.error):
        _encode_date(_bcp_date(date(1900, 1, 1)), "date")

    payload = _bcp_datetimeoffset(
        datetime(1969, 12, 31, 23, 59, 59),
        scale=7,
        final_digit=0,
        offset_minutes=0,
    )
    column = ColumnLayout(
        name="ts",
        storage_type="datetimeoffset",
        clickhouse_type="DateTime",
        nullable=False,
        prefix_width=0,
        fixed_length=10,
        precision=None,
        scale=7,
        encoding="utf-8",
    )
    with pytest.raises(struct.error):
        _encode_datetime(payload, column, "DateTime")


def test_native_accel_provider_rejects_fixedstring_oversize() -> None:
    from dpone_native_accel._mssql_clickhouse_native import ColumnLayout, _encode_fixed_string

    column = ColumnLayout(
        name="fs",
        storage_type="varchar",
        clickhouse_type="FixedString(2)",
        nullable=False,
        prefix_width=2,
        fixed_length=None,
        precision=None,
        scale=None,
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="native_acceleration_fixed_string_oversize"):
        _encode_fixed_string(b"abc", column, "FixedString(2)")


@pytest.mark.parametrize(
    ("local_value", "offset_minutes"),
    [
        (datetime(2026, 1, 1, 0, 30, 0, 123456), 180),
        (datetime(2026, 12, 31, 23, 30, 0, 654321), -240),
    ],
)
def test_datetimeoffset_bcp_layout_round_trips_across_utc_day_boundaries(
    local_value: datetime,
    offset_minutes: int,
) -> None:
    from dpone_native_accel._mssql_clickhouse_native import ColumnLayout, _datetimeoffset_text

    from dpone.runtime.native_wire_mssql import _decode_datetimeoffset

    payload = _bcp_datetimeoffset(
        local_value,
        scale=7,
        final_digit=7,
        offset_minutes=offset_minutes,
    )
    column = ColumnLayout(
        name="offset_at",
        storage_type="datetimeoffset",
        clickhouse_type="String",
        nullable=False,
        prefix_width=0,
        fixed_length=10,
        precision=None,
        scale=7,
        encoding="utf-8",
    )

    decoded = _decode_datetimeoffset(payload, 7)
    sign = "+" if offset_minutes >= 0 else "-"
    hours, minutes = divmod(abs(offset_minutes), 60)
    expected = f"{local_value:%Y-%m-%d %H:%M:%S}.{local_value.microsecond:06d}7 {sign}{hours:02d}:{minutes:02d}"

    assert _datetimeoffset_text(payload, column) == expected
    assert decoded.replace(tzinfo=None) == local_value
    assert decoded.utcoffset() == timedelta(minutes=offset_minutes)
    assert decoded.submicrosecond_100ns == 7


def _artifact(tmp_path: Path, name: str) -> SourceNativeArtifact:
    path = tmp_path / name
    path.write_bytes(
        b"".join(
            [
                (4).to_bytes(1, byteorder="little", signed=True),
                struct.pack("<i", 42),
                (-1).to_bytes(1, byteorder="little", signed=True),
            ]
        )
    )
    schema = [("id", "int nullable")]
    source_options = {
        "native_transfer": {
            "wire": {
                "mode": "typed_binary",
                "source_native_format": "bcp_native",
                "binary_format": "native",
                "acceleration": {"mode": "auto"},
            }
        }
    }
    artifact = SourceNativeArtifact(
        path,
        columns=["id"],
        estimated_rows=2,
        native_wire_contract=build_mssql_bcp_native_contract(
            schema=schema,
            query="SELECT [id] FROM [dbo].[orders]",
            target_format="Native",
        ),
        bulk_wire_contract=BulkWirePlanner().plan(
            source_type="mssql",
            sink_type="clickhouse",
            schema=schema,
            source_options=source_options,
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        ),
    )
    return artifact


def _text_artifact(tmp_path: Path, name: str) -> SourceNativeArtifact:
    path = tmp_path / name
    value = "Привет\tClickHouse".encode("utf-16le")
    path.write_bytes(len(value).to_bytes(2, byteorder="little", signed=True) + value)
    schema = [("payload", "nvarchar(100) nullable")]
    source_options = {
        "native_transfer": {
            "wire": {
                "mode": "typed_binary",
                "source_native_format": "bcp_native",
                "binary_format": "native",
                "acceleration": {"mode": "auto"},
            }
        }
    }
    return SourceNativeArtifact(
        path,
        columns=["payload"],
        estimated_rows=1,
        native_wire_contract=build_mssql_bcp_native_contract(
            schema=schema,
            query="SELECT [payload] FROM [dbo].[orders]",
            target_format="Native",
        ),
        bulk_wire_contract=BulkWirePlanner().plan(
            source_type="mssql",
            sink_type="clickhouse",
            schema=schema,
            source_options=source_options,
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        ),
    )


def _mixed_artifact(tmp_path: Path, name: str) -> SourceNativeArtifact:
    path = tmp_path / name
    guid = uuid.UUID("12345678-1234-5678-9abc-def012345678")
    schema = [
        ("amount", "decimal(10,2) nullable"),
        ("d", "date nullable"),
        ("ts", "datetime2(7) nullable"),
        ("guid", "uniqueidentifier nullable"),
        ("raw", "varbinary(10) nullable"),
        ("ratio", "float"),
    ]
    path.write_bytes(
        b"".join(
            [
                (19).to_bytes(1, byteorder="little", signed=True),
                bytes([10, 2, 1]) + (12345).to_bytes(16, byteorder="little", signed=False),
                (3).to_bytes(1, byteorder="little", signed=True),
                _bcp_date(date(2026, 6, 22)),
                (8).to_bytes(1, byteorder="little", signed=True),
                _bcp_datetime2(datetime(2026, 6, 22, 13, 14, 15, 123456), scale=7),
                (16).to_bytes(1, byteorder="little", signed=True),
                guid.bytes_le,
                (2).to_bytes(2, byteorder="little", signed=True),
                b"\x00\xff",
                struct.pack("<d", 1.5),
            ]
        )
    )
    source_options = {
        "native_transfer": {
            "wire": {
                "mode": "typed_binary",
                "source_native_format": "bcp_native",
                "binary_format": "native",
                "acceleration": {"mode": "auto"},
            }
        }
    }
    return SourceNativeArtifact(
        path,
        columns=[name for name, _ in schema],
        estimated_rows=1,
        native_wire_contract=build_mssql_bcp_native_contract(
            schema=schema,
            query="SELECT [amount], [d], [ts], [guid], [raw], [ratio] FROM [dbo].[orders]",
            target_format="Native",
        ),
        bulk_wire_contract=BulkWirePlanner().plan(
            source_type="mssql",
            sink_type="clickhouse",
            schema=schema,
            source_options=source_options,
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        ),
    )


def _legacy_temporal_artifact(
    tmp_path: Path,
    name: str,
    *,
    acceleration_mode: str,
) -> SourceNativeArtifact:
    path = tmp_path / name
    source_date = date(1900, 1, 1)
    source_datetime = datetime(2026, 6, 22, 13, 14, 15)
    schema = [
        ("d", "date nullable"),
        ("d32", "date nullable"),
        ("dt", "datetime2(0) nullable"),
        ("dt64", "datetime2(7) nullable"),
        ("legacy_dt", "datetime nullable"),
        ("rounded_dt", "smalldatetime nullable"),
        ("time_seconds", "time(7) nullable"),
        ("fixed", "char(4) nullable"),
    ]
    path.write_bytes(
        b"".join(
            [
                (3).to_bytes(1, byteorder="little", signed=True),
                _bcp_date(date(2026, 6, 22)),
                (3).to_bytes(1, byteorder="little", signed=True),
                _bcp_date(source_date),
                (6).to_bytes(1, byteorder="little", signed=True),
                _bcp_datetime2(source_datetime, scale=0),
                (8).to_bytes(1, byteorder="little", signed=True),
                _bcp_datetime2_100ns(source_datetime.replace(microsecond=123456), final_digit=7),
                (8).to_bytes(1, byteorder="little", signed=True),
                _bcp_datetime(source_datetime),
                (4).to_bytes(1, byteorder="little", signed=True),
                _bcp_smalldatetime(source_datetime),
                (5).to_bytes(1, byteorder="little", signed=True),
                _bcp_time(time(12, 34, 56, 123456), scale=7),
                (4).to_bytes(2, byteorder="little", signed=True),
                b"xy  ",
                (-1).to_bytes(1, byteorder="little", signed=True),
                (-1).to_bytes(1, byteorder="little", signed=True),
                (-1).to_bytes(1, byteorder="little", signed=True),
                (-1).to_bytes(1, byteorder="little", signed=True),
                (-1).to_bytes(1, byteorder="little", signed=True),
                (-1).to_bytes(1, byteorder="little", signed=True),
                (-1).to_bytes(1, byteorder="little", signed=True),
                (-1).to_bytes(2, byteorder="little", signed=True),
            ]
        )
    )
    source_options = {
        "native_transfer": {
            "wire": {
                "mode": "typed_binary",
                "source_native_format": "bcp_native",
                "binary_format": "native",
                "acceleration": {"mode": acceleration_mode},
            }
        }
    }
    return SourceNativeArtifact(
        path,
        columns=[name for name, _ in schema],
        estimated_rows=2,
        native_wire_contract=build_mssql_bcp_native_contract(
            schema=schema,
            query=(
                "SELECT [d], [d32], [dt], [dt64], [legacy_dt], [rounded_dt], [time_seconds], [fixed] "
                "FROM [dbo].[orders]"
            ),
            target_format="Native",
        ),
        bulk_wire_contract=BulkWirePlanner().plan(
            source_type="mssql",
            sink_type="clickhouse",
            schema=schema,
            source_options=source_options,
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        ),
    )


def _datetimeoffset_artifact(tmp_path: Path, name: str, acceleration_mode: str) -> SourceNativeArtifact:
    path = tmp_path / name
    value = datetime(2026, 6, 9, 12, 30, 0, 123456)
    schema = [("offset_at", "datetimeoffset(7) nullable")]
    path.write_bytes(
        (10).to_bytes(1, byteorder="little", signed=True)
        + _bcp_datetimeoffset(value, scale=7, final_digit=7, offset_minutes=180)
    )
    source_options = {
        "native_transfer": {
            "wire": {
                "mode": "typed_binary",
                "source_native_format": "bcp_native",
                "binary_format": "native",
                "acceleration": {"mode": acceleration_mode},
            }
        }
    }
    return SourceNativeArtifact(
        path,
        columns=["offset_at"],
        estimated_rows=1,
        native_wire_contract=build_mssql_bcp_native_contract(
            schema=schema,
            query="SELECT [offset_at] FROM [dbo].[orders]",
            target_format="Native",
            type_policy=MssqlClickHouseTypePolicy(),
        ),
        bulk_wire_contract=BulkWirePlanner().plan(
            source_type="mssql",
            sink_type="clickhouse",
            schema=schema,
            source_options=source_options,
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        ),
    )


def _bcp_date(value: date) -> bytes:
    return (value - date(1, 1, 1)).days.to_bytes(3, byteorder="little", signed=False)


def _bcp_datetime2(value: datetime, *, scale: int) -> bytes:
    midnight_ticks = int.from_bytes(_bcp_time(value.time(), scale=scale), byteorder="little", signed=False)
    time_length = 3 if scale <= 2 else 4 if scale <= 4 else 5
    return midnight_ticks.to_bytes(time_length, byteorder="little", signed=False) + _bcp_date(value.date())


def _bcp_datetime2_100ns(value: datetime, *, final_digit: int) -> bytes:
    ticks = int.from_bytes(_bcp_time(value.time(), scale=7), byteorder="little", signed=False) + final_digit
    return ticks.to_bytes(5, byteorder="little", signed=False) + _bcp_date(value.date())


def _bcp_datetimeoffset(
    value: datetime,
    *,
    scale: int,
    final_digit: int,
    offset_minutes: int,
) -> bytes:
    encoded_utc = _bcp_datetime2(value - timedelta(minutes=offset_minutes), scale=scale)
    ticks = int.from_bytes(encoded_utc[:5], byteorder="little", signed=False) + final_digit
    return ticks.to_bytes(5, byteorder="little", signed=False) + encoded_utc[5:] + struct.pack("<h", offset_minutes)


def _bcp_time(value: time, *, scale: int) -> bytes:
    ticks_per_second = 10**scale
    whole_seconds = value.hour * 3600 + value.minute * 60 + value.second
    if scale >= 6:
        ticks = whole_seconds * ticks_per_second + value.microsecond * (10 ** (scale - 6))
    else:
        ticks = whole_seconds * ticks_per_second + value.microsecond // (10 ** (6 - scale))
    time_length = 3 if scale <= 2 else 4 if scale <= 4 else 5
    return ticks.to_bytes(time_length, byteorder="little", signed=False)


def _bcp_datetime(value: datetime) -> bytes:
    days = (value.date() - date(1900, 1, 1)).days
    seconds = value.hour * 3600 + value.minute * 60 + value.second
    return struct.pack("<ii", days, seconds * 300)


def _bcp_smalldatetime(value: datetime) -> bytes:
    days = (value.date() - date(1900, 1, 1)).days
    minutes = value.hour * 60 + value.minute
    return struct.pack("<HH", days, minutes)
