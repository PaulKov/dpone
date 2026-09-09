"""Independent source/target vectors for every currently admitted native type family.

Temporal vectors describe the current explicit dpone wire profile, not a captured
exporter/version certification. Native varchar vectors use ASCII-safe characters.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.native_acceleration import NativeAccelerationRegistry
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder

# SQL type, native prefix width (NOT NULL), payload, target type, target payload.
VECTORS = [
    ("bit", 1, "01", "Bool", "01"),
    ("tinyint", 0, "ff", "UInt8", "ff"),
    ("smallint", 0, "0080", "Int16", "0080"),
    ("int", 0, "00000080", "Int32", "00000080"),
    ("bigint", 0, "0000000000000080", "Int64", "0000000000000080"),
    ("real", 0, "0000c03f", "Float32", "0000c03f"),
    ("float(24)", 0, "0000c03f", "Float64", "000000000000f83f"),
    ("float(53)", 0, "000000000000f83f", "Float64", "000000000000f83f"),
    ("float", 0, "000000000000f83f", "Float64", "000000000000f83f"),
    ("money", 0, "ffffffffffffffff", "Decimal(19,4)", "ff" * 16),
    ("smallmoney", 0, "ffffffff", "Decimal(10,4)", "ff" * 8),
    ("decimal(10)", 1, "0a000139300000000000000000000000000000", "Decimal(10,0)", "3930000000000000"),
    ("numeric(10,2)", 1, "0a020039300000000000000000000000000000", "Decimal(10,2)", "c7cfffffffffffff"),
    ("uniqueidentifier", 1, "33221100554477668899aabbccddeeff", "UUID", "7766554433221100ffeeddccbbaa9988"),
    ("char(4)", 2, "41202020", "String", "0441202020"),
    ("varchar(5)", 2, "4100090a5c", "String", "054100090a5c"),
    ("varchar(max)", 8, "4100090a5c", "String", "054100090a5c"),
    ("nchar(3)", 2, "410020002000", "String", "03412020"),
    ("nvarchar(8)", 2, "4100e9003dd800de", "String", "0741c3a9f09f9880"),
    ("nvarchar(max)", 8, "4100e9003dd800de", "String", "0741c3a9f09f9880"),
    ("binary(3)", 2, "00ff41", "String", "0300ff41"),
    ("varbinary(3)", 2, "00ff41", "String", "0300ff41"),
    ("varbinary(max)", 8, "00ff41", "String", "0300ff41"),
    ("date", 0, "3af90a", "Date", "0000"),
    ("datetime", 0, "df63000002000000", "DateTime64(3)", "0700000000000000"),
    ("smalldatetime", 0, "df630100", "DateTime64(0)", "3c00000000000000"),
    ("time(7)", 0, "8196980000", "String", "1030303a30303a30312e30303030303031"),
    ("datetime2(3)", 0, "c0741d2ac939f90a", "DateTime64(3)", "0cfeffffffffffff"),
    ("datetime2(7)", 0, "81969800003af90a", "DateTime64(7)", "8196980000000000"),
    ("datetimeoffset(7)", 0, "81969800003af90ab400", "DateTime64(7)", "8196980000000000"),
]


# One second plus one fractional unit, covering every temporal scale/width.
# The calendar bytes identify 1970-01-01; the target is an independent integer.
for _scale, _payload, _ticks in [
    (0, "8096980000", "0100000000000000"),
    (1, "c0d8a70000", "0b00000000000000"),
    (2, "201d9a0000", "6500000000000000"),
    (3, "90bd980000", "e903000000000000"),
    (4, "689a980000", "1127000000000000"),
    (5, "e496980000", "a186010000000000"),
    (6, "8a96980000", "41420f0000000000"),
    (7, "8196980000", "8196980000000000"),
]:
    for _kind, _suffix in [("datetime2", "3af90a"), ("datetimeoffset", "3af90a0000")]:
        VECTORS.append((f"{_kind}({_scale})", 0, _payload + _suffix, f"DateTime64({_scale})", _ticks))
    _text = "00:00:01" + ("." + "0" * (_scale - 1) + "1" if _scale else "")
    VECTORS.append((f"time({_scale})", 0, _payload, "String", (bytes([len(_text)]) + _text.encode()).hex()))


def _ch_string(value: str) -> bytes:
    raw = value.encode()
    assert len(raw) < 128
    return bytes([len(raw)]) + raw


def _target_default(target: str, payload: bytes) -> bytes:
    return b"\x00" if target == "String" else bytes(len(payload))


@pytest.mark.parametrize("dtype,prefix,source_hex,target,target_hex", VECTORS)
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
def test_all_types_through_actual_transcoder(
    tmp_path: Path, dtype, prefix, source_hex, target, target_hex, nullable, backend
):
    schema = [("before", "int"), ("value", dtype + (" nullable" if nullable else "")), ("after", "int")]
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    if dtype.startswith("char(") and not nullable:
        assert contract.blockers
        with pytest.raises(ValueError):
            NativeWireTranscoder().to_clickhouse_binary(
                SourceNativeArtifact(tmp_path / "absent", columns=[], native_wire_contract=contract), schema
            )
        return
    source = bytes.fromhex(source_hex)
    prefix = max(prefix, int(nullable))
    framed = (len(source).to_bytes(prefix, "little") if prefix else b"") + source
    before, after = bytes.fromhex("04030201"), bytes.fromhex("08070605")
    wire = (before + framed + after) * 2
    if nullable:
        wire += before + b"\xff" * prefix + after
    path = tmp_path / "matrix.bcp"
    path.write_bytes(wire)
    artifact = SourceNativeArtifact(path, columns=[name for name, _ in schema], native_wire_contract=contract)
    registry = None if backend == "accelerated" else NativeAccelerationRegistry(module_loader=lambda: None)
    if backend == "accelerated":
        pytest.importorskip("dpone_native_accel")
    target_type = f"Nullable({target})" if nullable else target
    target_schema = [("before", "Int32"), ("value", target_type), ("after", "Int32")]
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(
        artifact, schema, clickhouse_schema=target_schema
    )
    data = b"".join(stream.iter_bytes())
    cell = bytes.fromhex(target_hex)
    if fmt == "RowBinary":
        expected = (before + (b"\x00" if nullable else b"") + cell + after) * 2
        if nullable:
            expected += before + b"\x01" + after
    else:
        rows = 3 if nullable else 2
        expected = bytes([3, rows]) + _ch_string("before") + _ch_string("Int32") + before * rows
        expected += _ch_string("value") + _ch_string(target_type)
        expected += (b"\x00\x00\x01" if nullable else b"") + cell * 2
        expected += _target_default(target, cell) if nullable else b""
        expected += _ch_string("after") + _ch_string("Int32") + after * rows
    assert data == expected
    assert stream.native_wire_evidence.rows == (3 if nullable else 2)
    if backend == "accelerated":
        assert stream.native_acceleration_evidence.selected_backend == "native_accelerated"


def test_every_admitted_type_family_has_independent_target_vectors():
    from dpone.runtime.native_wire_mssql_framing import _FIXED_WIDTHS

    covered = {row[0].split("(", 1)[0] for row in VECTORS}
    assert set(_FIXED_WIDTHS) <= covered
    assert {
        "char",
        "varchar",
        "nchar",
        "nvarchar",
        "binary",
        "varbinary",
        "time",
        "datetime2",
        "datetimeoffset",
    } <= covered


@pytest.mark.parametrize(
    "dtype", ["xml", "sql_variant", "geography", "hierarchyid", "timestamp", "text", "ntext", "image"]
)
def test_unimplemented_type_never_enters_reader(dtype):
    from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder

    contract = build_mssql_bcp_native_contract(schema=[("value", dtype)], query="SELECT synthetic")
    assert contract.blockers
    with pytest.raises(ValueError):
        MssqlBcpNativeDecoder(contract)


@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
@pytest.mark.parametrize(
    "schema",
    [
        [("b", "int"), ("a", "int")],
        [("a", "int")],
        [("a", "int"), ("c", "int")],
        [("a", "bigint"), ("b", "int")],
    ],
)
def test_source_schema_bound_before_file_access(tmp_path, backend, schema):
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(
        schema=[("a", "int"), ("b", "int")], query="SELECT synthetic", target_format=fmt
    )
    artifact = SourceNativeArtifact(tmp_path / "absent.bcp", columns=["a", "b"], native_wire_contract=contract)
    registry = None if backend == "accelerated" else NativeAccelerationRegistry(module_loader=lambda: None)
    with pytest.raises(ValueError, match="native_wire_source_schema_mismatch"):
        NativeWireTranscoder(registry=registry).to_clickhouse_binary(
            artifact, schema, clickhouse_schema=[("a", "Int32"), ("b", "Int32")]
        )


@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
@pytest.mark.parametrize("policy,expected", [("hex", b"00ff41"), ("base64", b"AP9B")])
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("overflow", [False, True])
def test_fixed_binary_policy(tmp_path, backend, policy, expected, nullable, overflow):
    from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy

    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    schema = [("value", "varbinary(3)" + (" nullable" if nullable else ""))]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    path = tmp_path / "binary.bcp"
    path.write_bytes(bytes.fromhex("030000ff41") + (b"\xff\xff" if nullable else b""))
    artifact = SourceNativeArtifact(path, columns=["value"], native_wire_contract=contract)
    registry = None if backend == "accelerated" else NativeAccelerationRegistry(module_loader=lambda: None)
    width = len(expected) - int(overflow)
    target = f"FixedString({width})"
    target = f"Nullable({target})" if nullable else target
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(
        artifact,
        schema,
        clickhouse_schema=[("value", target)],
        type_policy=MssqlClickHouseTypePolicy(binary_encoding=policy),
    )
    if overflow:
        with pytest.raises(ValueError):
            b"".join(stream.iter_bytes())
        return
    if fmt == "RowBinary":
        golden = b"\x00" + expected + b"\x01" if nullable else expected
    else:
        golden = bytes([1, 2 if nullable else 1]) + _ch_string("value") + _ch_string(target)
        golden += b"\x00\x01" + expected + bytes(width) if nullable else expected
    assert b"".join(stream.iter_bytes()) == golden
    if backend == "accelerated":
        assert stream.native_acceleration_evidence.selected_backend == "native_accelerated"


@pytest.mark.parametrize("schema", [[], [("other", "int")], [("value", "bigint")]])
def test_standalone_provider_rejects_explicit_conflicting_schema(schema):
    provider = pytest.importorskip("dpone_native_accel._mssql_clickhouse_native")
    contract = build_mssql_bcp_native_contract(
        schema=[("value", "int")], query="SELECT synthetic", target_format="Native"
    )
    with pytest.raises(ValueError, match="native_wire_source_schema_mismatch"):
        provider.MssqlBcpClickHouseNativeBackend.from_request(
            {
                "artifact_path": "absent.bcp",
                "native_wire_contract": contract.to_dict(),
                "schema": schema,
                "bulk_wire_contract": {},
            }
        )


@pytest.mark.parametrize("supplied", [False, True])
def test_standalone_provider_accepts_optional_matching_schema(supplied):
    provider = pytest.importorskip("dpone_native_accel._mssql_clickhouse_native")
    schema = [("value", "int")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format="Native")
    request = {"artifact_path": "absent.bcp", "native_wire_contract": contract.to_dict(), "bulk_wire_contract": {}}
    if supplied:
        request["schema"] = schema
    assert provider.MssqlBcpClickHouseNativeBackend.from_request(request)
