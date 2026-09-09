"""Independent native BCP bytes, never encoded with the production layout helper."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from dpone.runtime.native_acceleration import NativeAccelerationRegistry
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_models import stable_hash
from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder, build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder

# Microsoft native prefix table requires 1 for UUID/decimal/numeric, including NOT NULL.
# Decimal bodies below explicitly use the existing admitted 19-byte native representation.
_GOLDENS = (
    (
        "uniqueidentifier",
        "10 33221100554477668899aabbccddeeff",
        "00112233-4455-6677-8899-aabbccddeeff",
        "UUID",
        "7766554433221100ffeeddccbbaa9988",
    ),
    (
        "decimal(10,2)",
        "13 0a0201 39300000000000000000000000000000",
        Decimal("123.45"),
        "Decimal(10, 2)",
        "3930000000000000",
    ),
    (
        "numeric(10,2)",
        "13 0a0200 39300000000000000000000000000000",
        Decimal("-123.45"),
        "Decimal(10, 2)",
        "c7cfffffffffffff",
    ),
)
_SENTINEL = bytes.fromhex("04030201")


@pytest.mark.parametrize(("dtype", "golden", "expected", "target", "encoded"), _GOLDENS)
@pytest.mark.parametrize("nullable", [False, True])
def test_native_prefix_preserves_two_rows_and_adjacent_values(
    tmp_path, dtype, golden, expected, target, encoded, nullable
):
    schema = [("value", dtype + (" nullable" if nullable else "")), ("sentinel", "int")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic")
    assert contract.columns[0].prefix_width == 1
    path = tmp_path / "sample.bcp"
    wire = bytes.fromhex(golden) + _SENTINEL
    path.write_bytes(wire * 2 + (b"\xff" + _SENTINEL if nullable else b""))
    expected_rows = [{"value": expected, "sentinel": 16909060}] * 2
    if nullable:
        expected_rows.append({"value": None, "sentinel": 16909060})
    assert list(MssqlBcpNativeDecoder(contract).iter_rows(path)) == expected_rows


@pytest.mark.parametrize(("dtype", "golden", "expected", "target", "encoded"), _GOLDENS)
@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
def test_actual_transcoder_matches_independent_target_bytes(
    tmp_path, dtype, golden, expected, target, encoded, backend
):
    schema = [("value", dtype), ("sentinel", "int")]
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    path = tmp_path / "sample.bcp"
    path.write_bytes((bytes.fromhex(golden) + _SENTINEL) * 2)
    artifact = SourceNativeArtifact(path, columns=["value", "sentinel"], native_wire_contract=contract)
    registry = NativeAccelerationRegistry(module_loader=lambda: None) if backend != "accelerated" else None
    if backend == "accelerated":
        pytest.importorskip("dpone_native_accel")
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(
        artifact, schema, clickhouse_schema=[("value", target), ("sentinel", "Int32")]
    )
    payload = b"".join(stream.iter_bytes())
    value_bytes = bytes.fromhex(encoded)
    if fmt == "RowBinary":
        assert payload == (value_bytes + _SENTINEL) * 2
    else:
        # Independent two-column/two-row Native frame: varuint strings, column-major values.
        assert payload == (
            b"\x02\x02\x05value"
            + bytes([len(target)])
            + target.encode()
            + value_bytes * 2
            + b"\x08sentinel\x05Int32"
            + _SENTINEL * 2
        )
    assert stream.native_wire_evidence.rows == 2
    assert stream.native_wire_evidence.failure_code is None
    if backend == "accelerated":
        assert stream.native_acceleration_evidence.selected_backend == "native_accelerated"


@pytest.mark.parametrize("precision", [1, 9, 10, 19, 20, 28, 29, 38])
@pytest.mark.parametrize("negative", [False, True])
@pytest.mark.parametrize("scaled", [False, True])
def test_decimal_precision_is_exact_without_decimal_context_rounding(tmp_path, precision, negative, scaled):
    scale = precision if scaled else 0
    magnitude = 10**precision - 1
    sign = 0 if negative else 1
    wire = bytes([19, precision, scale, sign]) + magnitude.to_bytes(16, "little") + _SENTINEL
    expected = Decimal((int(negative), tuple(map(int, str(magnitude))), -scale))
    schema = [("value", f"decimal({precision},{scale})"), ("sentinel", "int")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic")
    path = tmp_path / "decimal.bcp"
    path.write_bytes(wire)
    assert list(MssqlBcpNativeDecoder(contract).iter_rows(path)) == [{"value": expected, "sentinel": 16909060}]


@pytest.mark.parametrize("rehash", [False, True])
@pytest.mark.parametrize("entry", ["decoder", "native", "provider"])
def test_stale_layout_is_rejected_before_file_access(rehash, entry):
    contract = build_mssql_bcp_native_contract(
        schema=[("value", "uniqueidentifier")], query="SELECT synthetic", target_format="Native"
    )
    stale = replace(contract, columns=(replace(contract.columns[0], prefix_width=0),))
    if rehash:
        stale = replace(stale, type_layout_hash=stable_hash(tuple(column.to_dict() for column in stale.columns)))
    with pytest.raises(ValueError, match="native.*layout"):
        if entry == "decoder":
            MssqlBcpNativeDecoder(stale)
        elif entry == "native":
            artifact = SimpleNamespace(native_wire_contract=stale, file_path="missing.bcp")
            NativeWireTranscoder().to_clickhouse_native(artifact, [("value", "uniqueidentifier")])
        else:
            provider = pytest.importorskip("dpone_native_accel")
            list(
                provider.transcode(
                    {"artifact_path": "missing.bcp", "native_wire_contract": stale.to_dict(), "bulk_wire_contract": {}}
                )
            )


@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
@pytest.mark.parametrize("negative", [False, True])
def test_precision_38_reaches_target_without_rounding(tmp_path, backend, negative):
    magnitude = 10**38 - 1
    schema = [("value", "numeric(38,9)")]
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    path = tmp_path / "sample.bcp"
    path.write_bytes(bytes([19, 38, 9, int(not negative)]) + magnitude.to_bytes(16, "little"))
    artifact = SourceNativeArtifact(path, columns=["value"], native_wire_contract=contract)
    registry = NativeAccelerationRegistry(module_loader=lambda: None) if backend != "accelerated" else None
    if backend == "accelerated":
        pytest.importorskip("dpone_native_accel")
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(
        artifact, schema, clickhouse_schema=[("value", "Decimal(38, 9)")]
    )
    expected = (-magnitude if negative else magnitude).to_bytes(16, "little", signed=True)
    prefix = b"" if fmt == "RowBinary" else b"\x01\x01\x05value\x0eDecimal(38, 9)"
    assert b"".join(stream.iter_bytes()) == prefix + expected


@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
def test_partial_valid_output_never_seals_success_evidence(tmp_path, backend):
    schema = [("value", "uniqueidentifier")]
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    path = tmp_path / "sample.bcp"
    path.write_bytes(bytes.fromhex(_GOLDENS[0][1]) + b"\x10short")
    artifact = SourceNativeArtifact(path, columns=["value"], native_wire_contract=contract)
    registry = NativeAccelerationRegistry(module_loader=lambda: None) if backend != "accelerated" else None
    if backend == "accelerated":
        pytest.importorskip("dpone_native_accel")
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(artifact, schema)
    with pytest.raises(EOFError):
        list(stream.iter_bytes())
    assert stream.native_wire_evidence.failure_code == "native_wire_transcode_failed"
    assert stream.native_wire_evidence.checksum == "sha256:" + "0" * 64
    assert stream.native_wire_evidence.rows == 0


@pytest.mark.parametrize(
    "dtype,wire,value",
    [
        ("money", "000000007b000000", "0.0123"),
        ("smallmoney", "7b000000", "0.0123"),
        ("decimal(10,2)", "130a020139300000000000000000000000000000", "123.45"),
    ],
)
def test_decimal_context_does_not_change_source_values(tmp_path, dtype, wire, value):
    from decimal import localcontext

    contract = build_mssql_bcp_native_contract(schema=[("value", dtype)], query="SELECT synthetic")
    path = tmp_path / "decimal.bcp"
    path.write_bytes(bytes.fromhex(wire))
    with localcontext() as context:
        context.prec = 2
        assert list(MssqlBcpNativeDecoder(contract).iter_rows(path)) == [{"value": Decimal(value)}]


@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
@pytest.mark.parametrize(
    "dtype,wire,target,expected",
    [
        ("int", "2a000000", "Int64", "2a00000000000000"),
        ("money", "0000000010270000", "Decimal(19,2)", "64000000000000000000000000000000"),
        ("decimal(10,2)", "130a0201d2040000000000000000000000000000", "Decimal(12,3)", "3430000000000000"),
    ],
)
def test_numeric_target_conversion_preserves_value_and_width(tmp_path, backend, dtype, wire, target, expected):
    schema = [("value", dtype)]
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    path = tmp_path / "numeric.bcp"
    path.write_bytes(bytes.fromhex(wire))
    artifact = SourceNativeArtifact(path, columns=["value"], native_wire_contract=contract)
    registry = None if backend == "accelerated" else NativeAccelerationRegistry(module_loader=lambda: None)
    if backend == "accelerated":
        pytest.importorskip("dpone_native_accel")
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(
        artifact, schema, clickhouse_schema=[("value", target)]
    )
    prefix = b"" if fmt == "RowBinary" else b"\x01\x01\x05value" + bytes([len(target)]) + target.encode()
    assert b"".join(stream.iter_bytes()) == prefix + bytes.fromhex(expected)


@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
def test_inexact_decimal_rescaling_fails_without_success(tmp_path, backend):
    schema = [("value", "decimal(10,2)")]
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    path = tmp_path / "numeric.bcp"
    path.write_bytes(bytes.fromhex("130a02007d000000000000000000000000000000"))
    artifact = SourceNativeArtifact(path, columns=["value"], native_wire_contract=contract)
    registry = None if backend == "accelerated" else NativeAccelerationRegistry(module_loader=lambda: None)
    if backend == "accelerated":
        pytest.importorskip("dpone_native_accel")
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(
        artifact, schema, clickhouse_schema=[("value", "Decimal(10,1)")]
    )
    with pytest.raises(ValueError, match="decimal_precision_loss"):
        list(stream.iter_bytes())
    assert stream.native_wire_evidence.failure_code == "native_wire_transcode_failed"


@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
@pytest.mark.parametrize(
    "source,target,payload",
    [
        ("date", "Date32", bytes(3)),
        ("date", "Date32", (3652058).to_bytes(3, "little")),
        ("datetime2(7)", "DateTime64(7)", bytes(8)),
        ("datetime2(7)", "DateTime64(7)", bytes(5) + (840057).to_bytes(3, "little")),
        ("bigint", "Int32", (2**31).to_bytes(8, "little")),
    ],
)
def test_target_domain_overflow_never_seals_success(tmp_path, backend, source, target, payload):
    schema = [("value", source)]
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    path = tmp_path / "out-of-range.bcp"
    path.write_bytes(payload)
    artifact = SourceNativeArtifact(path, columns=["value"], native_wire_contract=contract)
    registry = None if backend == "accelerated" else NativeAccelerationRegistry(module_loader=lambda: None)
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(
        artifact, schema, clickhouse_schema=[("value", target)]
    )
    with pytest.raises(ValueError, match="out_of_range"):
        b"".join(stream.iter_bytes())
    assert stream.native_wire_evidence.failure_code == "native_wire_transcode_failed"
    assert stream.native_wire_evidence.rows == 0
