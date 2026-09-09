"""Actual SQL Server native exports reconciled against ClickHouse in disposable Docker."""

from __future__ import annotations

import json
import uuid
from decimal import Decimal

import pytest
from tests.integration.mssql.native_docker_cases import native_cases
from tests.integration.mssql.native_docker_support import configured_stand

from dpone.runtime.native_acceleration import NativeAccelerationRegistry
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


@pytest.fixture(scope="module")
def stand():
    return configured_stand()


def _stream(path, schema, targets, backend):
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    artifact = SourceNativeArtifact(path, columns=[name for name, _ in schema], native_wire_contract=contract)
    registry = None if backend == "accelerated" else NativeAccelerationRegistry(module_loader=lambda: None)
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(artifact, schema, clickhouse_schema=targets)
    return stream, fmt


@pytest.mark.parametrize("case", native_cases(), ids=lambda case: case.name)
@pytest.mark.parametrize("nullable", [False, True])
def test_native_docker_values(stand, tmp_path, case, nullable):
    name = "bcp_corner_" + uuid.uuid4().hex
    collation = " COLLATE " + case.collation if case.collation else ""
    ddl = case.source + collation + (" NULL" if nullable else " NOT NULL")
    schema = [("before", "int"), ("value", case.source + (" nullable" if nullable else "")), ("after", "int")]
    targets = [
        ("before", "Int32"),
        ("value", f"Nullable({case.target})" if nullable else case.target),
        ("after", "Int32"),
    ]
    values = list(case.values) + ([("NULL", None)] if nullable else [])
    expected = [
        {"before": index + 16909060, "value": text, "after": 84281096} for index, (_, text) in enumerate(values)
    ]
    try:
        stand.sql(
            f"CREATE TABLE dbo.[{name}] ([before] int NOT NULL, [value] {ddl}, [after] int NOT NULL);"
            + f"INSERT dbo.[{name}] VALUES "
            + ",".join(f"({index + 16909060},{expression},84281096)" for index, (expression, _) in enumerate(values))
            + ";"
        )
        path = tmp_path / "actual.bcp"
        wire = stand.export(name, path)
        assert wire
        if case.source.startswith("char(") and not nullable:
            for backend in ("rowbinary", "python_native", "accelerated"):
                with pytest.raises(ValueError, match="native_wire_invalid_layout"):
                    _stream(path, schema, targets, backend)
            return
        stand.ch(f"CREATE TABLE {name} (" + ",".join(f"`{n}` {t}" for n, t in targets) + ") ENGINE=Memory")
        for backend in ("rowbinary", "python_native", "accelerated"):
            backend_path = tmp_path / f"{backend}.bcp"
            backend_path.write_bytes(wire)
            stream, fmt = _stream(backend_path, schema, targets, backend)
            if case.reject:
                with pytest.raises(ValueError, match=case.reject):
                    b"".join(stream.iter_bytes())
                assert stream.native_wire_evidence.failure_code == "native_wire_transcode_failed"
                continue
            payload = b"".join(stream.iter_bytes())
            if backend == "accelerated":
                assert stream.native_acceleration_evidence.selected_backend == "native_accelerated"
            for _ in range(2):
                stand.ch(f"TRUNCATE TABLE {name}")
                stand.ch(f"INSERT INTO {name} FORMAT {fmt}", payload)
                actual = [
                    json.loads(line)
                    for line in stand.ch(
                        f"SELECT before,{case.projection} AS value,after FROM {name} ORDER BY before FORMAT JSONEachRow"
                    ).splitlines()
                ]
                if case.target.startswith("Decimal"):
                    for rows in (actual, expected):
                        for row in rows:
                            if row["value"] is not None:
                                row["value"] = Decimal(row["value"])
                assert actual == expected, (case.name, nullable, backend)
            assert stream.native_wire_evidence.rows == len(values)
    finally:
        stand.ch(f"DROP TABLE IF EXISTS {name}")
        stand.sql(f"DROP TABLE IF EXISTS dbo.[{name}]")


@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
def test_native_docker_empty_corrupt_and_retry(stand, tmp_path, backend):
    name = "bcp_corner_" + uuid.uuid4().hex
    schema = [("value", "uniqueidentifier")]
    targets = [("value", "UUID")]
    try:
        # Separate named columns are needed by the shared ordered-export helper.
        stand.sql(
            f"CREATE TABLE dbo.[{name}] ([before] int NOT NULL,[value] uniqueidentifier NOT NULL,[after] int NOT NULL);"
            + f"INSERT dbo.[{name}] VALUES(1,'00112233-4455-6677-8899-aabbccddeeff',2);"
        )
        schema = [("before", "int"), *schema, ("after", "int")]
        targets = [("before", "Int32"), *targets, ("after", "Int32")]
        path = tmp_path / "actual.bcp"
        assert stand.export(name, path, empty=True) == b""
        empty, _ = _stream(path, schema, targets, backend)
        assert b"".join(empty.iter_bytes()) == b""
        assert empty.native_wire_evidence.rows == 0
        path = tmp_path / "clean.bcp"
        clean = stand.export(name, path)
        for index, corrupted in enumerate((clean[:-1], clean[:4] + b"\x80" + clean[5:], clean + clean[:-1])):
            path = tmp_path / f"corrupt-{index}.bcp"
            path.write_bytes(corrupted)
            stream, _ = _stream(path, schema, targets, backend)
            with pytest.raises((ValueError, EOFError)):
                b"".join(stream.iter_bytes())
            assert stream.native_wire_evidence.failure_code == "native_wire_transcode_failed"
            assert stream.native_wire_evidence.rows == 0
        path = tmp_path / "retry.bcp"
        path.write_bytes(clean)
        retried, _ = _stream(path, schema, targets, backend)
        assert b"".join(retried.iter_bytes())
        assert retried.native_wire_evidence.rows == 1
    finally:
        stand.sql(f"DROP TABLE IF EXISTS dbo.[{name}]")
