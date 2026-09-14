"""Produce synthetic local observations without changing production code."""

from __future__ import annotations

import json
import platform
import subprocess
from argparse import ArgumentParser
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

from dpone.runtime.native_acceleration import NativeAccelerationRegistry
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.support.temporal_fidelity import TemporalFidelityProjector
from dpone.runtime.support.type_mapping.mssql_clickhouse import (
    MssqlClickHouseTypeMapper,
    MssqlClickHouseTypePolicy,
    schema_with_temporal_companion_columns,
)
from dpone.type_system.source_sink.mssql_clickhouse import MssqlClickHouseMatrixMapper

Backend = Literal["rowbinary", "python_native", "accelerated"]


def chs(value: str) -> bytes:
    """Encode these short expected strings independently of production codecs."""
    payload = value.encode()
    assert len(payload) < 128
    return bytes([len(payload)]) + payload


def transcode(
    out: Path,
    schema: Sequence[tuple[str, str]],
    wire: bytes,
    backend: Backend,
    *,
    target: Sequence[tuple[str, str]] | None = None,
    policy: MssqlClickHouseTypePolicy | None = None,
    name: str = "sample",
) -> dict[str, Any]:
    """Observe the public transcoder and preserve an independent source fixture."""
    path = out / f"{name}.bcp"
    path.write_bytes(wire)
    # Runtime artifacts may clean up their source path; retain independent fixtures.
    (out / f"fixture-{name}.bcp").write_bytes(wire)
    contract = build_mssql_bcp_native_contract(
        schema=schema,
        query="SELECT synthetic",
        type_policy=policy,
        target_format="RowBinary" if backend == "rowbinary" else "Native",
    )
    artifact = SourceNativeArtifact(path, columns=[n for n, _ in schema], native_wire_contract=contract)
    registry = (
        NativeAccelerationRegistry()
        if backend == "accelerated"
        else NativeAccelerationRegistry(module_loader=lambda: None)
    )
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(
        artifact,
        schema,
        clickhouse_schema=target,
        type_policy=policy,
    )
    try:
        payload = b"".join(stream.iter_bytes())
        result = {"payload_hex": payload.hex(), "error": None}
    except Exception as exc:
        result = {"payload_hex": None, "error": f"{type(exc).__name__}: {exc}"}
    result.update(
        rows=stream.native_wire_evidence.rows,
        failure=stream.native_wire_evidence.failure_code,
        backend=stream.native_acceleration_evidence.selected_backend,
        targets=[c.target_type for c in contract.columns],
    )
    return result


def nullable_time(out: Path) -> dict[str, Any]:
    """Compare exact frames for distinct fractional rows and a NULL sentinel row."""
    schema = [("before", "int"), ("value", "time nullable"), ("after", "int")]
    target = [("before", "Int32"), ("value", "Nullable(String)"), ("after", "Int32")]
    before = [bytes.fromhex(s) for s in ("04030201", "14131211", "24232221")]
    after = [bytes.fromhex(s) for s in ("08070605", "18171615", "28272625")]
    values = [chs("00:00:01.0000001"), chs("00:00:02.0000009")]
    wire = (
        b"".join(
            before[i] + b"\x05" + ticks.to_bytes(5, "little") + after[i] for i, ticks in enumerate((10000001, 20000009))
        )
        + before[2]
        + b"\xff"
        + after[2]
    )
    expected_row = (
        b"".join(before[i] + b"\x00" + values[i] + after[i] for i in range(2)) + before[2] + b"\x01" + after[2]
    )
    expected_native = (
        b"\x03\x03"
        + chs("before")
        + chs("Int32")
        + b"".join(before)
        + chs("value")
        + chs("Nullable(String)")
        + b"\x00\x00\x01"
        + b"".join(values)
        + b"\x00"
        + chs("after")
        + chs("Int32")
        + b"".join(after)
    )
    results = {}
    for backend in ("rowbinary", "python_native", "accelerated"):
        result = transcode(out, schema, wire, backend, target=target, name="nullable-time")
        expected = expected_row if backend == "rowbinary" else expected_native
        result.update(expected_hex=expected.hex(), exact_match=result["payload_hex"] == expected.hex())
        assert result["rows"] == 3 and result["error"] is None
        assert result["exact_match"] == (backend == "accelerated")
        results[backend] = result
    return results


def decimal_shorthand(out: Path) -> dict[str, Any]:
    """Compare implicit and explicit scale using the maximum 38-digit magnitude."""
    magnitude = 10**38 - 1
    wire = bytes([19, 38, 0, 1]) + magnitude.to_bytes(16, "little")
    results = {}
    for backend in ("rowbinary", "python_native", "accelerated"):
        shorthand = transcode(out, [("value", "decimal(38)")], wire, backend, name="decimal-38")
        explicit = transcode(out, [("value", "decimal(38,0)")], wire, backend, name="decimal-38")
        assert shorthand["error"] is not None and "out_of_range" in shorthand["error"]
        assert explicit["error"] is None and explicit["rows"] == 1
        results[backend] = {"shorthand": shorthand, "explicit_control": explicit}
    return results


def temporal_policies(_out: Path) -> dict[str, Any]:
    """Compare parsed authored column policies with their native target mapping."""
    policy = MssqlClickHouseTypePolicy.from_config(
        {
            "temporal": {
                "offset_timestamp": {
                    "mode": "utc_instant",
                    "columns": {
                        "raw_time": {"mode": "preserve_text"},
                        "offset_time": {"mode": "preserve_offset"},
                        "local_time": {"mode": "fixed_timezone", "timezone": "Europe/Paris"},
                    },
                }
            }
        }
    )
    schema = [(name, "datetimeoffset(7)") for name in ("raw_time", "offset_time", "local_time")]
    targets = {
        name: MssqlClickHouseTypeMapper(policy).resolve_column(name, dtype).clickhouse_type for name, dtype in schema
    }
    effective = {name: policy.temporal.for_column(name).offset_timestamp_mode for name, _ in schema}
    companion = schema_with_temporal_companion_columns(schema, policy)
    assert effective == {"raw_time": "preserve_text", "offset_time": "preserve_offset", "local_time": "fixed_timezone"}
    assert set(targets.values()) == {"DateTime64(7, 'UTC')"} and companion == schema
    return {
        "authored_effective_modes": effective,
        "actual_native_targets": targets,
        "actual_companion_schema": companion,
    }


def temporal_projection(out: Path) -> dict[str, Any]:
    """Observe schema projection without pretending immutable bytes changed."""
    schema = [("value", "datetimeoffset(7)")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic")
    path = out / "offset-projection.bcp"
    path.write_bytes((10000001).to_bytes(5, "little") + bytes.fromhex("3af90ab400"))
    artifact = SourceNativeArtifact(path, columns=["value"], native_wire_contract=contract)
    payload = TemporalFidelityProjector().project_payload(LoadPayload(artifact=artifact, schema=schema))
    try:
        NativeWireTranscoder().to_clickhouse_binary(payload.artifact, payload.schema)
    except ValueError as exc:
        error = str(exc)
    else:
        raise AssertionError("Expected current source-schema mismatch")
    assert "native_wire_source_schema_mismatch" in error
    return {
        "source_schema": schema,
        "projected_schema": payload.schema,
        "same_artifact": payload.artifact is artifact,
        "error": error,
    }


def temporal_narrowing(out: Path) -> dict[str, Any]:
    """Record the intentional legacy narrowing contract without calling it fixed."""
    wire = (10000001).to_bytes(5, "little") + bytes.fromhex("3af90a")
    results = {}
    for backend in ("rowbinary", "python_native", "accelerated"):
        result = transcode(
            out, [("value", "datetime2(7)")], wire, backend, target=[("value", "DateTime64(3)")], name="narrowing"
        )
        assert result["error"] is None and result["rows"] == 1
        assert result["payload_hex"].endswith((1000).to_bytes(8, "little", signed=True).hex())
        results[backend] = result
    return {
        "classification": "documented legacy behavior; requires stage-01 contract decision",
        "source_ticks_100ns": 10000001,
        "target_ticks_ms": 1000,
        "results": results,
    }


def planning_fidelity(_out: Path) -> dict[str, Any]:
    """Compare user-facing planning fidelity claims with runtime range caveats."""
    results = {}
    for dtype in ("date", "datetime", "datetime2(7)"):
        planning = MssqlClickHouseMatrixMapper().resolve(dtype)
        runtime = MssqlClickHouseTypeMapper().resolve_column("value", dtype)
        assert planning.lossless is True and runtime.lossless is False
        results[dtype] = {"planning": planning.to_dict(), "runtime": runtime.to_dict()}
    return results


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", type=Path, required=True, help="Directory for synthetic fixtures and JSON observations."
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "python": platform.python_version(),
        "environment": "synthetic-local",
        "live_certification": "SKIP: not authorized",
    }
    for name, producer in (
        ("nullable_time_backend_divergence", nullable_time),
        ("decimal_shorthand", decimal_shorthand),
        ("per_column_temporal_mapping", temporal_policies),
        ("native_temporal_projection", temporal_projection),
        ("legacy_temporal_narrowing", temporal_narrowing),
        ("planning_temporal_fidelity", planning_fidelity),
    ):
        report[name] = producer(args.output_dir)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    (args.output_dir / "binary-observations.json").write_text(rendered, encoding="utf-8")
    print("Six baseline observation groups reproduced; see binary-observations.json. Live certification: SKIP.")
