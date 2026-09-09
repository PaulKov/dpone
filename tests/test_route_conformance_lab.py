from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.route_conformance import RouteConformanceService
from dpone.ops.routes import RouteConformanceDatasetProfile
from dpone.ops.routes.conformance_dataset import SyntheticRouteDatasetFactory
from dpone.ops.routes.conformance_verifier import RouteConformanceVerifier


def _payload(path: str) -> dict[str, object]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def test_route_conformance_public_exports_are_available() -> None:
    from dpone.ops import RouteConformanceService as ExportedRouteConformanceService
    from dpone.ops.routes import RouteConformanceReport

    assert ExportedRouteConformanceService is RouteConformanceService
    assert RouteConformanceReport.__name__ == "RouteConformanceReport"


def test_synthetic_dataset_is_deterministic_wide_and_nested() -> None:
    profile = RouteConformanceDatasetProfile(
        name="wide_10k_contract",
        row_count=32,
        column_count=20,
        chunk_size=8,
        include_nested=True,
        include_schema_evolution=True,
    )
    factory = SyntheticRouteDatasetFactory()

    first = factory.generate(profile)
    second = factory.generate(profile)

    assert first.fingerprint == second.fingerprint
    assert len(first.columns) == 20
    assert len(first.rows) == 32
    assert first.chunk_count == 4
    assert {column.name for column in first.columns} >= {"id", "parent_id", "nested_payload"}
    assert any(column.physical_contract == "decimal(38,10)" for column in first.columns)
    assert first.schema_evolution_plan["operations"] == ["add_column", "widen_decimal", "make_nullable"]


def test_verifier_detects_row_hash_and_physical_contract_drift() -> None:
    dataset = SyntheticRouteDatasetFactory().generate(
        RouteConformanceDatasetProfile(name="drift_case", row_count=16, column_count=10, chunk_size=4)
    )
    sink_rows = [dict(row) for row in dataset.rows]
    sink_rows[3]["text_001"] = "changed downstream value"
    sink_columns = [
        column.with_physical_contract("nvarchar(255)") if column.name == "text_001" else column
        for column in dataset.columns
    ]

    result = RouteConformanceVerifier().verify(
        source_snapshot=dataset.to_snapshot("source"),
        sink_snapshot=dataset.with_rows_and_columns(sink_rows, sink_columns).to_snapshot("sink"),
    )

    assert result.passed is False
    assert "typed_hash.mismatch" in result.blockers
    assert "physical_contract.text_001.mismatch" in result.blockers
    assert result.mismatch_samples[0]["column"] == "text_001"


def test_route_conformance_service_writes_exact_verification_evidence(tmp_path: Path) -> None:
    report = RouteConformanceService().run(
        output_dir=tmp_path / "conformance",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset_profile=RouteConformanceDatasetProfile(
            name="wide_10k_contract",
            row_count=120,
            column_count=24,
            chunk_size=30,
            include_nested=True,
            include_schema_evolution=True,
        ),
        min_rows=100,
        require_schema_evolution=True,
    )
    payload = _payload(report.json_path)

    assert payload["schema_version"] == "dpone.route_conformance.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["passed"] is True
    assert payload["dataset"]["row_count"] == 120
    assert payload["dataset"]["column_count"] == 24
    assert payload["verification"]["chunk_count"] == 4
    assert payload["verification"]["source_rows"] == 120
    assert payload["verification"]["sink_rows"] == 120
    assert payload["schema_evolution"]["required"] is True
    assert payload["schema_evolution"]["passed"] is True
    assert Path(payload["artifacts"]["source_snapshot"]).exists()
    assert Path(payload["artifacts"]["sink_snapshot"]).exists()
    assert "route_conformance.json" in payload["artifacts"]["report"]
    assert "Route Conformance Lab" in Path(report.markdown_path).read_text(encoding="utf-8")


def test_route_conformance_release_gate_blocks_failed_artifact(tmp_path: Path) -> None:
    service = RouteConformanceService()
    passed = service.run(
        output_dir=tmp_path / "passed",
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        dataset_profile=RouteConformanceDatasetProfile(name="small", row_count=16, column_count=8, chunk_size=8),
    )
    failed = service.run(
        output_dir=tmp_path / "failed",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset_profile=RouteConformanceDatasetProfile(name="too_small", row_count=8, column_count=8, chunk_size=4),
        min_rows=100,
    )

    gate = service.release_gate(
        output_dir=tmp_path / "gate",
        release="v0.10.0-rc1",
        artifacts={"postgres_to_mssql": passed.json_path, "mssql_to_clickhouse": failed.json_path},
    )
    payload = _payload(gate.json_path)

    assert payload["schema_version"] == "dpone.route_conformance_release_gate.v1"
    assert payload["release"] == "v0.10.0-rc1"
    assert payload["passed"] is False
    assert "mssql_to_clickhouse.not_passed" in payload["blockers"]
    assert payload["routes"][0]["name"] == "postgres_to_mssql"
