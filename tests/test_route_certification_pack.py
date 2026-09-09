from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.certification_artifacts import artifact_requires_certification_trust
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.certification import RouteCertificationPackService, RouteEvidenceProbeResult
from dpone.ops.routes.models import RouteKey


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _green_artifacts_without_auto(tmp_path: Path, source: str, sink: str, strategy: str) -> dict[str, Path]:
    profile = RouteProfileCatalog.default().get(RouteKey.of(source, sink, strategy))
    assert profile is not None
    auto = {"matrix_case", "docs_runbook", "manifest_example"}
    artifacts: dict[str, Path] = {}
    for name in profile.required_evidence:
        if name in auto:
            continue
        payload: dict[str, object] = {"passed": True, "summary": f"{name} ok", "blockers": []}
        if artifact_requires_certification_trust(name, payload):
            payload["evidence_status"] = "PASS"
        artifacts[name] = _write_json(tmp_path / "input" / f"{name}.json", payload)
    return artifacts


def test_route_certification_pack_generates_readiness_compatible_evidence(tmp_path: Path) -> None:
    report = RouteCertificationPackService(repo_root=Path.cwd()).build(
        output_dir=tmp_path / "pack",
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        artifacts=_green_artifacts_without_auto(tmp_path, "postgres", "mssql", "incremental_merge"),
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    readiness = json.loads(Path(report.readiness_json_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert payload["schema_version"] == "dpone.route_certification_pack.v1"
    assert payload["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
    assert readiness["passed"] is True
    assert readiness["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
    assert (tmp_path / "pack" / "evidence" / "matrix_case.json").exists()
    assert (tmp_path / "pack" / "evidence" / "docs_runbook.json").exists()
    assert (tmp_path / "pack" / "evidence" / "manifest_example.json").exists()
    assert payload["artifacts"]["matrix_case"].endswith("matrix_case.json")
    assert "# dpone route certification pack" in Path(report.markdown_path).read_text(encoding="utf-8")


def test_route_certification_pack_blocks_missing_required_evidence(tmp_path: Path) -> None:
    report = RouteCertificationPackService(repo_root=Path.cwd()).build(
        output_dir=tmp_path / "pack",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={},
    )

    readiness = json.loads(Path(report.readiness_json_path).read_text(encoding="utf-8"))

    assert report.passed is False
    assert report.readiness_level == "blocked"
    assert "type_fidelity.missing" in report.blockers
    assert "typed_hash.missing" in report.blockers
    assert readiness["passed"] is False
    assert (tmp_path / "pack" / "evidence" / "type_fidelity.json").exists()


class _BenchmarkProbe:
    name = "benchmark_slo"

    def collect(self, *, output_dir: Path, route: RouteKey) -> RouteEvidenceProbeResult:
        assert route.case_id == "mssql_to_clickhouse__incremental_merge"
        path = output_dir / "probe_benchmark.json"
        return RouteEvidenceProbeResult(
            name=self.name,
            passed=True,
            summary="probe benchmark ok",
            payload={"rows_per_second": 50000},
            source_path=path,
        )


def test_route_certification_pack_uses_injected_probe_for_evidence(tmp_path: Path) -> None:
    artifacts = _green_artifacts_without_auto(tmp_path, "mssql", "clickhouse", "incremental_merge")
    artifacts.pop("benchmark_slo")

    report = RouteCertificationPackService(repo_root=Path.cwd(), probes=(_BenchmarkProbe(),)).build(
        output_dir=tmp_path / "pack",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts=artifacts,
    )

    benchmark = json.loads((tmp_path / "pack" / "evidence" / "benchmark_slo.json").read_text(encoding="utf-8"))

    assert report.passed is True
    assert benchmark["passed"] is True
    assert benchmark["summary"] == "probe benchmark ok"
