from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.contracts.airflow_deployment import release_id
from dpone.ops.route_certification_matrix_models import RouteCertificationMatrixError
from dpone.ops.route_certify import RouteCertificationService
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _green_artifact(path: Path, name: str, route: RouteKey) -> Path:
    payload: dict[str, object] = {
        "passed": True,
        "summary": f"{name} ok",
        "route": route.to_dict(),
        "blockers": [],
    }
    if "certification" in name or name in {"route_live_evidence_bundle", "live_state_reconciliation"}:
        payload["evidence_status"] = "PASS"
        payload["production_certification"] = "VERIFIED"
    return _write_json(path, payload)


def _required_artifact_names(route: RouteKey) -> tuple[str, ...]:
    profile = RouteProfileCatalog.default().get(route)
    assert profile is not None
    return tuple(
        dict.fromkeys(
            (
                "route_refresh_execution",
                "route_refresh_snapshot_capture",
                "route_refresh_verification",
                "route_execution_ledger",
                "state_promotion",
                "benchmark_slo",
                "pre_release_checklist",
                "evidence_chain",
                *profile.required_evidence,
            )
        )
    )


def _complete_artifacts(tmp_path: Path, route: RouteKey) -> dict[str, Path]:
    return {
        name: _green_artifact(tmp_path / "evidence" / f"{name}.json", name, route)
        for name in _required_artifact_names(route)
    }


def test_route_certify_promotes_generated_profile_evidence_to_release_gate(tmp_path: Path) -> None:
    route = RouteKey.of("postgres", "mssql", "incremental_merge")
    artifacts = {
        name: path
        for name, path in _complete_artifacts(tmp_path, route).items()
        if name not in {"docs_runbook", "manifest_example", "matrix_case"}
    }

    report = RouteCertificationService().certify(
        output_dir=tmp_path / "certify",
        release="v0.9.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="oss_ci",
        artifacts=artifacts,
    )
    promotion_gate = json.loads(Path(report.artifact_index["route_promotion_gate"]).read_text(encoding="utf-8"))

    assert report.passed is True
    assert promotion_gate["passed"] is True
    assert "matrix_case.missing" not in promotion_gate["blockers"]
    assert "docs_runbook.missing" not in promotion_gate["blockers"]
    assert "manifest_example.missing" not in promotion_gate["blockers"]


def test_route_certify_builds_certified_bundle_for_complete_refresh_evidence(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")

    report = RouteCertificationService().certify(
        output_dir=tmp_path / "certify",
        release="v0.9.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="oss_ci",
        artifacts=_complete_artifacts(tmp_path, route),
    )
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert report.level == "certified"
    assert report.score == 100.0
    assert payload["schema_version"] == "dpone.route_certification_bundle.v1"
    assert payload["evidence_status"] == "UNVERIFIED"
    assert payload["release"] == "v0.9.0-rc1"
    assert payload["profile"] == "oss_ci"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert [stage["name"] for stage in payload["stages"]] == [
        "route_certification_pack",
        "route_promotion_gate",
        "release_evidence_pack",
    ]
    assert payload["artifact_index"]["route_certification_pack"].endswith("route_certification_pack.json")
    assert payload["artifact_index"]["route_readiness"].endswith("route_readiness.json")
    assert payload["artifact_index"]["route_promotion_gate"].endswith("route_release_gate.json")
    assert payload["artifact_index"]["release_evidence_pack"].endswith("release_evidence_pack.json")
    assert Path(payload["artifact_index"]["route_certification_pack"]).exists()
    assert Path(payload["artifact_index"]["route_promotion_gate"]).exists()
    assert Path(payload["artifact_index"]["release_evidence_pack"]).exists()
    assert Path(payload["markdown_path"]).exists()


def test_route_certify_fails_closed_when_snapshot_capture_evidence_is_missing(tmp_path: Path) -> None:
    route = RouteKey.of("postgres", "mssql", "incremental_merge")
    artifacts = _complete_artifacts(tmp_path, route)
    artifacts.pop("route_refresh_snapshot_capture")

    report = RouteCertificationService().certify(
        output_dir=tmp_path / "certify",
        release="v0.9.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="oss_ci",
        artifacts=artifacts,
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert "route_refresh_snapshot_capture.missing" in report.blockers


def test_route_certify_vendor_live_requires_live_bundle(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")

    report = RouteCertificationService().certify(
        output_dir=tmp_path / "certify",
        release="v0.9.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="vendor_live",
        artifacts=_complete_artifacts(tmp_path, route),
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert "route_live_evidence_bundle.missing" in report.blockers


def test_route_certify_accepts_vendor_live_bundle(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    artifacts = _complete_artifacts(tmp_path, route)
    artifacts["route_live_evidence_bundle"] = _green_artifact(
        tmp_path / "evidence" / "route_live_evidence_bundle.json",
        "route_live_evidence_bundle",
        route,
    )

    report = RouteCertificationService().certify(
        output_dir=tmp_path / "certify",
        release="v0.9.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="vendor_live",
        artifacts=artifacts,
    )

    assert report.passed is True
    assert report.level == "certified"
    assert "route_live_evidence_bundle" in report.required_evidence


def test_route_certify_is_matrix_driven_for_first_routes(tmp_path: Path) -> None:
    for source, sink in (("postgres", "mssql"), ("mssql", "clickhouse")):
        route = RouteKey.of(source, sink, "incremental_merge")

        report = RouteCertificationService().certify(
            output_dir=tmp_path / route.case_id / "certify",
            release="v0.9.0-rc1",
            source=route.source,
            sink=route.sink,
            strategy=route.strategy,
            profile="oss_ci",
            artifacts=_complete_artifacts(tmp_path / route.case_id, route),
        )

        assert report.passed is True
        assert report.route == route
        assert report.profile == "oss_ci"


def test_route_certify_service_has_no_route_specific_branches() -> None:
    source = Path("src/dpone/ops/route_certify.py").read_text(encoding="utf-8")

    assert "postgres_to_mssql" not in source
    assert "mssql_to_clickhouse" not in source


def test_route_certify_can_bind_release_and_six_dimensions_for_matrix(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    release_set = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {"dag_specs": [], "workload_packs": [], "canonical_schemas": []},
        "provenance": {
            "source_commit": "a" * 40,
            "build_id": "pytest",
            "built_at": "2026-07-16T10:00:00Z",
        },
    }
    release_set["release_id"] = release_id(release_set)
    release_path = _write_json(tmp_path / "release-set.json", release_set)

    report = RouteCertificationService(clock=lambda: datetime(2026, 7, 16, 12, tzinfo=UTC)).certify(
        output_dir=tmp_path / "certify",
        release="v0.72.3",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="oss_ci",
        artifacts=_complete_artifacts(tmp_path, route),
        release_set=release_path,
    )

    claim = report.to_dict()["matrix_claim"]
    assert claim["release_id"] == release_set["release_id"]
    assert claim["source_commit"] == "a" * 40
    assert claim["route"]["route_id"] == "mssql_clickhouse_incremental_merge_airflow_kpo"
    assert claim["route"]["transport"] == "native_bcp_to_clickhouse"


def test_invalid_matrix_release_fails_before_route_certification_outputs(tmp_path: Path) -> None:
    output = tmp_path / "certify"

    with pytest.raises(RouteCertificationMatrixError) as exc:
        RouteCertificationService().certify(
            output_dir=output,
            release="v0.72.3",
            source="mssql",
            sink="clickhouse",
            strategy="incremental_merge",
            artifacts={},
            release_set=tmp_path / "missing-release-set.json",
        )

    assert exc.value.code == "DPONE_ROUTE_MATRIX_CLAIM_RELEASE_UNSAFE"
    assert not output.exists()
