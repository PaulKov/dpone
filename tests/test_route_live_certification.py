from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.route_live_certification import RouteLiveCertificationService
from dpone.ops.route_release_gate import RouteReleaseGateService
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


def _required_names(route: RouteKey) -> tuple[str, ...]:
    profile = RouteProfileCatalog.default().get(route)
    assert profile is not None
    route_extras = {
        "mssql_to_clickhouse": (
            "cdc_handoff",
            "cdc_apply_certification",
            "cdc_observability_evidence",
            "cdc_recovery_evidence",
            "cdc_schema_evolution_evidence",
            "cdc_promotion_gate",
        ),
        "postgres_to_mssql": (
            "native_transfer_evidence",
            "lossless_transport_contract",
            "resume_checkpoint",
            "type_matrix",
        ),
    }
    return tuple(
        dict.fromkeys(
            (
                "service_markers",
                "route_readiness",
                "route_certification_pack",
                "route_execution_ledger",
                "route_refresh_verification",
                "state_promotion",
                "benchmark_slo",
                "performance_certification",
                "live_state_reconciliation",
                "evidence_chain",
                *profile.required_evidence,
                *route_extras.get(route.pair_id, ()),
            )
        )
    )


def _complete_artifacts(tmp_path: Path, route: RouteKey) -> dict[str, Path]:
    return {
        name: _green_artifact(tmp_path / "evidence" / f"{name}.json", name, route) for name in _required_names(route)
    }


def test_route_live_certification_bundle_passes_for_mssql_clickhouse_cdc(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")

    report = RouteLiveCertificationService().build(
        output_dir=tmp_path / "route-live",
        release="0.8.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="vendor_live",
        row_count=25000,
        artifacts=_complete_artifacts(tmp_path, route),
    )
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert report.level == "release_ready"
    assert report.score == 100.0
    assert payload["schema_version"] == "dpone.route_live_certification.v1"
    assert payload["release"] == "0.8.0-rc1"
    assert payload["profile"] == "vendor_live"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert "cdc_apply_certification" in payload["required_evidence"]
    assert "cdc_promotion_gate" in payload["required_evidence"]
    assert payload["evidence_index"]["cdc_apply_certification"]["route_matched"] is True
    assert payload["evidence_index"]["cdc_apply_certification"]["sha256"] != "0" * 64
    commands = "\n".join(step["command"] for step in payload["harness_steps"])
    assert "docker compose -f docker/docker-compose.integration.yml up -d" in commands
    assert "tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py" in commands
    assert "dpone ops route-release-gate" in commands
    assert "--artifact route_live_evidence_bundle=" in commands
    assert Path(payload["markdown_path"]).exists()
    assert "Route live certification" in Path(report.markdown_path).read_text(encoding="utf-8")


def test_route_live_certification_bundle_feeds_route_release_gate(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    artifacts = _complete_artifacts(tmp_path, route)
    live = RouteLiveCertificationService().build(
        output_dir=tmp_path / "route-live",
        release="0.8.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="vendor_live",
        artifacts=artifacts,
    )

    gate = RouteReleaseGateService().evaluate(
        output_dir=tmp_path / "route-release",
        release="0.8.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        artifacts={**artifacts, "route_live_evidence_bundle": Path(live.json_path)},
        required_evidence=("route_live_evidence_bundle",),
    )

    assert gate.passed is True
    assert "route_live_evidence_bundle" in gate.required_evidence
    assert gate.evidence[-1].route_case_id == route.case_id


def test_route_live_certification_blocks_missing_failed_and_mismatched_evidence(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    wrong_route = RouteKey.of("postgres", "mssql", "incremental_merge")
    failed_readiness = _write_json(
        tmp_path / "route_readiness.json",
        {
            "passed": False,
            "summary": "readiness failed",
            "route": route.to_dict(),
            "blockers": ["benchmark_slo.not_passed"],
        },
    )
    mismatched_apply = _write_json(
        tmp_path / "cdc_apply_certification.json",
        {
            "passed": True,
            "summary": "wrong route",
            "route": wrong_route.to_dict(),
        },
    )

    report = RouteLiveCertificationService().build(
        output_dir=tmp_path / "route-live",
        release="0.8.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="vendor_live",
        artifacts={
            "route_readiness": failed_readiness,
            "cdc_apply_certification": mismatched_apply,
        },
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert "route_readiness.not_passed" in report.blockers
    assert "cdc_apply_certification.route_mismatch" in report.blockers
    assert "route_certification_pack.missing" in report.blockers
    assert report.score < 100.0


def test_route_live_certification_is_matrix_driven_for_first_routes(tmp_path: Path) -> None:
    for source, sink in (("postgres", "mssql"), ("mssql", "clickhouse")):
        route = RouteKey.of(source, sink, "incremental_merge")
        report = RouteLiveCertificationService().build(
            output_dir=tmp_path / "route-live" / route.case_id,
            release="0.8.0-rc1",
            source=route.source,
            sink=route.sink,
            strategy=route.strategy,
            profile="real_local",
            artifacts=_complete_artifacts(tmp_path / route.case_id, route),
        )
        payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
        commands = "\n".join(step["command"] for step in payload["harness_steps"])

        assert report.passed is True
        assert payload["route"]["case_id"] == route.case_id
        assert payload["profile"] == "real_local"
        assert "dpone ops route-release-gate" in commands
        assert any(item["name"] == "run_route_live_tests" for item in payload["harness_steps"])


def test_route_live_certification_native_transfer_profile_requires_transport_certification(tmp_path: Path) -> None:
    route = RouteKey.of("postgres", "mssql", "incremental_merge")

    report = RouteLiveCertificationService().build(
        output_dir=tmp_path / "route-live",
        release="0.22.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="native_transfer",
        artifacts=_complete_artifacts(tmp_path, route),
    )

    assert report.passed is False
    assert "native_transfer_transport_certification.missing" in report.blockers
    assert "native_transfer_route_certification.missing" in report.blockers
