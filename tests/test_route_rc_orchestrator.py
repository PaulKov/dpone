from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.certification_artifacts import artifact_requires_certification_trust
from dpone.ops.route_rc_orchestrator import RouteReleaseCandidateOrchestratorService
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
    if artifact_requires_certification_trust(name, payload):
        payload["evidence_status"] = "PASS"
    return _write_json(
        path,
        payload,
    )


def _required_route_artifacts(route: RouteKey) -> tuple[str, ...]:
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
                "route_execution_ledger",
                "route_refresh_verification",
                "state_promotion",
                "benchmark_slo",
                "performance_certification",
                "live_state_reconciliation",
                "pre_release_checklist",
                "evidence_chain",
                *profile.required_evidence,
                *route_extras.get(route.pair_id, ()),
            )
        )
    )


def _complete_artifacts(tmp_path: Path, route: RouteKey) -> dict[str, Path]:
    return {
        name: _green_artifact(tmp_path / "evidence" / f"{name}.json", name, route)
        for name in _required_route_artifacts(route)
    }


def test_route_rc_orchestrator_builds_complete_release_train(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")

    report = RouteReleaseCandidateOrchestratorService().run(
        output_dir=tmp_path / "rc",
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
    assert payload["schema_version"] == "dpone.route_rc_orchestrator.v1"
    assert payload["release"] == "0.8.0-rc1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert [step["name"] for step in payload["steps"]] == [
        "route_certification_pack",
        "route_live_certification",
        "route_release_gate",
        "release_evidence_pack",
    ]
    assert payload["artifact_index"]["route_readiness"].endswith("route_readiness.json")
    assert payload["artifact_index"]["route_certification_pack"].endswith("route_certification_pack.json")
    assert payload["artifact_index"]["route_live_evidence_bundle"].endswith("route_live_certification.json")
    assert payload["artifact_index"]["route_release_gate"].endswith("route_release_gate.json")
    assert payload["artifact_index"]["release_evidence_pack"].endswith("release_evidence_pack.json")
    assert Path(payload["artifact_index"]["release_evidence_pack"]).exists()
    release_pack = json.loads(Path(payload["artifact_index"]["release_evidence_pack"]).read_text(encoding="utf-8"))
    assert release_pack["passed"] is True
    assert "route_release_gate" in {item["name"] for item in release_pack["artifacts"]}
    commands = "\n".join(step["command"] for step in payload["steps"])
    assert "dpone ops route-certification-pack" in commands
    assert "dpone ops route-live-certification" in commands
    assert "dpone ops route-release-gate" in commands
    assert "dpone ops release-evidence-pack" in commands
    assert Path(payload["markdown_path"]).exists()


def test_route_rc_orchestrator_fails_closed_when_release_evidence_is_missing(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    artifacts = _complete_artifacts(tmp_path, route)
    artifacts.pop("pre_release_checklist")

    report = RouteReleaseCandidateOrchestratorService().run(
        output_dir=tmp_path / "rc",
        release="0.8.0-rc1",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        profile="vendor_live",
        artifacts=artifacts,
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert "release_evidence_pack.not_passed" in report.blockers
    assert "pre_release_checklist.missing" in report.blockers


def test_route_rc_orchestrator_is_matrix_driven_for_first_routes(tmp_path: Path) -> None:
    for source, sink in (("postgres", "mssql"), ("mssql", "clickhouse")):
        route = RouteKey.of(source, sink, "incremental_merge")
        report = RouteReleaseCandidateOrchestratorService().run(
            output_dir=tmp_path / "rc" / route.case_id,
            release="0.8.0-rc1",
            source=route.source,
            sink=route.sink,
            strategy=route.strategy,
            profile="real_local",
            artifacts=_complete_artifacts(tmp_path / route.case_id, route),
        )
        payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

        assert report.passed is True
        assert payload["route"]["case_id"] == route.case_id
        assert payload["artifact_index"]["route_release_gate"].endswith("route_release_gate.json")
        assert payload["steps"][0]["name"] == "route_certification_pack"
