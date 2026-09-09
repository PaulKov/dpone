from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from dpone.ops.route_execution import RouteExecutionService
from dpone.ops.route_release_gate import RouteReleaseGateService
from dpone.ops.route_state_promotion import RouteStatePromotionService
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.certification import RouteCertificationPackService
from dpone.ops.routes.models import RouteKey

UTC = timezone.utc  # noqa: UP017


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 6, 13, 15, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _green_route_evidence(tmp_path: Path, source: str, sink: str, strategy: str) -> dict[str, Path]:
    profile = RouteProfileCatalog.default().get(RouteKey.of(source, sink, strategy))
    assert profile is not None
    auto = {"matrix_case", "docs_runbook", "manifest_example"}
    result: dict[str, Path] = {}
    for name in profile.required_evidence:
        if name in auto:
            continue
        payload: dict[str, object] = {
            "passed": True,
            "summary": f"{name} ok",
            "route": RouteKey.of(source, sink, strategy).to_dict(),
        }
        if "certification" in name or name in {"route_live_evidence_bundle", "live_state_reconciliation"}:
            payload["evidence_status"] = "PASS"
            payload["production_certification"] = "VERIFIED"
        result[name] = _write_json(tmp_path / "route-evidence" / f"{name}.json", payload)
    return result


def _release_artifacts(tmp_path: Path) -> dict[str, Path]:
    source = "mssql"
    sink = "clickhouse"
    strategy = "incremental_merge"
    clock = _Clock()
    route_artifacts = _green_route_evidence(tmp_path, source, sink, strategy)
    ledger = RouteExecutionService(clock=clock.now).record_step(
        output_dir=tmp_path / "ledger",
        source=source,
        sink=sink,
        strategy=strategy,
        dataset="dbo.orders",
        run_id="run-1",
        stage="loaded_to_staging",
        status="succeeded",
        runner_id="worker-a",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:part:001",
        lease_ttl_seconds=300,
    )
    token = json.loads(Path(ledger.json_path).read_text(encoding="utf-8"))["lease"]["fencing_token"]
    promotion = RouteStatePromotionService(clock=clock.now).promote(
        output_dir=tmp_path / "state",
        source=source,
        sink=sink,
        strategy=strategy,
        dataset="dbo.orders",
        run_id="run-1",
        ledger_json=ledger.json_path,
        proposed_state="lsn:001",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:part:001",
        idempotency_key="promote-lsn-001",
        commit_token="commit-001",
        target="analytics.orders",
        fencing_token=str(token),
        rows_applied=100,
        events_applied=100,
    )
    route_artifacts["route_execution_ledger"] = Path(ledger.json_path)
    route_artifacts["state_promotion"] = Path(promotion.json_path)
    pack = RouteCertificationPackService(repo_root=Path.cwd()).build(
        output_dir=tmp_path / "pack",
        source=source,
        sink=sink,
        strategy=strategy,
        artifacts=route_artifacts,
    )
    release_artifacts = {name: Path(path) for name, path in pack.artifacts.items()}
    release_artifacts.update(
        {
            "route_readiness": Path(pack.readiness_json_path),
            "route_certification_pack": Path(pack.json_path),
            "route_execution_ledger": Path(ledger.json_path),
            "state_promotion": Path(promotion.json_path),
            "route_refresh_verification": _write_json(
                tmp_path / "verification" / "route_refresh_verification.json",
                {
                    "schema_version": "dpone.route_refresh_verification.v1",
                    "passed": True,
                    "status": "verified",
                    "summary": {"chunks_verified": 1},
                    "route": RouteKey.of(source, sink, strategy).to_dict(),
                },
            ),
        }
    )
    return release_artifacts


def test_route_release_gate_passes_with_complete_route_evidence(tmp_path: Path) -> None:
    report = RouteReleaseGateService().evaluate(
        output_dir=tmp_path / "release-gate",
        release="0.8.0-rc1",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts=_release_artifacts(tmp_path),
    )
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert report.level == "release_ready"
    assert report.score == 100.0
    assert payload["schema_version"] == "dpone.route_release_gate.v1"
    assert payload["release"] == "0.8.0-rc1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert "route_readiness" in payload["required_evidence"]
    assert "state_promotion" in payload["required_evidence"]
    assert payload["evidence_index"]["route_readiness"]["sha256"] != "0" * 64
    assert Path(payload["markdown_path"]).exists()
    assert "Route release gate" in Path(report.markdown_path).read_text(encoding="utf-8")


def test_route_release_gate_blocks_missing_and_failed_required_evidence(tmp_path: Path) -> None:
    failed_readiness = _write_json(
        tmp_path / "route_readiness.json",
        {
            "passed": False,
            "blockers": ["benchmark_slo.not_passed"],
            "route": RouteKey.of("postgres", "mssql", "incremental_merge").to_dict(),
        },
    )

    report = RouteReleaseGateService().evaluate(
        output_dir=tmp_path / "release-gate",
        release="0.8.0-rc1",
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        artifacts={"route_readiness": failed_readiness},
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert "route_readiness.not_passed" in report.blockers
    assert "route_certification_pack.missing" in report.blockers
    assert "state_promotion.missing" in report.blockers
    assert report.score < 100.0


def test_route_release_gate_blocks_route_identity_mismatch(tmp_path: Path) -> None:
    mismatched = _write_json(
        tmp_path / "cdc_apply_certification.json",
        {
            "passed": True,
            "summary": "wrong route",
            "route": RouteKey.of("postgres", "mssql", "incremental_merge").to_dict(),
        },
    )

    report = RouteReleaseGateService().evaluate(
        output_dir=tmp_path / "release-gate",
        release="0.8.0-rc1",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={"cdc_apply_certification": mismatched},
        required_evidence=("cdc_apply_certification",),
    )

    assert report.passed is False
    assert "cdc_apply_certification.route_mismatch" in report.blockers


def test_route_release_gate_accepts_route_data_quality_required_evidence(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    quality = _write_json(
        tmp_path / "route_data_quality.json",
        {
            "schema_version": "dpone.route_data_quality.v1",
            "passed": True,
            "status": "passed",
            "score": 100.0,
            "summary": "route data quality passed",
            "route": route.to_dict(),
        },
    )

    report = RouteReleaseGateService().evaluate(
        output_dir=tmp_path / "release-gate",
        release="0.9.0-rc2",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={"route_data_quality": quality},
        required_evidence=("route_data_quality",),
    )

    assert "route_data_quality" in report.required_evidence
    assert report.evidence[-1].name == "route_data_quality"
    assert report.evidence[-1].route_matched is True
    assert "route_data_quality.not_passed" not in report.blockers


def test_route_release_gate_accepts_route_refresh_plan_required_evidence(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    refresh_plan = _write_json(
        tmp_path / "route_refresh_plan.json",
        {
            "schema_version": "dpone.route_refresh_plan.v1",
            "passed": True,
            "status": "ready",
            "summary": "route refresh plan is ready",
            "route": route.to_dict(),
        },
    )

    report = RouteReleaseGateService().evaluate(
        output_dir=tmp_path / "release-gate",
        release="0.10.0-rc1",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={"route_refresh_plan": refresh_plan},
        required_evidence=("route_refresh_plan",),
    )

    assert "route_refresh_plan" in report.required_evidence
    assert report.evidence[-1].name == "route_refresh_plan"
    assert report.evidence[-1].route_matched is True
    assert "route_refresh_plan.not_passed" not in report.blockers


def test_route_release_gate_accepts_route_refresh_execution_required_evidence(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    execution = _write_json(
        tmp_path / "route_refresh_execution.json",
        {
            "schema_version": "dpone.route_refresh_execution.v1",
            "passed": True,
            "status": "succeeded",
            "summary": "route refresh execution succeeded",
            "route": route.to_dict(),
        },
    )

    report = RouteReleaseGateService().evaluate(
        output_dir=tmp_path / "release-gate",
        release="0.10.0-rc2",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={"route_refresh_execution": execution},
        required_evidence=("route_refresh_execution",),
    )

    assert "route_refresh_execution" in report.required_evidence
    assert report.evidence[-1].name == "route_refresh_execution"
    assert report.evidence[-1].route_matched is True
    assert "route_refresh_execution.not_passed" not in report.blockers


def test_route_release_gate_markdown_renders_every_evidence_row(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    artifacts = {
        name: _write_json(
            tmp_path / f"{name}.json",
            {"passed": True, "summary": f"{name} ok", "route": route.to_dict()},
        )
        for name in ("route_readiness", "route_certification_pack")
    }

    report = RouteReleaseGateService().evaluate(
        output_dir=tmp_path / "release-gate",
        release="0.10.0-rc3",
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        artifacts=artifacts,
        required_evidence=("route_readiness", "route_certification_pack"),
    )
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert "| `route_readiness` |" in markdown
    assert "| `route_certification_pack` |" in markdown


def test_route_release_gate_requires_route_refresh_verification_by_default(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    verification = _write_json(
        tmp_path / "route_refresh_verification.json",
        {
            "schema_version": "dpone.route_refresh_verification.v1",
            "passed": True,
            "status": "verified",
            "summary": "route refresh verification passed",
            "route": route.to_dict(),
        },
    )

    report = RouteReleaseGateService().evaluate(
        output_dir=tmp_path / "release-gate",
        release="0.10.0-rc3",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={
            "route_readiness": verification,
            "route_certification_pack": verification,
            "route_execution_ledger": verification,
            "state_promotion": verification,
            "route_refresh_verification": verification,
        },
    )

    assert "route_refresh_verification" in report.required_evidence
    assert "route_refresh_verification.missing" not in report.blockers
    assert any(item.name == "route_refresh_verification" and item.passed for item in report.evidence)


def test_route_release_gate_is_matrix_driven_for_first_routes(tmp_path: Path) -> None:
    for source, sink in (("postgres", "mssql"), ("mssql", "clickhouse")):
        route = RouteKey.of(source, sink, "incremental_merge")
        profile = RouteProfileCatalog.default().get(route)
        assert profile is not None
        required = tuple(
            dict.fromkeys(
                (
                    "route_readiness",
                    "route_certification_pack",
                    "route_execution_ledger",
                    "route_refresh_verification",
                    "state_promotion",
                    *profile.required_evidence,
                )
            )
        )
        artifacts = {}
        for name in required:
            payload: dict[str, object] = {
                "passed": True,
                "summary": f"{name} ok",
                "route": route.to_dict(),
            }
            if "certification" in name or name in {
                "route_live_evidence_bundle",
                "live_state_reconciliation",
            }:
                payload["evidence_status"] = "PASS"
                payload["production_certification"] = "VERIFIED"
            artifacts[name] = _write_json(tmp_path / route.case_id / f"{name}.json", payload)

        report = RouteReleaseGateService().evaluate(
            output_dir=tmp_path / "gate" / route.case_id,
            release="0.8.0-rc1",
            source=source,
            sink=sink,
            strategy="incremental_merge",
            artifacts=artifacts,
        )

        assert report.passed is True
        assert report.level == "release_ready"
        assert set(profile.required_evidence).issubset(set(report.required_evidence))
