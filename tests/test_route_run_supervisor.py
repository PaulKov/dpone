from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.ops.route_run_supervisor import RouteRunSupervisorService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _route(source: str = "mssql", sink: str = "clickhouse", strategy: str = "incremental_merge") -> dict[str, str]:
    return {"source": source, "sink": sink, "strategy": strategy}


def _artifact(path: Path, *, passed: bool = True, route: dict[str, str] | None = None, **extra: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "dpone.test.v1",
        "route": route or _route(),
        "passed": passed,
        "blockers": [] if passed else ["runtime.transient_connection"],
        "summary": "test artifact",
    }
    payload.update(extra)
    return _write_json(path, payload)


def test_route_run_supervisor_builds_ready_receipt(tmp_path: Path) -> None:
    artifacts = {
        "route_readiness": _artifact(tmp_path / "readiness.json"),
        "route_execution_ledger": _artifact(tmp_path / "ledger.json"),
        "state_promotion": _artifact(tmp_path / "state.json"),
    }

    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest="manifests/orders.yml",
        artifacts=artifacts,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert payload["schema_version"] == "dpone.route_run_supervisor.v1"
    assert payload["decision"]["status"] == "ready"
    assert payload["passed"] is True
    assert payload["run"]["run_id"] == "orders-run-001"
    assert payload["run"]["dataset"] == "analytics.orders"
    assert payload["run"]["manifest"] == "manifests/orders.yml"
    assert payload["phases"]["preflight"]["passed"] is True
    assert payload["phases"]["execution"]["passed"] is True
    assert payload["phases"]["state"]["passed"] is True
    assert "Route run receipt" in markdown


def test_route_run_supervisor_blocks_missing_required_evidence(tmp_path: Path) -> None:
    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={"route_readiness": _artifact(tmp_path / "readiness.json")},
    )

    assert report.passed is False
    assert report.decision.status == "blocked"
    assert "route_execution_ledger.missing" in report.blockers
    assert "state_promotion.missing" in report.blockers


def test_route_run_supervisor_route_refresh_mode_builds_execution_contract(tmp_path: Path) -> None:
    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest="manifests/orders.yml",
        run_mode="route_refresh",
        artifacts={"route_readiness": _artifact(tmp_path / "readiness.json")},
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    contract = payload["execution_contract"]
    stage_index = {stage["evidence"]: stage for stage in contract["stages"]}

    assert report.passed is False
    assert payload["run"]["mode"] == "route_refresh"
    assert contract["mode"] == "route_refresh"
    assert contract["ready"] is False
    assert "route_refresh_execution" in payload["required_evidence"]
    assert "route_refresh_snapshot_capture" in payload["required_evidence"]
    assert "route_refresh_verification" in payload["required_evidence"]
    assert "route_refresh_execution" in payload["phases"]["execution"]["evidence"]
    assert "route_refresh_verification" in payload["phases"]["reconciliation"]["evidence"]
    assert stage_index["route_readiness"]["status"] == "complete"
    assert stage_index["route_refresh_execution"]["status"] == "missing"
    assert "dpone ops route-refresh-execute" in stage_index["route_refresh_execution"]["command"]
    assert "--execute" in stage_index["route_refresh_execution"]["command"]
    assert "route_refresh_execution.missing" in report.blockers
    assert "route_refresh_verification.missing" in report.blockers
    assert any("dpone ops route-refresh-execute" in command for command in contract["next_commands"])


def test_route_run_supervisor_contract_covers_custom_required_evidence(tmp_path: Path) -> None:
    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness.json"),
            "route_execution_ledger": _artifact(tmp_path / "ledger.json"),
            "state_promotion": _artifact(tmp_path / "state.json"),
        },
        required_evidence=("cdc_observability",),
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    stage_index = {stage["evidence"]: stage for stage in payload["execution_contract"]["stages"]}

    assert report.passed is False
    assert stage_index["cdc_observability"]["required"] is True
    assert stage_index["cdc_observability"]["status"] == "missing"
    assert "Attach `cdc_observability` evidence" in stage_index["cdc_observability"]["command"]
    assert "cdc_observability.missing" in report.blockers


def test_route_run_supervisor_rejects_unknown_run_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="route run mode must be one of"):
        RouteRunSupervisorService().evaluate(
            output_dir=tmp_path / "route-run",
            source="mssql",
            sink="clickhouse",
            strategy="incremental_merge",
            run_id="orders-run-001",
            dataset="analytics.orders",
            manifest=None,
            run_mode="surprise",
            artifacts={},
        )


def test_route_run_supervisor_blocks_route_mismatched_artifacts(tmp_path: Path) -> None:
    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness.json"),
            "route_execution_ledger": _artifact(
                tmp_path / "ledger.json",
                route=_route(source="postgres", sink="mssql"),
            ),
            "state_promotion": _artifact(tmp_path / "state.json"),
        },
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert report.passed is False
    assert report.decision.status == "blocked"
    assert "route_execution_ledger.route_mismatch" in report.blockers
    assert payload["evidence_index"]["route_execution_ledger"]["route_matched"] is False


def test_route_run_supervisor_blocks_malformed_required_artifacts(tmp_path: Path) -> None:
    invalid_json = tmp_path / "ledger.json"
    invalid_json.write_text("{not-json", encoding="utf-8")

    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness.json"),
            "route_execution_ledger": invalid_json,
            "state_promotion": _artifact(tmp_path / "state.json"),
        },
    )

    assert report.passed is False
    assert report.decision.status == "blocked"
    assert "route_execution_ledger.invalid_json" in report.blockers


def test_route_run_supervisor_classifies_retry_and_unsafe_retry(tmp_path: Path) -> None:
    retryable = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "retryable",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness.json"),
            "route_execution_ledger": _artifact(
                tmp_path / "ledger.json",
                passed=False,
                blockers=["runtime.transient_connection"],
                safe_to_retry=True,
            ),
            "state_promotion": _artifact(tmp_path / "state.json"),
        },
    )
    unsafe = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "unsafe",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness2.json"),
            "route_execution_ledger": _artifact(tmp_path / "ledger2.json"),
            "state_promotion": _artifact(
                tmp_path / "state2.json",
                passed=False,
                blockers=["state_promotion.commit_token_mismatch"],
                safe_to_retry=False,
            ),
        },
    )

    assert retryable.passed is False
    assert retryable.decision.status == "retryable"
    assert unsafe.passed is False
    assert unsafe.decision.status == "unsafe_to_retry"


def test_route_run_supervisor_never_retries_untrusted_release_certification(tmp_path: Path) -> None:
    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        required_evidence=("route_release_gate",),
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness.json"),
            "route_execution_ledger": _artifact(tmp_path / "ledger.json"),
            "state_promotion": _artifact(tmp_path / "state.json"),
            "route_release_gate": _artifact(
                tmp_path / "release.json",
                evidence_status="UNVERIFIED",
                safe_to_retry=True,
            ),
        },
    )

    item = next(item for item in report.evidence if item.name == "route_release_gate")
    assert report.passed is False
    assert item.passed is False
    assert item.safe_to_retry is False
    assert report.decision.status == "unsafe_to_retry"


def test_route_run_supervisor_requires_manual_approval_for_schema_gate(tmp_path: Path) -> None:
    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness.json"),
            "route_execution_ledger": _artifact(tmp_path / "ledger.json"),
            "state_promotion": _artifact(tmp_path / "state.json"),
            "route_schema_evolution": _artifact(
                tmp_path / "schema.json",
                apply_decision={"mode": "manual_approval", "requires_approval": True},
            ),
        },
    )

    assert report.passed is False
    assert report.decision.status == "manual_approval_required"
    assert "route_schema_evolution.manual_approval_required" in report.blockers


def test_route_run_supervisor_allows_matrix_driven_supported_first_routes(tmp_path: Path) -> None:
    for source, sink in (("postgres", "mssql"), ("mssql", "clickhouse")):
        route_dir = tmp_path / f"{source}-to-{sink}"
        artifacts = {
            "route_readiness": _artifact(route_dir / "readiness.json", route=_route(source=source, sink=sink)),
            "route_execution_ledger": _artifact(route_dir / "ledger.json", route=_route(source=source, sink=sink)),
            "state_promotion": _artifact(route_dir / "state.json", route=_route(source=source, sink=sink)),
        }

        report = RouteRunSupervisorService().evaluate(
            output_dir=route_dir / "route-run",
            source=source,
            sink=sink,
            strategy="incremental_merge",
            run_id=f"{source}-run-001",
            dataset="public.orders",
            manifest=None,
            artifacts=artifacts,
        )

        assert report.profile is not None
        assert report.passed is True
        assert report.route.case_id == f"{source}_to_{sink}__incremental_merge"
