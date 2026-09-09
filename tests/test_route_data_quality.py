from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.route_data_quality import RouteDataQualityService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _route(source: str = "mssql", sink: str = "clickhouse", strategy: str = "incremental_merge") -> dict[str, str]:
    return {"source": source, "sink": sink, "strategy": strategy}


def _evidence(
    path: Path,
    *,
    name: str,
    score: float = 100.0,
    passed: bool = True,
    route: dict[str, str] | None = None,
    exception_count: int = 0,
    exception_ratio: float = 0.0,
    max_exception_age_hours: float = 0.0,
    blockers: list[str] | None = None,
    warnings: list[str] | None = None,
    waiver: bool = False,
) -> Path:
    payload: dict[str, object] = {
        "schema_version": "dpone.test_data_quality.v1",
        "route": route or _route(),
        "passed": passed,
        "summary": f"{name} evidence",
        "quality": {
            "score": score,
            "dimensions": {
                name: {
                    "score": score,
                    "passed": passed,
                    "weight": 1.0,
                    "summary": f"{name} score",
                }
            },
        },
        "exceptions": {
            "count": exception_count,
            "ratio": exception_ratio,
            "max_age_hours": max_exception_age_hours,
        },
        "blockers": blockers if blockers is not None else ([] if passed else [f"{name}.failed"]),
        "warnings": warnings or [],
    }
    if waiver:
        payload["waiver"] = {
            "required": True,
            "approved": False,
            "reason": "critical DQ blocker needs steward approval",
        }
    return _write_json(path, payload)


def _green_artifacts(tmp_path: Path, *, source: str = "mssql", sink: str = "clickhouse") -> dict[str, Path]:
    route = _route(source=source, sink=sink)
    return {
        "data_contract": _evidence(tmp_path / "data_contract.json", name="data_contract", route=route),
        "quarantine": _evidence(tmp_path / "quarantine.json", name="quarantine", route=route),
        "reconciliation": _evidence(tmp_path / "reconciliation.json", name="reconciliation", route=route),
    }


def test_route_data_quality_builds_ready_scorecard(tmp_path: Path) -> None:
    report = RouteDataQualityService().evaluate(
        output_dir=tmp_path / "route-dq",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts=_green_artifacts(tmp_path),
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert payload["schema_version"] == "dpone.route_data_quality.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["passed"] is True
    assert payload["status"] == "passed"
    assert payload["score"] == 100.0
    assert payload["dimensions"]["data_contract"]["passed"] is True
    assert payload["exceptions"]["total_count"] == 0
    assert payload["evidence_index"]["data_contract"]["sha256"] != "0" * 64
    assert "Route data quality scorecard" in markdown


def test_route_data_quality_blocks_missing_required_evidence(tmp_path: Path) -> None:
    report = RouteDataQualityService().evaluate(
        output_dir=tmp_path / "route-dq",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={"data_contract": _evidence(tmp_path / "data_contract.json", name="data_contract")},
    )

    assert report.passed is False
    assert report.status == "blocked"
    assert "quarantine.missing" in report.blockers
    assert "reconciliation.missing" in report.blockers


def test_route_data_quality_blocks_route_mismatch_and_malformed_json(tmp_path: Path) -> None:
    invalid_json = tmp_path / "quarantine.json"
    invalid_json.write_text("{not-json", encoding="utf-8")
    report = RouteDataQualityService().evaluate(
        output_dir=tmp_path / "route-dq",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={
            "data_contract": _evidence(
                tmp_path / "data_contract.json",
                name="data_contract",
                route=_route(source="postgres", sink="mssql"),
            ),
            "quarantine": invalid_json,
            "reconciliation": _evidence(tmp_path / "reconciliation.json", name="reconciliation"),
        },
    )

    assert report.passed is False
    assert report.status == "blocked"
    assert "data_contract.route_mismatch" in report.blockers
    assert "quarantine.invalid_json" in report.blockers


def test_route_data_quality_classifies_warning_and_quarantine_sla_breach(tmp_path: Path) -> None:
    warning = RouteDataQualityService().evaluate(
        output_dir=tmp_path / "warning",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={
            "data_contract": _evidence(tmp_path / "warn_contract.json", name="data_contract", score=91.0),
            "quarantine": _evidence(tmp_path / "warn_quarantine.json", name="quarantine", score=89.0),
            "reconciliation": _evidence(tmp_path / "warn_reconciliation.json", name="reconciliation", score=90.0),
        },
        min_score=85.0,
        warning_score=95.0,
    )
    breach = RouteDataQualityService().evaluate(
        output_dir=tmp_path / "breach",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={
            "data_contract": _evidence(tmp_path / "contract.json", name="data_contract"),
            "quarantine": _evidence(
                tmp_path / "breach_quarantine.json",
                name="quarantine",
                exception_count=51,
                exception_ratio=0.03,
                max_exception_age_hours=25.0,
            ),
            "reconciliation": _evidence(tmp_path / "reconciliation.json", name="reconciliation"),
        },
        max_quarantine_rows=50,
        max_exception_ratio=0.02,
        max_exception_age_hours=24.0,
    )

    assert warning.passed is True
    assert warning.status == "warning"
    assert any(item.startswith("route_data_quality.score_below_warning") for item in warning.warnings)
    assert breach.passed is False
    assert breach.status == "quarantine_sla_breached"
    assert "quarantine.exception_count_exceeded" in breach.blockers
    assert "quarantine.exception_ratio_exceeded" in breach.blockers
    assert "quarantine.exception_age_exceeded" in breach.blockers


def test_route_data_quality_requires_waiver_for_critical_blocker(tmp_path: Path) -> None:
    report = RouteDataQualityService().evaluate(
        output_dir=tmp_path / "route-dq",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={
            "data_contract": _evidence(
                tmp_path / "data_contract.json",
                name="data_contract",
                passed=False,
                blockers=["data_contract.null_pk"],
                waiver=True,
            ),
            "quarantine": _evidence(tmp_path / "quarantine.json", name="quarantine"),
            "reconciliation": _evidence(tmp_path / "reconciliation.json", name="reconciliation"),
        },
    )

    assert report.passed is False
    assert report.status == "waiver_required"
    assert "data_contract.waiver_required" in report.blockers
    assert "data_contract.null_pk" in report.blockers


def test_route_data_quality_supports_first_routes_from_matrix(tmp_path: Path) -> None:
    for source, sink in (("postgres", "mssql"), ("mssql", "clickhouse")):
        report = RouteDataQualityService().evaluate(
            output_dir=tmp_path / f"{source}-to-{sink}",
            source=source,
            sink=sink,
            strategy="incremental_merge",
            artifacts=_green_artifacts(tmp_path / f"{source}-to-{sink}-artifacts", source=source, sink=sink),
        )

        assert report.profile is not None
        assert report.passed is True
        assert report.route.case_id == f"{source}_to_{sink}__incremental_merge"
