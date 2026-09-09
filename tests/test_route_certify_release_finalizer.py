from __future__ import annotations

import json
import os
from pathlib import Path

from dpone.ops.checksums import sha256_file
from dpone.ops.route_certify_release_finalizer import RouteCertificationReleaseFinalizerService
from dpone.ops.routes.models import RouteKey


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _bundle(
    path: Path,
    route: RouteKey,
    *,
    release: str = "v0.9.0-rc1",
    profile: str = "oss_ci",
    score: float = 100.0,
    passed: bool = True,
) -> Path:
    stage = _write_json(
        path.parent / "route_release_gate.json",
        {
            "passed": passed,
            "evidence_status": "PASS" if passed else "FAIL",
            "blockers": [] if passed else ["route_promotion_gate.not_passed"],
        },
    )
    return _write_json(
        path,
        {
            "schema_version": "dpone.route_certification_bundle.v1",
            "release": release,
            "profile": profile,
            "route": route.to_dict(),
            "passed": passed,
            "evidence_status": "PASS" if passed else "FAIL",
            "level": "certified" if passed else "blocked",
            "score": score,
            "blockers": [] if passed else ["route_promotion_gate.not_passed"],
            "stages": [
                {
                    "name": "route_release_gate",
                    "required": True,
                    "passed": passed,
                    "blockers": [] if passed else ["route_promotion_gate.not_passed"],
                    "path": stage.name,
                    "sha256": sha256_file(stage),
                }
            ],
            "artifact_index": {"route_release_gate": stage.name},
        },
    )


def _first_class_bundles(root: Path, *, release: str = "v0.9.0-rc1") -> dict[str, Path]:
    routes = (
        RouteKey.of("postgres", "mssql", "incremental_merge"),
        RouteKey.of("mssql", "clickhouse", "incremental_merge"),
    )
    return {
        route.case_id: _bundle(root / route.case_id / "route_certification_bundle.json", route, release=release)
        for route in routes
    }


def test_route_release_finalizer_discovers_bundles_and_records_history(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundles"
    _first_class_bundles(bundle_root)

    report = RouteCertificationReleaseFinalizerService().finalize(
        output_dir=tmp_path / "final",
        release="v0.9.0-rc1",
        profile="oss_ci",
        bundle_roots=(bundle_root,),
        history_dir=tmp_path / "history",
    )
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    history = json.loads(Path(report.history_index_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert report.level == "final_ready"
    assert payload["schema_version"] == "dpone.route_certification_release_finalizer.v1"
    assert payload["release_report"]["level"] == "release_ready"
    assert payload["discovered_routes"] == [
        "mssql_to_clickhouse__incremental_merge",
        "postgres_to_mssql__incremental_merge",
    ]
    assert Path(report.release_report_path).exists()
    assert Path(report.markdown_path).exists()
    assert history["releases"][0]["release"] == "v0.9.0-rc1"
    assert history["releases"][0]["route_scores"]["postgres_to_mssql__incremental_merge"] == 100.0


def test_route_release_finalizer_blocks_stale_required_bundle(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundles"
    bundles = _first_class_bundles(bundle_root)
    stale_path = bundles["postgres_to_mssql__incremental_merge"]
    stale_time = stale_path.stat().st_mtime - 7200
    os.utime(stale_path, (stale_time, stale_time))

    report = RouteCertificationReleaseFinalizerService().finalize(
        output_dir=tmp_path / "final",
        release="v0.9.0-rc1",
        profile="oss_ci",
        bundle_roots=(bundle_root,),
        history_dir=tmp_path / "history",
        max_age_hours=1.0,
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert "postgres_to_mssql__incremental_merge.stale" in report.blockers


def test_route_release_finalizer_blocks_bundle_release_mismatch(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundles"
    _first_class_bundles(bundle_root, release="v0.8.0")

    report = RouteCertificationReleaseFinalizerService().finalize(
        output_dir=tmp_path / "final",
        release="v0.9.0-rc1",
        profile="oss_ci",
        bundle_roots=(bundle_root,),
        history_dir=tmp_path / "history",
    )

    assert report.passed is False
    assert "postgres_to_mssql__incremental_merge.release_mismatch" in report.blockers
    assert "mssql_to_clickhouse__incremental_merge.release_mismatch" in report.blockers


def test_route_release_finalizer_blocks_score_regression_against_baseline(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundles"
    routes = _first_class_bundles(bundle_root)
    route = RouteKey.of("postgres", "mssql", "incremental_merge")
    routes[route.case_id] = _bundle(
        bundle_root / route.case_id / "route_certification_bundle.json",
        route,
        score=80.0,
    )
    baseline = _write_json(
        tmp_path / "baseline.json",
        {
            "route_scores": {
                "postgres_to_mssql__incremental_merge": 100.0,
                "mssql_to_clickhouse__incremental_merge": 100.0,
            }
        },
    )

    report = RouteCertificationReleaseFinalizerService().finalize(
        output_dir=tmp_path / "final",
        release="v0.9.0-rc1",
        profile="oss_ci",
        bundle_roots=(bundle_root,),
        history_dir=tmp_path / "history",
        baseline_json=baseline,
    )

    assert report.passed is False
    assert "postgres_to_mssql__incremental_merge.score_regression" in report.blockers
