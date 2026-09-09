from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.checksums import sha256_file
from dpone.ops.route_certify_release import RouteCertificationReleaseService
from dpone.ops.routes.models import RouteKey


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _bundle(path: Path, route: RouteKey, *, profile: str = "oss_ci", passed: bool = True) -> Path:
    stage_names = ("route_certification_pack", "route_promotion_gate", "release_evidence_pack")
    stage_paths = {
        name: _write_json(
            path.parent / f"{name}.json",
            {
                "passed": passed,
                "evidence_status": "PASS" if passed else "FAIL",
                "blockers": [] if passed else ["upstream.failed"],
            },
        )
        for name in stage_names
    }
    return _write_json(
        path,
        {
            "schema_version": "dpone.route_certification_bundle.v1",
            "evidence_status": "PASS",
            "release": "v0.9.0-rc1",
            "profile": profile,
            "route": route.to_dict(),
            "passed": passed,
            "level": "certified" if passed else "blocked",
            "score": 100.0 if passed else 40.0,
            "blockers": [] if passed else ["route_promotion_gate.not_passed"],
            "stages": [
                {
                    "name": name,
                    "path": str(stage_path),
                    "sha256": sha256_file(stage_path),
                    "passed": passed,
                    "required": True,
                    "summary": f"{name} {'passed' if passed else 'failed'}",
                    "blockers": [] if passed else ["upstream.failed"],
                }
                for name, stage_path in stage_paths.items()
            ],
            "artifact_index": {name: str(stage_path) for name, stage_path in stage_paths.items()},
        },
    )


def _complete_bundles(tmp_path: Path, *, profile: str = "oss_ci") -> dict[str, Path]:
    routes = (
        RouteKey.of("postgres", "mssql", "incremental_merge"),
        RouteKey.of("mssql", "clickhouse", "incremental_merge"),
    )
    return {
        route.case_id: _bundle(tmp_path / route.case_id / "route_certification_bundle.json", route, profile=profile)
        for route in routes
    }


def test_route_certify_release_is_ready_when_first_class_routes_are_certified(tmp_path: Path) -> None:
    report = RouteCertificationReleaseService().evaluate(
        output_dir=tmp_path / "release",
        release="v0.9.0-rc1",
        profile="oss_ci",
        route_bundles=_complete_bundles(tmp_path),
    )
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert report.level == "release_ready"
    assert report.score == 100.0
    assert payload["schema_version"] == "dpone.route_certification_release.v1"
    assert payload["release"] == "v0.9.0-rc1"
    assert payload["required_routes"] == [
        "postgres_to_mssql__incremental_merge",
        "mssql_to_clickhouse__incremental_merge",
    ]
    assert payload["route_index"]["postgres_to_mssql__incremental_merge"]["level"] == "certified"
    assert payload["route_index"]["mssql_to_clickhouse__incremental_merge"]["level"] == "certified"
    assert payload["release_notes_path"].endswith("route_certification_release_notes.md")
    assert Path(payload["release_notes_path"]).exists()
    assert Path(payload["markdown_path"]).exists()


def test_route_certify_release_fails_closed_when_required_route_bundle_is_missing(tmp_path: Path) -> None:
    bundles = _complete_bundles(tmp_path)
    bundles.pop("mssql_to_clickhouse__incremental_merge")

    report = RouteCertificationReleaseService().evaluate(
        output_dir=tmp_path / "release",
        release="v0.9.0-rc1",
        profile="oss_ci",
        route_bundles=bundles,
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert "mssql_to_clickhouse__incremental_merge.missing" in report.blockers


def test_route_certify_release_fails_closed_for_blocked_route_bundle(tmp_path: Path) -> None:
    bundles = _complete_bundles(tmp_path)
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    bundles[route.case_id] = _bundle(
        tmp_path / route.case_id / "route_certification_bundle.json",
        route,
        passed=False,
    )

    report = RouteCertificationReleaseService().evaluate(
        output_dir=tmp_path / "release",
        release="v0.9.0-rc1",
        profile="oss_ci",
        route_bundles=bundles,
    )

    assert report.passed is False
    assert "mssql_to_clickhouse__incremental_merge.not_certified" in report.blockers
    assert "route_promotion_gate.not_passed" in report.blockers


def test_route_certify_release_rejects_statusless_or_unverified_bundles(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    for evidence_status in (None, "UNVERIFIED", " pass "):
        case_root = tmp_path / str(evidence_status)
        bundles = _complete_bundles(case_root)
        path = bundles[route.case_id]
        payload = json.loads(path.read_text(encoding="utf-8"))
        if evidence_status is None:
            payload.pop("evidence_status")
        else:
            payload["evidence_status"] = evidence_status
        _write_json(path, payload)

        report = RouteCertificationReleaseService().evaluate(
            output_dir=case_root / "release",
            release="v0.9.0-rc1",
            profile="oss_ci",
            route_bundles=bundles,
        )

        assert report.passed is False
        assert f"{route.case_id}.not_certified" in report.blockers


def test_route_certify_release_rejects_schema_and_release_replay(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    for mutation, blocker in (
        ({"schema_version": "forged.v1"}, "route_certification_bundle.invalid_schema"),
        ({"release": "v0.8.0"}, "route_certification_bundle.release_mismatch"),
    ):
        case_root = tmp_path / blocker
        bundles = _complete_bundles(case_root)
        path = bundles[route.case_id]
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.update(mutation)
        _write_json(path, payload)

        report = RouteCertificationReleaseService().evaluate(
            output_dir=case_root / "release",
            release="v0.9.0-rc1",
            profile="oss_ci",
            route_bundles=bundles,
        )

        assert report.passed is False
        assert blocker in report.blockers


def test_route_certify_release_rejects_forged_artifact_index(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    bundles = _complete_bundles(tmp_path)
    path = bundles[route.case_id]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["artifact_index"]["forged"] = str(tmp_path / "missing.json")
    _write_json(path, payload)

    report = RouteCertificationReleaseService().evaluate(
        output_dir=tmp_path / "release",
        release="v0.9.0-rc1",
        profile="oss_ci",
        route_bundles=bundles,
    )

    assert report.passed is False
    assert "route_certification_bundle.artifact_invalid:forged" in report.blockers


def test_route_certify_release_fails_closed_for_embedded_route_mismatch(tmp_path: Path) -> None:
    bundles = _complete_bundles(tmp_path)
    expected = RouteKey.of("postgres", "mssql", "incremental_merge")
    embedded = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    bundles[expected.case_id] = _bundle(
        tmp_path / expected.case_id / "route_certification_bundle.json",
        embedded,
    )

    report = RouteCertificationReleaseService().evaluate(
        output_dir=tmp_path / "release",
        release="v0.9.0-rc1",
        profile="oss_ci",
        route_bundles=bundles,
    )

    assert report.passed is False
    assert "postgres_to_mssql__incremental_merge.route_mismatch" in report.blockers


def test_route_certify_release_vendor_live_requires_vendor_live_bundles(tmp_path: Path) -> None:
    report = RouteCertificationReleaseService().evaluate(
        output_dir=tmp_path / "release",
        release="v0.9.0-rc1",
        profile="vendor_live",
        route_bundles=_complete_bundles(tmp_path, profile="oss_ci"),
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert "postgres_to_mssql__incremental_merge.profile_mismatch" in report.blockers
    assert "mssql_to_clickhouse__incremental_merge.profile_mismatch" in report.blockers


def test_route_certify_release_accepts_vendor_live_bundles(tmp_path: Path) -> None:
    report = RouteCertificationReleaseService().evaluate(
        output_dir=tmp_path / "release",
        release="v0.9.0-rc1",
        profile="vendor_live",
        route_bundles=_complete_bundles(tmp_path, profile="vendor_live"),
    )

    assert report.passed is True
    assert report.level == "release_ready"


def test_route_certify_release_reports_optional_route_warnings(tmp_path: Path) -> None:
    bundles = _complete_bundles(tmp_path)
    optional = RouteKey.of("api", "mssql", "incremental_merge")
    bundles[optional.case_id] = _bundle(
        tmp_path / optional.case_id / "route_certification_bundle.json",
        optional,
        passed=False,
    )

    report = RouteCertificationReleaseService().evaluate(
        output_dir=tmp_path / "release",
        release="v0.9.0-rc1",
        profile="oss_ci",
        route_bundles=bundles,
    )

    assert report.passed is True
    assert report.level == "warning"
    assert f"{optional.case_id}.not_certified" in report.warnings


def test_route_certify_release_service_has_no_route_specific_branches() -> None:
    source = Path("src/dpone/ops/route_certify_release.py").read_text(encoding="utf-8")

    assert "postgres_to_mssql" not in source
    assert "mssql_to_clickhouse" not in source
