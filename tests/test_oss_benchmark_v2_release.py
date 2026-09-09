from __future__ import annotations

from pathlib import Path

from tools.oss_benchmark.contexts import ReleaseContext, resolve_release_context
from tools.oss_benchmark.quality_gates import evaluate_quality_gates
from tools.oss_benchmark.release_delta import build_release_delta
from tools.oss_benchmark.schema import normalize_benchmark_payload_v2, with_project_identity
from tools.oss_benchmark.state import merge_benchmark_payload


def test_release_context_prefers_explicit_cli_values(tmp_path: Path) -> None:
    context = resolve_release_context(
        repo_root=tmp_path,
        branch="codex/release-v0.62.1",
        git_sha="abc123",
        explicit_version="0.62.1",
        explicit_tag="v0.62.1",
        explicit_sha="abc123",
        dirty=False,
    )

    assert context == ReleaseContext(
        dpone_version="0.62.1",
        release_tag="v0.62.1",
        release_sha="abc123",
        branch="codex/release-v0.62.1",
        dirty=False,
        resolved_by="cli",
    )


def test_release_context_infers_pyproject_version_and_matching_tag(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "dpone"\nversion = "0.62.1"\n', encoding="utf-8")

    def git_runner(*args: str) -> str:
        if args == ("rev-list", "-n", "1", "v0.62.1"):
            return "abc123"
        if args == ("describe", "--tags", "--abbrev=0"):
            return "v0.62.0"
        return ""

    context = resolve_release_context(
        repo_root=tmp_path,
        branch="master",
        git_sha="abc123",
        dirty=False,
        git_runner=git_runner,
    )

    assert context.dpone_version == "0.62.1"
    assert context.release_tag == "v0.62.1"
    assert context.release_sha == "abc123"
    assert context.resolved_by == "pyproject"


def test_v2_payload_normalization_adds_project_identity_without_breaking_spec() -> None:
    project = {
        "spec": {
            "name": "Airbyte",
            "slug": "airbyte",
            "repo_url": "https://github.com/airbytehq/airbyte.git",
            "commit": "84c2",
        }
    }

    normalized_project = with_project_identity(project)
    normalized_payload = normalize_benchmark_payload_v2({"projects": [project]})

    assert normalized_project["project_id"] == "airbyte"
    assert normalized_project["display_name"] == "Airbyte"
    assert normalized_project["repo_url"] == "https://github.com/airbytehq/airbyte.git"
    assert normalized_project["revision"] == "84c2"
    assert normalized_project["spec"]["slug"] == "airbyte"
    assert normalized_payload["schema_version"] == 2
    assert normalized_payload["projects"][0]["project_id"] == "airbyte"


def test_stale_merge_records_stale_age_days_for_failed_refresh() -> None:
    previous = {
        "generated_at": "2026-06-01T00:00:00+00:00",
        "projects": [
            {
                "spec": {"name": "Airbyte", "slug": "airbyte", "kind": "oss-core"},
                "metric_groups": {
                    "loc_sloc": {
                        "status": "fresh",
                        "last_updated_at": "2026-06-01T00:00:00+00:00",
                        "refresh_attempted_at": "2026-06-01T00:00:00+00:00",
                        "last_error": None,
                    }
                },
                "quality": {"solid": 4.0, "clean_oop": 4.0},
            }
        ],
    }

    merged = merge_benchmark_payload(
        {"projects": []},
        previous_payload=previous,
        requested_slugs={"airbyte"},
        attempted_at="2026-06-11T00:00:00+00:00",
        failures={"airbyte": "network timeout"},
        allow_stale=True,
    )

    project = merged["projects"][0]
    group = project["metric_groups"]["loc_sloc"]
    assert group["status"] == "stale"
    assert group["stale_age_days"] == 10
    assert group["last_error"] == "network timeout"


def test_release_delta_marks_stale_projects_not_comparable_and_flags_dpone_regressions() -> None:
    baseline = {
        "projects": [
            {
                "spec": {"slug": "dpone", "name": "dpone"},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
                "industrial_maintainability": {"score": 96},
                "quality": {"solid": 5.0, "clean_oop": 5.0},
                "loc_without_tests": {"max_sloc": 390, "max_lines": 449},
                "coupling": {"cohesion_ratio": 0.62, "avg_clustering": 0.170, "max_ce": 20},
                "architecture_risk": {"score": 12},
            }
        ]
    }
    current = {
        "projects": [
            {
                "spec": {"slug": "dpone", "name": "dpone"},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
                "industrial_maintainability": {"score": 92},
                "quality": {"solid": 5.0, "clean_oop": 4.9},
                "loc_without_tests": {"max_sloc": 401, "max_lines": 450},
                "coupling": {"cohesion_ratio": 0.61, "avg_clustering": 0.181, "max_ce": 21},
                "architecture_risk": {"score": 18},
            }
        ],
        "release_context": {"release_tag": "v0.62.1"},
    }

    delta = build_release_delta(current, baseline_payload=baseline)

    dpone = delta["projects"]["dpone"]
    assert dpone["metrics"]["industrial_maintainability"]["classification"] == "regressed"
    assert dpone["metrics"]["max_module_sloc"]["classification"] == "regressed"
    assert dpone["metrics"]["avg_clustering"]["classification"] == "regressed"
    assert delta["blocking_regressions"]


def test_quality_gates_block_sloc_clustering_missing_release_and_unavailable_dpone() -> None:
    payload = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "unavailable": True,
                "industrial_maintainability": {"score": 95},
                "architecture_risk": {"score": 16},
                "coverage_confidence": {"score": 90},
                "loc_without_tests": {"max_lines": 449, "max_sloc": 401},
                "coupling": {"max_ce": 20, "cohesion_ratio": 0.62, "avg_clustering": 0.181},
                "metric_groups": {"loc_sloc": {"status": "unavailable"}},
            }
        ],
    }

    gates = evaluate_quality_gates(payload)

    assert gates["status"] == "failed"
    failed_ids = {check["id"] for check in gates["failed_checks"]}
    assert {"project_available", "max_module_sloc", "avg_clustering", "release_context"}.issubset(failed_ids)
