from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "oss_code_quality_benchmark.py"


def _load_tool():
    spec = importlib.util.spec_from_file_location("oss_code_quality_benchmark", TOOL_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_stable_contract_registry_is_benchmark_owned() -> None:
    from tools.oss_benchmark.stable_contracts import stable_contract_for

    contract = stable_contract_for("dpone.runtime.sinks.load_payload")

    assert contract is not None
    assert contract.to_jsonable(fan_in=17)["fan_in"] == 17
    assert stable_contract_for("dpone.runtime.sinks.clickhouse_impl") is None
    assert "dpone.metrics.stable_contracts" not in (ROOT / "tools" / "oss_benchmark" / "core.py").read_text(
        encoding="utf-8"
    )


def test_counts_loc_and_sloc_without_comment_or_blank_noise() -> None:
    tool = _load_tool()
    text = "\n".join(
        [
            "# python comment",
            "",
            "class Loader:",
            "    pass",
            "",
            "// java-style comment",
            "public class Loader {}",
        ]
    )

    assert tool.count_lines(text) == 7
    assert tool.count_sloc(text) == 3


def test_test_file_detection_covers_common_python_java_and_ts_patterns() -> None:
    tool = _load_tool()

    assert tool.is_test_file(Path("tests/test_pipeline.py")) is True
    assert tool.is_test_file(Path("src/pipeline_test.py")) is True
    assert tool.is_test_file(Path("src/main/java/org/example/PipelineTest.java")) is True
    assert tool.is_test_file(Path("web/src/Pipeline.spec.ts")) is True
    assert tool.is_test_file(Path("web/src/Pipeline.test.tsx")) is True
    assert tool.is_test_file(Path("src/main/java/org/example/Pipeline.java")) is False


def test_import_parsing_covers_python_java_kotlin_and_typescript(tmp_path: Path) -> None:
    tool = _load_tool()

    py_file = tmp_path / "src" / "demo" / "app.py"
    java_file = tmp_path / "engine" / "src" / "main" / "java" / "org" / "demo" / "Engine.java"
    kt_file = tmp_path / "engine" / "src" / "main" / "kotlin" / "org" / "demo" / "Task.kt"
    ts_file = tmp_path / "web" / "src" / "feature" / "view.ts"

    assert tool.parse_import_targets(py_file, "import demo.core\nfrom demo.services import runner\n") == {
        "demo.core",
        "demo.services",
    }
    assert tool.parse_import_targets(java_file, "package org.demo;\nimport org.demo.core.Engine;\n") == {
        "org.demo.core.Engine",
    }
    assert tool.parse_import_targets(kt_file, "package org.demo\nimport org.demo.core.TaskRunner\n") == {
        "org.demo.core.TaskRunner",
    }
    assert tool.parse_import_targets(
        ts_file, "import { render } from '../ui/render';\nimport type { Api } from './api';"
    ) == {
        "web.src.ui.render",
        "web.src.feature.api",
    }


def test_repository_metrics_collect_top_modules_and_internal_coupling(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "src" / "demo" / "__init__.py", "")
    _write(tmp_path / "src" / "demo" / "core.py", "class Engine:\n    pass\n")
    _write(tmp_path / "src" / "demo" / "services.py", "from demo.core import Engine\n")
    _write(tmp_path / "src" / "demo" / "app.py", "from demo.services import Service\nfrom demo.core import Engine\n")
    _write(tmp_path / "tests" / "test_app.py", "from demo.app import main\n")

    metrics = tool.collect_project_metrics(
        tool.ProjectSpec(name="Demo", slug="demo", kind="oss-core", repo_url="", branch="", commit="", path=tmp_path),
        top_n=3,
    )

    assert metrics.loc_with_tests.files == 5
    assert metrics.loc_without_tests.files == 4
    assert metrics.top_loc_with_tests[0].path == "src/demo/app.py"
    assert metrics.coupling.internal_edges == 3
    assert metrics.coupling.max_ce_module == "src.demo.app"
    assert metrics.coupling.max_ca_module == "src.demo.core"
    assert metrics.coupling.cohesion_ratio > 0


def test_collect_all_projects_applies_local_run_context_to_dpone(tmp_path: Path, monkeypatch) -> None:
    tool = _load_tool()
    from tools.oss_benchmark import collectors

    dpone_root = tmp_path / "dpone"
    _write(dpone_root / "src" / "dpone" / "__init__.py", "")
    _write(dpone_root / "src" / "dpone" / "core.py", "class Engine:\n    pass\n")
    specs = (
        tool.ProjectSpec(
            name="dpone",
            slug="dpone",
            kind="local-framework",
            repo_url="",
            branch="local",
            commit="dynamic-at-refresh",
            path=dpone_root,
        ),
        tool.ProjectSpec(
            name="External",
            slug="external",
            kind="oss-core",
            repo_url="",
            branch="main",
            commit="pinned",
            path=tmp_path / "missing-external",
        ),
    )
    monkeypatch.setattr(collectors, "default_project_specs", lambda workspace: specs)

    metrics, failures = tool.collect_all_projects(
        tmp_path / "workspace",
        top_n=3,
        include_external=False,
        local_branch="codex/release-v0.60.0-integration",
        local_commit="abc123",
    )

    assert failures == {}
    assert [metric.spec.slug for metric in metrics] == ["dpone"]
    assert metrics[0].spec.branch == "codex/release-v0.60.0-integration"
    assert metrics[0].spec.commit == "abc123"


def test_internal_dependency_resolution_does_not_shadow_stdlib_imports(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "src" / "demo" / "__init__.py", "")
    _write(tmp_path / "src" / "demo" / "logging.py", "def setup_logging():\n    pass\n")
    _write(tmp_path / "src" / "demo" / "consumer.py", "import logging\n")
    _write(tmp_path / "src" / "demo" / "explicit.py", "from demo.logging import setup_logging\n")

    metrics = tool.collect_project_metrics(
        tool.ProjectSpec(name="Demo", slug="demo", kind="oss-core", repo_url="", branch="", commit="", path=tmp_path),
        top_n=5,
    )

    assert metrics.coupling.internal_edges == 1
    assert metrics.coupling.max_ca == 1
    assert metrics.coupling.max_ca_module == "src.demo.logging"


def test_quality_scoring_rewards_small_cohesive_projects_and_penalizes_hotspots() -> None:
    tool = _load_tool()
    healthy = tool.QualitySignal(
        max_module_loc=220,
        p90_module_loc=140,
        avg_ce=1.5,
        p90_ce=3.0,
        max_ce=6,
        avg_clustering=0.05,
        cohesion_ratio=0.78,
        interface_density=0.12,
    )
    risky = tool.QualitySignal(
        max_module_loc=1800,
        p90_module_loc=850,
        avg_ce=15.0,
        p90_ce=28.0,
        max_ce=80,
        avg_clustering=0.36,
        cohesion_ratio=0.24,
        interface_density=0.01,
    )

    healthy_score = tool.score_quality(healthy)
    risky_score = tool.score_quality(risky)

    assert healthy_score.solid >= 4.0
    assert healthy_score.clean_oop >= 4.0
    assert risky_score.solid < 3.0
    assert risky_score.clean_oop < 3.0
    assert healthy_score.evidence
    assert risky_score.evidence


def test_run_context_serializes_manual_ci_metadata() -> None:
    tool = _load_tool()

    context = tool.RunContext(
        generated_at="2026-06-12T12:00:00+00:00",
        updated_by="paul",
        workflow_name="OSS code quality benchmark",
        run_id="123456",
        run_url="https://github.com/PaulKov/dpone/actions/runs/123456",
        git_sha="abc123",
        branch="master",
    )

    assert tool.run_context_to_jsonable(context) == {
        "branch": "master",
        "generated_at": "2026-06-12T12:00:00+00:00",
        "git_sha": "abc123",
        "run_id": "123456",
        "run_url": "https://github.com/PaulKov/dpone/actions/runs/123456",
        "updated_by": "paul",
        "workflow_name": "OSS code quality benchmark",
    }


def test_merge_payload_preserves_previous_metric_groups_when_refresh_fails() -> None:
    tool = _load_tool()
    previous = {
        "benchmark_date": "2026-06-12",
        "projects": [
            {
                "spec": {"name": "Airbyte", "slug": "airbyte", "kind": "oss-core"},
                "metric_groups": {
                    "loc_sloc": {
                        "status": "fresh",
                        "last_updated_at": "2026-06-10T08:00:00+00:00",
                        "refresh_attempted_at": "2026-06-10T08:00:00+00:00",
                        "last_error": None,
                    }
                },
                "loc_without_tests": {"total_lines": 295493},
                "quality": {"solid": 4.2, "clean_oop": 4.0},
            }
        ],
    }
    refreshed = {"benchmark_date": "2026-06-12", "projects": []}

    merged = tool.merge_benchmark_payload(
        refreshed,
        previous_payload=previous,
        requested_slugs={"airbyte"},
        attempted_at="2026-06-12T12:00:00+00:00",
        failures={"airbyte": "git fetch failed"},
        allow_stale=True,
    )

    [project] = merged["projects"]
    assert project["loc_without_tests"]["total_lines"] == 295493
    assert project["quality"]["solid"] == 4.2
    assert project["metric_groups"]["loc_sloc"]["status"] == "stale"
    assert project["metric_groups"]["loc_sloc"]["last_updated_at"] == "2026-06-10T08:00:00+00:00"
    assert project["metric_groups"]["loc_sloc"]["refresh_attempted_at"] == "2026-06-12T12:00:00+00:00"
    assert project["metric_groups"]["loc_sloc"]["last_error"] == "git fetch failed"


def test_merge_payload_marks_unavailable_when_no_previous_metric_exists() -> None:
    tool = _load_tool()
    refreshed = {"benchmark_date": "2026-06-12", "projects": []}

    merged = tool.merge_benchmark_payload(
        refreshed,
        previous_payload=None,
        requested_slugs={"apache-hop"},
        attempted_at="2026-06-12T12:00:00+00:00",
        failures={"apache-hop": "checkout failed"},
        allow_stale=True,
    )

    [project] = merged["projects"]
    assert project["spec"]["slug"] == "apache-hop"
    assert project["unavailable"] is True
    assert set(project["metric_groups"]) == set(tool.METRIC_GROUPS)
    assert {group["status"] for group in project["metric_groups"].values()} == {"unavailable"}
    assert project["metric_groups"]["loc_sloc"]["last_error"] == "checkout failed"


def test_industrial_maintainability_index_rewards_balanced_projects() -> None:
    tool = _load_tool()
    strong_project = {
        "spec": {"name": "dpone", "slug": "dpone"},
        "loc_without_tests": {"max_lines": 420, "total_sloc": 90000},
        "coupling": {"avg_ce": 2.0, "p90_ce": 5.0, "cohesion_ratio": 0.62, "avg_clustering": 0.12},
        "quality": {"solid": 5.0, "clean_oop": 5.0},
        "loc_with_tests": {"total_sloc": 130000},
        "metric_groups": {"loc_sloc": {"status": "fresh"}},
    }
    risky_project = {
        "spec": {"name": "Legacy", "slug": "legacy"},
        "loc_without_tests": {"max_lines": 2500, "total_sloc": 100000},
        "coupling": {"avg_ce": 16.0, "p90_ce": 30.0, "cohesion_ratio": 0.18, "avg_clustering": 0.34},
        "quality": {"solid": 2.0, "clean_oop": 1.5},
        "loc_with_tests": {"total_sloc": 110000},
        "metric_groups": {"loc_sloc": {"status": "stale"}},
    }

    strong = tool.compute_industrial_maintainability_index(strong_project)
    risky = tool.compute_industrial_maintainability_index(risky_project)

    assert strong["score"] >= 90
    assert strong["band"] == "excellent"
    assert risky["score"] < 55
    assert risky["band"] in {"watch", "risk"}
    assert set(strong["components"]) == {
        "quality",
        "module_size",
        "coupling",
        "cohesion",
        "test_footprint",
        "freshness",
    }
    assert strong["evidence"]


def test_history_snapshot_keeps_latest_score_trends(tmp_path: Path) -> None:
    tool = _load_tool()
    payload = {
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul", "run_url": "https://run"},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "industrial_maintainability": {"score": 94, "band": "excellent"},
                "quality": {"solid": 5.0, "clean_oop": 5.0},
            }
        ],
    }
    previous_history = {
        "entries": [
            {
                "generated_at": "2026-06-11T12:00:00+00:00",
                "projects": {"dpone": {"score": 90, "band": "excellent"}},
            }
        ]
    }

    history = tool.update_history_payload(payload, previous_history=previous_history, max_entries=10)

    assert len(history["entries"]) == 2
    assert history["entries"][-1]["projects"]["dpone"]["score"] == 94
    assert history["latest_deltas"]["dpone"]["score"] == 4
    output_path = tmp_path / "history.json"
    tool.write_history_payload(output_path, history)
    assert json.loads(output_path.read_text(encoding="utf-8"))["latest_deltas"]["dpone"]["score"] == 4


def test_history_snapshot_tracks_quality_governance_metrics() -> None:
    tool = _load_tool()
    payload = {
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul"},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "industrial_maintainability": {"score": 96, "band": "excellent"},
                "quality": {"solid": 5.0, "clean_oop": 5.0},
                "loc_without_tests": {"max_lines": 453},
                "coupling": {
                    "avg_ce": 1.99,
                    "p90_ce": 5.0,
                    "max_ce": 13,
                    "max_ce_module": "dpone.manifest.explain",
                },
                "coverage_confidence": {"test_footprint_ratio": 0.438},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
            }
        ],
    }
    previous_history = {
        "entries": [
            {
                "generated_at": "2026-06-11T12:00:00+00:00",
                "projects": {
                    "dpone": {
                        "score": 96,
                        "band": "excellent",
                        "solid": 5.0,
                        "clean_oop": 5.0,
                        "max_ce": 18,
                        "max_ce_module": "dpone.runtime.etl.processor",
                        "avg_ce": 2.2,
                        "p90_ce": 6.0,
                        "largest_module_loc": 520,
                        "test_footprint_ratio": 0.40,
                    }
                },
            }
        ]
    }

    history = tool.update_history_payload(payload, previous_history=previous_history, max_entries=10)
    dpone = history["entries"][-1]["projects"]["dpone"]
    delta = history["latest_deltas"]["dpone"]

    assert dpone["max_ce"] == 13
    assert dpone["max_ce_module"] == "dpone.manifest.explain"
    assert dpone["largest_module_loc"] == 453
    assert dpone["test_footprint_ratio"] == 0.438
    assert delta["max_ce"] == -5
    assert delta["p90_ce"] == -1
    assert delta["largest_module_loc"] == -67
    assert delta["test_footprint_ratio"] == 0.038


def test_coverage_confidence_scores_test_footprint_and_ci_evidence() -> None:
    tool = _load_tool()
    high_confidence = {
        "loc_with_tests": {"files": 140, "total_sloc": 140000},
        "loc_without_tests": {"files": 100, "total_sloc": 90000},
        "ci_evidence": {"has_ci": True, "has_coverage_config": True, "signals": ("pytest --cov", "coverage.xml")},
    }
    low_confidence = {
        "loc_with_tests": {"files": 104, "total_sloc": 104000},
        "loc_without_tests": {"files": 100, "total_sloc": 100000},
        "ci_evidence": {"has_ci": False, "has_coverage_config": False, "signals": ()},
    }

    strong = tool.compute_coverage_confidence(high_confidence)
    weak = tool.compute_coverage_confidence(low_confidence)

    assert strong["confidence"] == "high"
    assert strong["score"] >= 75
    assert strong["runtime_coverage"] == "configured"
    assert weak["confidence"] == "low"
    assert weak["score"] < 45
    assert set(strong["components"]) == {"test_footprint", "test_file_ratio", "ci_evidence", "coverage_config"}
    assert strong["evidence"]


def test_architecture_risk_heatmap_highlights_hotspots() -> None:
    tool = _load_tool()
    healthy = {
        "loc_without_tests": {"max_lines": 420},
        "top_loc_without_tests": [{"path": "src/dpone/good.py", "lines": 420}],
        "coupling": {
            "max_ce": 12,
            "max_ce_module": "dpone.good",
            "max_ca": 20,
            "max_ca_module": "dpone.port",
            "cohesion_ratio": 0.62,
            "avg_clustering": 0.12,
        },
    }
    risky = {
        "loc_without_tests": {"max_lines": 2400},
        "top_loc_without_tests": [{"path": "legacy/God.java", "lines": 2400}],
        "coupling": {
            "max_ce": 145,
            "max_ce_module": "legacy.God",
            "max_ca": 180,
            "max_ca_module": "legacy.Shared",
            "cohesion_ratio": 0.18,
            "avg_clustering": 0.36,
        },
    }

    low = tool.compute_architecture_risk(healthy)
    high = tool.compute_architecture_risk(risky)

    assert low["level"] == "low"
    assert high["level"] == "critical"
    assert high["score"] >= 75
    assert [item["kind"] for item in high["hotspots"][:3]] == ["module_size", "fan_out", "fan_in"]
    assert any(item["kind"] == "cohesion" for item in high["hotspots"])


def test_architecture_risk_ignores_approved_stable_fan_in_contracts() -> None:
    tool = _load_tool()
    project = {
        "spec": {"slug": "dpone"},
        "loc_without_tests": {"max_lines": 420},
        "top_loc_without_tests": [{"path": "src/dpone/good.py", "lines": 420}],
        "coupling": {
            "max_ce": 12,
            "max_ce_module": "dpone.good",
            "max_ca": 43,
            "max_ca_module": "dpone.commands.output_json",
            "top_in": [
                ["dpone.commands.output_json", 43],
                ["dpone.runtime.sources.extract_result", 42],
                ["dpone.runtime.etl.processor", 41],
            ],
            "cohesion_ratio": 0.62,
            "avg_clustering": 0.12,
        },
    }

    risk = tool.compute_architecture_risk(project)

    assert any(item["module"] == "dpone.commands.output_json" for item in risk["approved_fan_in_contracts"])
    assert any(item["module"] == "dpone.runtime.sources.extract_result" for item in risk["approved_fan_in_contracts"])
    assert any(item["kind"] == "fan_in" and item["label"] == "dpone.runtime.etl.processor" for item in risk["hotspots"])
    assert not any(
        item["kind"] == "fan_in" and item["label"] == "dpone.commands.output_json" for item in risk["hotspots"]
    )


def test_quality_gates_enforce_dpone_governance_thresholds() -> None:
    tool = _load_tool()
    passing_payload = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "industrial_maintainability": {"score": 95, "band": "excellent"},
                "architecture_risk": {"score": 16, "level": "low", "hotspots": []},
                "coverage_confidence": {"score": 92, "confidence": "high"},
                "loc_without_tests": {"max_lines": 447},
                "coupling": {"max_ce": 20, "cohesion_ratio": 0.591},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
            }
        ]
    }
    failing_payload = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "industrial_maintainability": {"score": 72, "band": "strong"},
                "architecture_risk": {"score": 58, "level": "high", "hotspots": []},
                "coverage_confidence": {"score": 41, "confidence": "low"},
                "loc_without_tests": {"max_lines": 900},
                "coupling": {"max_ce": 64, "cohesion_ratio": 0.32},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
            }
        ]
    }

    passing = tool.evaluate_quality_gates(passing_payload)
    failing = tool.evaluate_quality_gates(failing_payload)

    assert passing["status"] == "passed"
    assert passing["failed"] == 0
    assert {check["id"] for check in passing["checks"]} == {
        "industrial_maintainability",
        "architecture_risk",
        "coverage_confidence",
        "max_module_loc",
        "max_fan_out",
        "cohesion",
        "freshness",
    }
    assert failing["status"] == "failed"
    assert failing["failed"] >= 5
    assert any(check["id"] == "cohesion" and check["status"] == "failed" for check in failing["checks"])


def test_score_explanations_make_industrial_index_auditable() -> None:
    tool = _load_tool()
    payload = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "industrial_maintainability": {
                    "score": 96,
                    "band": "excellent",
                    "components": {
                        "quality": 35.0,
                        "module_size": 20.0,
                        "coupling": 15.0,
                        "cohesion": 13.0,
                        "test_footprint": 10.0,
                        "freshness": 5.0,
                    },
                },
                "quality": {
                    "solid": 5.0,
                    "clean_oop": 4.8,
                    "evidence": ["largest module stays within target", "average fan-out is controlled"],
                },
                "loc_without_tests": {"max_lines": 450},
                "coupling": {"avg_ce": 2.0, "p90_ce": 5.0, "cohesion_ratio": 0.64, "avg_clustering": 0.16},
                "coverage_confidence": {"test_footprint_ratio": 0.43},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
            }
        ]
    }

    explanations = tool.build_score_explanations(payload)
    dpone = explanations["projects"]["dpone"]

    assert explanations["schema_version"] == 1
    assert dpone["formula"] == "quality + module_size + coupling + cohesion + test_footprint + freshness"
    assert dpone["score"] == 96
    assert {component["id"] for component in dpone["components"]} == {
        "quality",
        "module_size",
        "coupling",
        "cohesion",
        "test_footprint",
        "freshness",
    }
    assert dpone["components"][0]["max_score"] == 35
    assert "SOLID" in dpone["rubric"]["solid"]["label"]
    assert dpone["rubric"]["clean_oop"]["score"] == 4.8


def test_regression_summary_flags_quality_degradation_against_previous_evidence() -> None:
    tool = _load_tool()
    previous = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "industrial_maintainability": {"score": 98},
                "loc_without_tests": {"max_lines": 430},
                "coupling": {"p90_ce": 4.0, "cross_slice_ratio": 0.30},
                "coverage_confidence": {"test_footprint_ratio": 0.45},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
            }
        ]
    }
    current = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "industrial_maintainability": {"score": 94},
                "loc_without_tests": {"max_lines": 470},
                "coupling": {"p90_ce": 6.0, "cross_slice_ratio": 0.36},
                "coverage_confidence": {"test_footprint_ratio": 0.40},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
            }
        ]
    }

    summary = tool.build_regression_summary(current, previous_payload=previous)

    assert summary["schema_version"] == 1
    assert summary["project"] == "dpone"
    assert summary["status"] == "regressed"
    assert {change["id"] for change in summary["regressions"]} == {
        "industrial_maintainability",
        "max_module_loc",
        "p90_fan_out",
        "cross_slice_ratio",
        "test_footprint",
    }
    assert any(change["delta"] == -4 for change in summary["changes"] if change["id"] == "industrial_maintainability")


def test_pr_regression_gate_summarizes_blockers_warnings_and_improvements() -> None:
    tool = _load_tool()
    payload = {
        "quality_gates": {
            "status": "passed",
            "failed_checks": [],
            "checks": [
                {
                    "id": "max_fan_out",
                    "label": "Max module fan-out",
                    "actual": 13,
                    "operator": "<=",
                    "threshold": 24,
                    "status": "passed",
                    "severity": "blocker",
                }
            ],
        },
        "regression_summary": {
            "status": "regressed",
            "regressions": [
                {
                    "id": "test_footprint",
                    "label": "Static test footprint",
                    "previous": 0.45,
                    "current": 0.40,
                    "delta": -0.05,
                    "status": "regressed",
                }
            ],
        },
        "candidate_quality_delta": {"status": "passed", "failed_budgets": []},
        "trend_summary": {
            "dpone": {
                "score": 0,
                "max_ce": -5,
                "p90_ce": -1,
                "largest_module_loc": -67,
                "test_footprint_ratio": 0.038,
            }
        },
    }

    gate = tool.build_pr_regression_gate(payload)

    assert gate["schema_version"] == 1
    assert gate["status"] == "warning"
    assert any(check["id"] == "test_footprint" and check["severity"] == "warning" for check in gate["checks"])
    assert any(check["id"] == "max_ce" and check["status"] == "improved" for check in gate["checks"])
    assert gate["summary"]["warnings"] == 1
    assert gate["summary"]["improvements"] >= 3


def test_remediation_backlog_prioritizes_dpone_quality_work() -> None:
    tool = _load_tool()
    payload = {
        "quality_gates": {
            "failed_checks": [
                {
                    "id": "cohesion",
                    "label": "Cohesion ratio",
                    "actual": 0.43,
                    "operator": ">=",
                    "threshold": 0.55,
                    "status": "failed",
                    "severity": "blocker",
                }
            ]
        },
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "loc_without_tests": {"max_lines": 520},
                "top_loc_without_tests": [{"path": "src/dpone/runtime/sinks/clickhouse_sink.py", "lines": 520}],
                "coupling": {"max_ce": 24, "max_ce_module": "dpone.runtime.sinks.clickhouse_sink"},
                "architecture_risk": {
                    "hotspots": [
                        {
                            "kind": "fan_out",
                            "severity": "medium",
                            "label": "dpone.runtime.sinks.clickhouse_sink",
                            "value": 24,
                            "unit": "Ce",
                        }
                    ]
                },
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
            }
        ],
    }

    backlog = tool.build_remediation_backlog(payload)

    assert backlog["schema_version"] == 1
    assert backlog["items"]
    assert backlog["items"][0]["priority"] == "P0"
    assert backlog["items"][0]["category"] == "quality_gate"
    assert any(item["category"] == "module_size_watch" for item in backlog["items"])
    assert any(item["category"] == "fan_out_watch" for item in backlog["items"])


def test_candidate_quality_delta_flags_budget_regressions_and_changed_modules() -> None:
    tool = _load_tool()
    baseline = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "loc_without_tests": {"max_lines": 430},
                "top_loc_without_tests": [
                    {"path": "src/dpone/runtime/etl/processor.py", "lines": 390, "sloc": 350, "is_test": False}
                ],
                "coupling": {
                    "max_ce": 18,
                    "p90_ce": 5.0,
                    "cohesion_ratio": 0.65,
                    "cross_slice_ratio": 0.35,
                    "top_out": [["dpone.runtime.etl.processor", 17]],
                },
                "coverage_confidence": {"test_footprint_ratio": 0.44},
            }
        ]
    }
    candidate = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "loc_without_tests": {"max_lines": 620},
                "top_loc_without_tests": [
                    {"path": "src/dpone/runtime/etl/processor.py", "lines": 455, "sloc": 410, "is_test": False},
                    {"path": "src/dpone/runtime/cdc/retention_models.py", "lines": 420, "sloc": 390, "is_test": False},
                ],
                "coupling": {
                    "max_ce": 34,
                    "p90_ce": 14.0,
                    "cohesion_ratio": 0.50,
                    "cross_slice_ratio": 0.42,
                    "top_out": [["dpone.runtime.etl.processor", 24]],
                },
                "coverage_confidence": {"test_footprint_ratio": 0.40},
            }
        ]
    }

    delta = tool.build_candidate_quality_delta(candidate, baseline_payload=baseline)

    assert delta["schema_version"] == 1
    assert delta["status"] == "failed"
    assert {budget["id"] for budget in delta["failed_budgets"]} == {
        "max_module_loc",
        "p90_fan_out",
        "max_fan_out",
        "cohesion",
        "test_footprint",
    }
    changed = {module["path"]: module for module in delta["changed_modules"]}
    assert changed["src/dpone/runtime/etl/processor.py"]["loc_delta"] == 65
    assert changed["src/dpone/runtime/etl/processor.py"]["fan_out_delta"] == 7
    assert changed["src/dpone/runtime/cdc/retention_models.py"]["status"] == "new"


def test_candidate_quality_delta_reports_missing_baseline() -> None:
    tool = _load_tool()
    delta = tool.build_candidate_quality_delta({"projects": []}, baseline_payload=None)

    assert delta["status"] == "no_baseline"
    assert delta["quality_budgets"] == []
    assert delta["changed_modules"] == []


def test_architecture_taxonomy_scores_contract_and_naming_discipline(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "sinks" / "sink_protocol.py",
        "class SinkProtocol:\n    pass\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "sinks" / "clickhouse_impl.py",
        "from dpone.runtime.sinks.sink_protocol import SinkProtocol\n\nclass ClickHouseSink(SinkProtocol):\n    pass\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "sources" / "postgres.py",
        "class PostgresSource:\n    pass\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "commands" / "run_cmd.py",
        "from dpone.runtime.sinks.clickhouse_impl import ClickHouseSink\n\ndef run():\n    return ClickHouseSink()\n",
    )
    project = {
        "spec": {"slug": "dpone", "name": "dpone", "path": str(tmp_path)},
        "loc_without_tests": {"max_lines": 4, "total_lines": 12, "total_sloc": 9, "files": 4},
        "coupling": {
            "cross_slice_ratio": 0.42,
            "cohesion_ratio": 0.58,
            "top_out": [("dpone.commands.run_cmd", 7)],
        },
    }

    matrix = tool.build_architecture_taxonomy_matrix([project])
    summary = matrix["summary"]["dpone"]
    sink_slice = next(item for item in summary["slices"] if item["slice"] == "runtime.sinks")
    violations = summary["contract_conformance"]["violations"]

    assert matrix["schema_version"] == 1
    assert summary["score"] < 100
    assert sink_slice["interface_files"] == 1
    assert any(item["slice"] == "runtime.sources" and item["kind"] == "missing_contract" for item in violations)
    assert any(
        item["path"].endswith("clickhouse_impl.py") and item["kind"] == "naming_consistency" for item in violations
    )
    assert summary["contract_conformance"]["naming_consistency"]["score"] < 100


def test_architecture_taxonomy_approves_documented_compatibility_facades(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "connectors" / "api" / "base.py",
        "class AbstractAPIConnector:\n    pass\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "connectors" / "api" / "orders_connector.py",
        "from dpone.runtime.connectors.api.base import AbstractAPIConnector\n\nclass OrdersConnector(AbstractAPIConnector):\n    pass\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "connectors" / "api" / "orders.py",
        '"""Public facade for the Orders API connector."""\n\n'
        "from dpone.runtime.connectors.api.orders_connector import OrdersConnector\n\n"
        '__all__ = ["OrdersConnector"]\n',
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "connectors" / "api" / "orders_impl.py",
        '"""Deprecated compatibility shim for Orders connector imports."""\n\n'
        "from dpone.runtime.connectors.api.orders_connector import OrdersConnector\n\n"
        '__all__ = ["OrdersConnector"]\n',
    )
    project = {
        "spec": {"slug": "dpone", "name": "dpone", "path": str(tmp_path)},
        "coupling": {"cross_slice_ratio": 0.2, "cohesion_ratio": 0.8, "top_out": []},
    }

    matrix = tool.build_architecture_taxonomy_matrix([project])
    contract = matrix["summary"]["dpone"]["contract_conformance"]

    assert contract["compatibility_facades"]["count"] == 2
    assert contract["naming_consistency"]["score"] == 100
    assert contract["di_boundary"]["score"] == 100
    assert contract["violations"] == []


def test_architecture_taxonomy_allows_same_slice_support_imports_but_flags_cross_slice_concrete_imports(
    tmp_path: Path,
) -> None:
    tool = _load_tool()
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "connectors" / "connector_protocol.py",
        "class ConnectorProtocol:\n    pass\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "connectors" / "bigquery_gcs_mixin.py",
        "class BigQueryGCSMixin:\n    pass\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "connectors" / "bigquery_connector.py",
        "from dpone.runtime.connectors.bigquery_gcs_mixin import BigQueryGCSMixin\n\n"
        "class BigQueryConnector(BigQueryGCSMixin):\n    pass\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "sinks" / "sink_protocol.py",
        "class SinkProtocol:\n    pass\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "sinks" / "bigquery.py",
        "from dpone.runtime.connectors.bigquery_connector import BigQueryConnector\n\n"
        "class BigQuerySink:\n    connector = BigQueryConnector\n",
    )
    project = {
        "spec": {"slug": "dpone", "name": "dpone", "path": str(tmp_path)},
        "coupling": {"cross_slice_ratio": 0.2, "cohesion_ratio": 0.8, "top_out": []},
    }

    matrix = tool.build_architecture_taxonomy_matrix([project])
    violations = matrix["summary"]["dpone"]["contract_conformance"]["violations"]

    assert not any(item["path"].endswith("bigquery_connector.py") for item in violations)
    assert any(
        item["path"].endswith("runtime/sinks/bigquery.py") and item["kind"] == "di_boundary" for item in violations
    )


def test_complexity_boundary_analysis_scores_complexity_and_contracts(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "etl" / "loader.py",
        "\n".join(
            [
                "def complex_loader(rows, mode):",
                "    total = 0",
                "    for row in rows:",
                "        if row and (row.get('active') or mode == 'force'):",
                "            try:",
                "                if row.get('kind') == 'a':",
                "                    total += 1",
                "                elif row.get('kind') == 'b':",
                "                    total += 2",
                "            except KeyError:",
                "                total -= 1",
                "    return [item for item in rows if item or mode == 'force']",
            ]
        ),
    )
    _write(
        tmp_path / "tools" / "oss_benchmark" / "renderers" / "bad.py",
        "from tools.oss_benchmark.collectors import collect_all_projects\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "runtime" / "sinks" / "postgres.py",
        "from dpone.runtime.sinks.clickhouse_impl import ClickHouseSink\n",
    )
    _write(
        tmp_path / "src" / "dpone" / "ports" / "sink.py",
        "from typing import Protocol\n\nclass SinkPort(Protocol):\n    def load(self) -> None: ...\n",
    )
    project = {
        "spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework", "path": str(tmp_path)},
        "source_files_without_tests": 4,
        "interface_like_files": 1,
        "loc_without_tests": {"max_lines": 12},
        "coupling": {"avg_ce": 2.0, "p90_ce": 4.0, "max_ce": 8},
    }

    python_metrics = tool.measure_python_complexity((tmp_path / "src/dpone/runtime/etl/loader.py").read_text())
    analysis = tool.analyze_complexity_boundary_discipline([project])
    summary = analysis["summary"]["dpone"]

    assert python_metrics["max_complexity"] >= 8
    assert python_metrics["top_units"][0]["unit"] == "complex_loader"
    assert analysis["schema_version"] == 1
    assert summary["complexity_score"] < 100
    assert summary["boundary_score"] < 100
    assert summary["di_score"] < 100
    assert summary["overall_score"] < 100
    assert summary["top_complex_units"][0]["unit"] == "complex_loader"
    violation_kinds = {item["kind"] for item in summary["boundary_violations"]}
    assert {"benchmark_renderer_imports_collector", "direct_implementation_import"} <= violation_kinds
    assert any(item["project"] == "dpone" and item["priority"] in {"P1", "P2"} for item in analysis["risk_register"])


def test_cli_accepts_baseline_data_argument() -> None:
    tool = _load_tool()

    args = tool.parse_args(["--baseline-data", "docs/benchmarks/data/baseline.json"])

    assert args.baseline_data == "docs/benchmarks/data/baseline.json"


def test_cli_parses_external_analyzer_timeout() -> None:
    tool = _load_tool()

    args = tool.parse_args(["--external-analyzer-timeout", "45"])

    assert args.external_analyzer_timeout == 45


def test_cli_parses_verify_source_urls_toggle() -> None:
    tool = _load_tool()

    args = tool.parse_args(["--verify-source-urls"])

    assert args.verify_source_urls is True


def test_pr_summary_renders_quality_gates_deltas_and_hotspots() -> None:
    tool = _load_tool()
    payload = {
        "run_context": {
            "generated_at": "2026-06-12T12:00:00+00:00",
            "updated_by": "paul",
            "run_url": "https://github.example/run/123",
            "git_sha": "abc123",
            "branch": "feature/benchmark",
        },
        "freshness_summary": {"fresh": 5, "stale": 0, "unavailable": 0},
        "trend_summary": {"dpone": {"score": 2, "solid": 0, "clean_oop": 0}},
        "quality_gates": {
            "status": "passed",
            "passed": 7,
            "failed": 0,
            "checks": [
                {
                    "id": "industrial_maintainability",
                    "label": "Industrial Maintainability Index",
                    "actual": 95,
                    "operator": ">=",
                    "threshold": 85,
                    "status": "passed",
                    "severity": "blocker",
                }
            ],
        },
        "regression_summary": {
            "status": "regressed",
            "regressions": [
                {
                    "label": "Largest production module LOC",
                    "previous": 430,
                    "current": 470,
                    "delta": 40,
                }
            ],
        },
        "candidate_quality_delta": {
            "status": "failed",
            "failed_budgets": [
                {
                    "label": "P90 fan-out",
                    "actual": 14,
                    "operator": "<=",
                    "threshold": 12,
                }
            ],
            "changed_modules": [
                {
                    "path": "src/dpone/runtime/etl/processor.py",
                    "status": "changed",
                    "loc_delta": 65,
                    "fan_out_delta": 7,
                    "risk": "watch",
                }
            ],
        },
        "pr_regression_gate": {
            "status": "warning",
            "summary": {"blockers": 0, "warnings": 1, "improvements": 2},
            "checks": [
                {
                    "id": "test_footprint",
                    "label": "Static test footprint",
                    "status": "regressed",
                    "severity": "warning",
                    "previous": 0.45,
                    "current": 0.40,
                    "delta": -0.05,
                },
                {
                    "id": "max_ce",
                    "label": "Max fan-out",
                    "status": "improved",
                    "severity": "info",
                    "previous": 18,
                    "current": 13,
                    "delta": -5,
                },
            ],
        },
        "complexity_boundary": {
            "summary": {
                "dpone": {
                    "overall_score": 88,
                    "status": "watch",
                    "complexity_score": 91,
                    "boundary_score": 85,
                    "di_score": 88,
                    "risk_count": 1,
                    "top_complex_units": [
                        {
                            "module": "src/dpone/runtime/etl/processor.py",
                            "unit": "load_payload",
                            "complexity": 14,
                        }
                    ],
                    "boundary_violations": [
                        {
                            "kind": "direct_implementation_import",
                            "path": "src/dpone/runtime/sinks/postgres.py",
                            "severity": "warning",
                        }
                    ],
                }
            },
            "risk_register": [
                {
                    "priority": "P2",
                    "project": "dpone",
                    "module": "src/dpone/runtime/sinks/postgres.py",
                    "reason": "Direct implementation import bypasses a thin port.",
                    "recommendation": "Depend on a sink protocol or factory contract.",
                }
            ],
        },
        "remediation_backlog": {
            "items": [
                {
                    "priority": "P2",
                    "project": "dpone",
                    "title": "Lower fan-out in dpone.runtime.sinks.clickhouse_sink",
                    "recommendation": "Introduce narrower ports.",
                }
            ]
        },
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "industrial_maintainability": {"score": 95, "band": "excellent"},
                "architecture_risk": {
                    "score": 16,
                    "level": "low",
                    "hotspots": [
                        {
                            "severity": "medium",
                            "kind": "fan_out",
                            "label": "dpone.ops.catalog_artifacts",
                            "value": 20,
                            "unit": "Ce",
                        }
                    ],
                },
                "coverage_confidence": {"score": 92, "confidence": "high"},
            }
        ],
    }

    summary = tool.render_pr_summary(payload)

    assert "OSS code quality benchmark refresh" in summary
    assert "Quality gates: **passed**" in summary
    assert "Industrial Maintainability Index" in summary
    assert "dpone" in summary
    assert "+2" in summary
    assert "dpone.ops.catalog_artifacts" in summary
    assert "Regression summary" in summary
    assert "Largest production module LOC" in summary
    assert "Remediation backlog" in summary
    assert "Lower fan-out" in summary
    assert "Candidate quality delta" in summary
    assert "P90 fan-out" in summary
    assert "src/dpone/runtime/etl/processor.py" in summary
    assert "PR regression gate" in summary
    assert "Static test footprint" in summary
    assert "Max fan-out" in summary
    assert "Complexity & Boundary Discipline" in summary
    assert "load_payload" in summary
    assert "Direct implementation import" in summary


def test_benchmark_markdown_renders_intelligence_sections() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-12T12:00:00+00:00",
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 7, "failed": 0, "checks": []},
        "architecture_delta": {
            "project": "dpone",
            "status": "improved",
            "items": [
                {
                    "id": "max_ce",
                    "label": "Max fan-out",
                    "previous": 18,
                    "current": 13,
                    "delta": -5,
                    "direction": "lower_is_better",
                    "status": "improved",
                }
            ],
        },
        "complexity_boundary": {
            "summary": {
                "dpone": {
                    "name": "dpone",
                    "overall_score": 92,
                    "status": "strong",
                    "complexity_score": 94,
                    "boundary_score": 90,
                    "di_score": 88,
                    "max_complexity": 10,
                    "p90_complexity": 6,
                    "risk_count": 1,
                    "top_complex_units": [
                        {
                            "module": "src/dpone/runtime/etl/processor.py",
                            "unit": "load_payload",
                            "complexity": 10,
                            "kind": "function",
                        }
                    ],
                    "boundary_violations": [
                        {
                            "kind": "direct_implementation_import",
                            "path": "src/dpone/runtime/sinks/postgres.py",
                            "severity": "warning",
                            "message": "Depend on a port instead of concrete implementation.",
                        }
                    ],
                }
            },
            "risk_register": [
                {
                    "priority": "P2",
                    "project": "dpone",
                    "module": "src/dpone/runtime/sinks/postgres.py",
                    "reason": "Direct implementation import bypasses DI.",
                    "recommendation": "Introduce or reuse a sink protocol.",
                }
            ],
        },
        "score_explanations": {
            "projects": {
                "dpone": {
                    "score": 96,
                    "band": "excellent",
                    "formula": "quality + module_size + coupling + cohesion + test_footprint + freshness",
                    "components": [
                        {
                            "id": "quality",
                            "label": "SOLID and Clean OOP",
                            "score": 35.0,
                            "max_score": 35,
                            "actual": "SOLID 5.0/5, Clean OOP 5.0/5",
                            "reason": "small modules and low fan-out",
                        }
                    ],
                    "rubric": {"solid": {"score": 5.0}, "clean_oop": {"score": 5.0}},
                }
            }
        },
        "regression_summary": {
            "status": "unchanged",
            "changes": [
                {
                    "label": "Industrial Maintainability Index",
                    "previous": 96,
                    "current": 96,
                    "delta": 0,
                    "direction": "higher_is_better",
                    "status": "unchanged",
                }
            ],
        },
        "remediation_backlog": {
            "items": [
                {
                    "priority": "P2",
                    "project": "dpone",
                    "category": "module_size_watch",
                    "evidence": "450 LOC",
                    "recommendation": "Split before crossing the hard gate.",
                }
            ]
        },
        "architecture_taxonomy": {
            "summary": {
                "dpone": {
                    "score": 91,
                    "band": "leader",
                    "contract_conformance": {
                        "score": 88,
                        "naming_consistency": {"score": 82},
                        "contract_reuse": {"score": 92},
                        "di_boundary": {"score": 90},
                        "violations": [
                            {
                                "kind": "naming_consistency",
                                "slice": "runtime.sinks",
                                "path": "src/dpone/runtime/sinks/clickhouse_impl.py",
                                "severity": "watch",
                                "message": "Use canonical connector naming.",
                            }
                        ],
                    },
                    "slices": [
                        {
                            "slice": "runtime.sinks",
                            "modules": 3,
                            "loc": 220,
                            "sloc": 180,
                            "interface_files": 1,
                            "max_loc": 120,
                        }
                    ],
                }
            }
        },
        "candidate_quality_delta": {
            "status": "passed",
            "quality_budgets": [
                {
                    "label": "Largest production module LOC",
                    "actual": 450,
                    "operator": "<=",
                    "threshold": 600,
                    "status": "passed",
                }
            ],
            "changed_modules": [
                {
                    "path": "src/dpone/runtime/etl/processor.py",
                    "status": "changed",
                    "loc_delta": 10,
                    "fan_out_delta": 1,
                    "risk": "watch",
                }
            ],
        },
        "projects": [
            {
                "spec": {
                    "name": "dpone",
                    "slug": "dpone",
                    "kind": "local-framework",
                    "branch": "main",
                    "commit": "abc123",
                },
                "dirty": False,
                "loc_with_tests": {"total_lines": 100, "total_sloc": 90, "files": 3},
                "loc_without_tests": {"total_lines": 70, "total_sloc": 60, "files": 2, "max_lines": 450},
                "top_loc_with_tests": [],
                "top_sloc_with_tests": [],
                "top_loc_without_tests": [],
                "top_sloc_without_tests": [],
                "coupling": {
                    "avg_ce": 2.0,
                    "p90_ce": 5.0,
                    "max_ce": 18,
                    "max_ce_module": "dpone.runtime.sinks.clickhouse_sink",
                    "max_ca": 20,
                    "cohesion_ratio": 0.64,
                    "cross_slice_ratio": 0.36,
                    "avg_clustering": 0.16,
                },
                "quality": {"solid": 5.0, "clean_oop": 5.0, "evidence": ["small modules"]},
                "coverage_confidence": {
                    "confidence": "high",
                    "score": 100,
                    "runtime_coverage": "configured",
                    "test_footprint_ratio": 0.5,
                    "test_file_ratio": 0.5,
                    "evidence": ["test footprint 50.0%"],
                },
                "ci_evidence": {"has_ci": True, "has_coverage_config": True},
                "architecture_risk": {"score": 0, "level": "low", "hotspots": []},
                "industrial_maintainability": {
                    "score": 96,
                    "band": "excellent",
                    "components": {"quality": 35.0},
                },
                "metric_groups": {"loc_sloc": {"status": "fresh", "last_updated_at": "2026-06-12T12:00:00+00:00"}},
            }
        ],
    }

    markdown = tool.render_markdown(payload)

    assert "## Explainable scoring" in markdown
    assert "## Regression summary" in markdown
    assert "## Remediation backlog" in markdown
    assert "## Candidate quality delta" in markdown
    assert "SOLID and Clean OOP" in markdown
    assert "module_size_watch" in markdown
    assert "Largest production module LOC" in markdown
    assert "src/dpone/runtime/etl/processor.py" in markdown
    assert "## Architecture Taxonomy & Contract Discipline" in markdown
    assert "Contract conformance" in markdown
    assert "runtime.sinks" in markdown
    assert "clickhouse_impl.py" in markdown
    assert "## Architecture delta" in markdown
    assert "assets/oss-architecture-delta.svg" in markdown
    assert "## Complexity & Boundary Discipline" in markdown
    assert "assets/oss-complexity-boundary.svg" in markdown
    assert "Enterprise change risk" in markdown
    assert "load_payload" in markdown


def test_complexity_boundary_svg_renders_scores_and_violations() -> None:
    tool = _load_tool()
    payload = {
        "complexity_boundary": {
            "summary": {
                "dpone": {
                    "name": "dpone",
                    "overall_score": 92,
                    "status": "strong",
                    "complexity_score": 94,
                    "boundary_score": 90,
                    "di_score": 88,
                    "boundary_violation_count": 1,
                },
                "airbyte": {
                    "name": "Airbyte",
                    "overall_score": 71,
                    "status": "watch",
                    "complexity_score": 74,
                    "boundary_score": 69,
                    "di_score": 70,
                    "boundary_violation_count": 4,
                },
            }
        }
    }

    svg = tool.render_complexity_boundary_svg(payload)

    assert "<svg" in svg
    assert "Complexity and boundary discipline" in svg
    assert "dpone" in svg
    assert "Airbyte" in svg
    assert "violations 1" in svg


def test_refactor_roi_roadmap_ranks_quality_economics() -> None:
    tool = _load_tool()
    payload = {
        "projects": [
            {"spec": {"slug": "dpone", "name": "dpone"}, "architecture_risk": {"score": 16, "level": "low"}},
            {"spec": {"slug": "airbyte", "name": "Airbyte"}, "architecture_risk": {"score": 58, "level": "high"}},
        ],
        "complexity_boundary": {
            "summary": {
                "dpone": {
                    "name": "dpone",
                    "overall_score": 59,
                    "status": "risk",
                    "max_complexity": 77,
                    "god_unit_count": 8,
                    "boundary_violation_count": 4,
                    "risk_count": 8,
                },
                "airbyte": {
                    "name": "Airbyte",
                    "overall_score": 67,
                    "status": "watch",
                    "max_complexity": 154,
                    "god_unit_count": 20,
                    "boundary_violation_count": 0,
                    "risk_count": 8,
                },
            },
            "risk_register": [
                {
                    "priority": "P1",
                    "project": "dpone",
                    "module": "src/dpone/commands/dag/list_edges_cmd.py",
                    "reason": "cmd_dag_list_edges complexity is 77.",
                    "recommendation": "Split decision paths behind a smaller service.",
                },
                {
                    "priority": "P2",
                    "project": "dpone",
                    "module": "src/dpone/runtime/sinks/postgres.py",
                    "reason": "Direct implementation import bypasses DI.",
                    "recommendation": "Depend on a sink protocol.",
                },
                {
                    "priority": "P1",
                    "project": "airbyte",
                    "module": "airbyte-integrations/source-mssql/MsSqlServerDebeziumOperations.kt",
                    "reason": "module complexity is 154.",
                    "recommendation": "Split connector operations.",
                },
            ],
        },
        "remediation_backlog": {
            "items": [
                {
                    "priority": "P2",
                    "project": "dpone",
                    "title": "Lower fan-out in manifest explain",
                    "recommendation": "Move report formatting behind a renderer.",
                }
            ]
        },
    }

    roadmap = tool.build_refactor_roi_roadmap(payload)
    dpone = roadmap["summary"]["dpone"]
    top = roadmap["items"][0]

    assert roadmap["schema_version"] == 1
    assert dpone["debt_points"] > 0
    assert dpone["top_debt_driver"] == "complexity"
    assert dpone["quick_win_count"] >= 1
    assert top["project"] == "dpone"
    assert top["module"] == "src/dpone/commands/dag/list_edges_cmd.py"
    assert top["roi_score"] > 70
    assert top["quadrant"] == "high-impact / low-effort"
    assert top["target_architecture"] == "Command -> Application service -> Renderer"
    assert any(item["target_architecture"] == "Sink -> SinkProtocol -> StrategyFactory" for item in roadmap["items"])
    assert roadmap["quadrants"]["high-impact / low-effort"]


def test_refactor_roi_markdown_svg_and_pr_summary() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-12T12:00:00+00:00",
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 7, "failed": 0, "checks": []},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
                "dirty": False,
                "loc_with_tests": {"total_lines": 100, "total_sloc": 90, "files": 3},
                "loc_without_tests": {"total_lines": 70, "total_sloc": 60, "files": 2, "max_lines": 450},
                "top_loc_with_tests": [],
                "top_sloc_with_tests": [],
                "top_loc_without_tests": [],
                "top_sloc_without_tests": [],
                "coupling": {"avg_ce": 2.0, "p90_ce": 5.0, "cohesion_ratio": 0.64, "avg_clustering": 0.16},
                "quality": {"solid": 5.0, "clean_oop": 5.0, "evidence": ["small modules"]},
                "coverage_confidence": {"confidence": "high", "score": 100},
                "ci_evidence": {"has_ci": True, "has_coverage_config": True},
                "architecture_risk": {"score": 0, "level": "low", "hotspots": []},
                "industrial_maintainability": {"score": 96, "band": "excellent", "components": {"quality": 35.0}},
                "metric_groups": {"loc_sloc": {"status": "fresh", "last_updated_at": "2026-06-12T12:00:00+00:00"}},
            }
        ],
        "refactor_roi": {
            "schema_version": 1,
            "summary": {
                "dpone": {
                    "project": "dpone",
                    "name": "dpone",
                    "debt_points": 74,
                    "status": "active-debt",
                    "top_debt_driver": "complexity",
                    "quick_win_count": 2,
                    "strategic_refactor_count": 1,
                }
            },
            "items": [
                {
                    "rank": 1,
                    "project": "dpone",
                    "module": "src/dpone/commands/dag/list_edges_cmd.py",
                    "reason": "cmd_dag_list_edges complexity is 77.",
                    "impact_score": 95,
                    "effort_score": 80,
                    "debt_points": 32,
                    "roi_score": 90,
                    "quadrant": "high-impact / low-effort",
                    "target_architecture": "Command -> Application service -> Renderer",
                    "recommended_action": "Split decision paths behind a smaller service.",
                }
            ],
            "quadrants": {"high-impact / low-effort": [1]},
        },
    }

    markdown = tool.render_markdown(payload)
    svg = tool.render_refactor_roi_svg(payload)
    summary = tool.render_pr_summary(payload)

    assert "## Refactor ROI Roadmap" in markdown
    assert "assets/oss-refactor-roi-roadmap.svg" in markdown
    assert "Quality debt estimate" in markdown
    assert "ROI-ranked refactor backlog" in markdown
    assert "Target architecture recommendations" in markdown
    assert "Command -> Application service -> Renderer" in markdown
    assert "<svg" in svg
    assert "Refactor ROI roadmap" in svg
    assert "cmd_dag_list_edges" in summary
    assert "Refactor ROI Roadmap" in summary


def test_evidence_trust_summary_scores_measured_derived_inferred_and_stale() -> None:
    tool = _load_tool()
    payload = {
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul"},
        "closed_core_notes": [{"name": "Fivetran"}, {"name": "Informatica"}],
        "projects": [
            {
                "spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework"},
                "metric_groups": {
                    "loc_sloc": {"status": "fresh"},
                    "coupling_cohesion": {"status": "fresh"},
                    "solid_clean_oop": {"status": "fresh"},
                },
                "loc_without_tests": {"total_lines": 100, "total_sloc": 80},
                "quality": {"solid": 5.0, "clean_oop": 5.0},
                "industrial_maintainability": {"score": 96},
            },
            {
                "spec": {"slug": "airbyte", "name": "Airbyte", "kind": "oss-core"},
                "metric_groups": {
                    "loc_sloc": {"status": "stale"},
                    "coupling_cohesion": {"status": "stale"},
                },
                "loc_without_tests": {"total_lines": 200, "total_sloc": 160},
                "quality": {"solid": 4.0, "clean_oop": 4.0},
            },
        ],
        "feature_parity": {"summary": {"dpone": {"score": 100}, "fivetran": {"score": 95}}},
    }

    trust = tool.build_evidence_trust_summary(payload)
    dpone = trust["projects"]["dpone"]
    airbyte = trust["projects"]["airbyte"]
    modes = {item["mode"] for item in trust["mode_counts"]}
    provenance = {item["metric_group"]: item for item in trust["metric_provenance"]}

    assert trust["schema_version"] == 1
    assert dpone["confidence_score"] > airbyte["confidence_score"]
    assert dpone["band"] in {"high", "audit-ready"}
    assert airbyte["stale_groups"] >= 1
    assert {"measured", "derived", "inferred", "stale", "closed-core note"} <= modes
    assert provenance["loc_sloc"]["mode"] == "measured"
    assert provenance["industrial_maintainability"]["mode"] == "derived"
    assert trust["provenance_path"] == "docs/benchmarks/data/oss-benchmark-provenance.json"


def test_evidence_trust_markdown_svg_pr_and_provenance_export(tmp_path: Path) -> None:
    tool = _load_tool()
    artifact = _write(tmp_path / "docs" / "benchmarks" / "report.md", "# report\n")
    payload = {
        "generated_at": "2026-06-12T12:00:00+00:00",
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 7, "failed": 0, "checks": []},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
                "dirty": False,
                "loc_with_tests": {"total_lines": 100, "total_sloc": 90, "files": 3},
                "loc_without_tests": {"total_lines": 70, "total_sloc": 60, "files": 2, "max_lines": 450},
                "top_loc_with_tests": [],
                "top_sloc_with_tests": [],
                "top_loc_without_tests": [],
                "top_sloc_without_tests": [],
                "coupling": {"avg_ce": 2.0, "p90_ce": 5.0, "cohesion_ratio": 0.64, "avg_clustering": 0.16},
                "quality": {"solid": 5.0, "clean_oop": 5.0, "evidence": ["small modules"]},
                "coverage_confidence": {"confidence": "high", "score": 100},
                "ci_evidence": {"has_ci": True, "has_coverage_config": True},
                "architecture_risk": {"score": 0, "level": "low", "hotspots": []},
                "industrial_maintainability": {"score": 96, "band": "excellent", "components": {"quality": 35.0}},
                "metric_groups": {"loc_sloc": {"status": "fresh", "last_updated_at": "2026-06-12T12:00:00+00:00"}},
            }
        ],
        "evidence_trust": {
            "schema_version": 1,
            "overall_confidence_score": 94,
            "overall_band": "audit-ready",
            "provenance_path": "docs/benchmarks/data/oss-benchmark-provenance.json",
            "cross_check": {"status": "unavailable", "tool": None, "reason": "no external LOC tool found"},
            "projects": {
                "dpone": {
                    "name": "dpone",
                    "confidence_score": 96,
                    "band": "audit-ready",
                    "fresh_groups": 5,
                    "stale_groups": 0,
                    "unavailable_groups": 0,
                }
            },
            "mode_counts": [
                {"mode": "measured", "count": 5},
                {"mode": "derived", "count": 6},
                {"mode": "inferred", "count": 2},
            ],
            "metric_provenance": [
                {
                    "metric_group": "loc_sloc",
                    "mode": "measured",
                    "collector": "collect_project_metrics",
                    "source": "pinned source tree",
                    "formula_version": "loc-sloc-v1",
                }
            ],
        },
    }

    provenance = tool.build_provenance_export(payload, artifact_paths=[artifact], root=tmp_path)
    markdown = tool.render_markdown(payload)
    svg = tool.render_evidence_confidence_svg(payload)
    summary = tool.render_pr_summary(payload)

    assert provenance["schema_version"] == 1
    assert provenance["reproducibility"]["artifact_checksums"][0]["sha256"]
    assert provenance["reproducibility"]["artifact_checksums"][0]["path"] == "docs/benchmarks/report.md"
    assert provenance["confidence"]["overall_confidence_score"] == 94
    assert provenance["metric_provenance"][0]["metric_group"] == "loc_sloc"
    assert "## Evidence Trust & Auditability" in markdown
    assert "assets/oss-evidence-confidence.svg" in markdown
    assert "Measured vs derived vs inferred" in markdown
    assert "Metric provenance ledger" in markdown
    assert "Reproducibility manifest" in markdown
    assert "<svg" in svg
    assert "Evidence confidence" in svg
    assert "Evidence Trust & Auditability" in summary
    assert "oss-benchmark-provenance.json" in summary


def test_semantic_maintainability_deep_scan_scores_god_objects_and_contracts(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "src" / "demo" / "__init__.py", "")
    _write(
        tmp_path / "src" / "demo" / "contracts.py",
        "from typing import Protocol\n\nclass LoaderPort(Protocol):\n    def load(self) -> None: ...\n",
    )
    service_path = _write(
        tmp_path / "src" / "demo" / "runtime" / "service.py",
        "from demo.runtime.worker_impl import WorkerImpl\n\n"
        "class HugeService:\n"
        "    def __init__(self, loader, sink, clock):\n"
        "        self.loader = loader\n"
        "        self.sink = sink\n"
        "        self.clock = clock\n\n"
        "    def execute(self, rows):\n"
        + "".join(f"        if rows and len(rows) > {idx}:\n            rows = rows[{idx}:]\n" for idx in range(55))
        + "        return rows\n"
        + "\n".join(f"    value_{idx} = {idx}" for idx in range(230))
        + "\n",
    )
    _write(tmp_path / "src" / "demo" / "runtime" / "worker_impl.py", "class WorkerImpl:\n    pass\n")
    project = {
        "spec": {"name": "Demo", "slug": "demo", "kind": "oss-core", "path": str(tmp_path)},
        "loc_without_tests": {"files": 3, "max_lines": 670},
        "top_loc_without_tests": [{"path": "src/demo/runtime/service.py", "lines": 670, "sloc": 610}],
        "coupling": {"max_ce": 18, "cross_slice_ratio": 0.42},
        "quality": {"solid": 3.0, "clean_oop": 3.0},
        "metric_groups": {"loc_sloc": {"status": "fresh"}},
    }

    file_scan = tool.analyze_semantic_file(service_path, tmp_path)
    semantic = tool.analyze_semantic_maintainability([project])
    demo = semantic["summary"]["demo"]

    assert file_scan["max_class_lines"] >= 220
    assert file_scan["max_function_lines"] >= 90
    assert file_scan["constructor_dependency_count"] == 3
    assert semantic["schema_version"] == 1
    assert demo["god_module_count"] >= 1
    assert demo["god_class_count"] >= 1
    assert demo["god_function_count"] >= 1
    assert demo["direct_implementation_imports"] >= 1
    assert demo["interface_density"] > 0
    assert demo["overall_score"] < 100
    assert any(item["kind"] == "direct_implementation_import" for item in demo["boundary_findings"])
    assert any("SOLID" in item["principle"] or "DI" in item["principle"] for item in demo["solid_di_findings"])
    assert semantic["risk_register"]


def test_semantic_maintainability_markdown_svg_and_pr_summary() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-12T12:00:00+00:00",
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 7, "failed": 0, "checks": []},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
                "loc_with_tests": {"total_lines": 100, "total_sloc": 90, "files": 3},
                "loc_without_tests": {"total_lines": 70, "total_sloc": 60, "files": 2, "max_lines": 420},
                "top_loc_with_tests": [],
                "top_sloc_with_tests": [],
                "top_loc_without_tests": [],
                "top_sloc_without_tests": [],
                "coupling": {"avg_ce": 2.0, "p90_ce": 5.0, "cohesion_ratio": 0.64, "avg_clustering": 0.16},
                "quality": {"solid": 5.0, "clean_oop": 5.0, "evidence": ["small modules"]},
                "coverage_confidence": {"confidence": "high", "score": 100},
                "architecture_risk": {"score": 0, "level": "low", "hotspots": []},
                "industrial_maintainability": {"score": 96, "band": "excellent", "components": {"quality": 35.0}},
                "metric_groups": {"loc_sloc": {"status": "fresh", "last_updated_at": "2026-06-12T12:00:00+00:00"}},
            }
        ],
        "semantic_maintainability": {
            "schema_version": 1,
            "summary": {
                "dpone": {
                    "name": "dpone",
                    "overall_score": 92,
                    "status": "excellent",
                    "god_object_score": 94,
                    "solid_di_score": 91,
                    "dry_kiss_score": 90,
                    "boundary_score": 96,
                    "god_module_count": 0,
                    "god_class_count": 0,
                    "god_function_count": 1,
                    "interface_density": 0.08,
                    "direct_implementation_imports": 0,
                    "responsibility_spread": 2,
                    "top_god_objects": [
                        {
                            "kind": "function",
                            "module": "src/dpone/runtime/sinks/clickhouse_sink.py",
                            "name": "load",
                            "value": 62,
                            "unit": "LOC",
                        }
                    ],
                    "solid_di_findings": [{"principle": "DI", "message": "Constructor injection signals detected."}],
                    "dry_kiss_findings": [{"principle": "KISS", "message": "Responsibility spread is controlled."}],
                    "boundary_findings": [],
                    "risk_register": [],
                }
            },
            "risk_register": [],
        },
    }

    markdown = tool.render_markdown(payload)
    semantic_svg = tool.render_semantic_maintainability_svg(payload)
    radar_svg = tool.render_god_object_radar_svg(payload)
    summary = tool.render_pr_summary(payload)

    assert "## Semantic Maintainability Deep Scan" in markdown
    assert "assets/oss-semantic-maintainability.svg" in markdown
    assert "assets/oss-god-object-radar.svg" in markdown
    assert "God object radar" in markdown
    assert "SOLID/DI/Clean Code evidence" in markdown
    assert "DRY/KISS responsibility signals" in markdown
    assert "<svg" in semantic_svg
    assert "Semantic maintainability" in semantic_svg
    assert "<svg" in radar_svg
    assert "God object radar" in radar_svg
    assert "Semantic Maintainability Deep Scan" in summary
    assert "god objects" in summary


def test_scoring_calibration_normalizes_profiles_and_guardrails() -> None:
    tool = _load_tool()
    payload = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
                "loc_without_tests": {"files": 120, "total_sloc": 9000, "max_lines": 430},
                "top_loc_without_tests": [
                    {"path": "src/dpone/runtime/sinks/postgres.py", "lines": 420, "sloc": 350},
                    {"path": "src/dpone/runtime/sources/postgres.py", "lines": 390, "sloc": 310},
                    {"path": "docs/benchmarks/assets/chart.svg", "lines": 40, "sloc": 35},
                ],
                "top_sloc_without_tests": [
                    {"path": "src/dpone/runtime/sinks/postgres.py", "lines": 420, "sloc": 350},
                    {"path": "src/dpone/runtime/sources/postgres.py", "lines": 390, "sloc": 310},
                ],
                "coupling": {"p90_ce": 5.0, "avg_ce": 2.0, "cross_slice_ratio": 0.31},
                "coverage_confidence": {"test_footprint_ratio": 0.42, "score": 90},
                "industrial_maintainability": {"score": 92},
            },
            {
                "spec": {"name": "Airbyte", "slug": "airbyte", "kind": "oss-core"},
                "loc_without_tests": {"files": 5000, "total_sloc": 800000, "max_lines": 41035},
                "top_loc_without_tests": [
                    {
                        "path": "airbyte-server/src/main/java/io/airbyte/server/Server.java",
                        "lines": 41035,
                        "sloc": 39000,
                    },
                    {"path": "airbyte-webapp/src/components/Connection.tsx", "lines": 900, "sloc": 760},
                    {"path": "airbyte-integrations/connectors/source-demo/source.py", "lines": 600, "sloc": 500},
                ],
                "top_sloc_without_tests": [
                    {
                        "path": "airbyte-server/src/main/java/io/airbyte/server/Server.java",
                        "lines": 41035,
                        "sloc": 39000,
                    },
                    {"path": "airbyte-webapp/src/components/Connection.tsx", "lines": 900, "sloc": 760},
                ],
                "coupling": {"p90_ce": 12.0, "avg_ce": 6.5, "cross_slice_ratio": 0.51},
                "coverage_confidence": {"test_footprint_ratio": 0.18, "score": 55},
                "industrial_maintainability": {"score": 70},
            },
        ],
        "semantic_maintainability": {
            "summary": {
                "dpone": {
                    "overall_score": 78,
                    "god_module_count": 0,
                    "god_class_count": 2,
                    "god_function_count": 4,
                    "interface_density": 0.18,
                    "responsibility_spread": 7,
                    "direct_implementation_imports": 1,
                    "dry_kiss_score": 70,
                    "solid_di_score": 85,
                    "boundary_score": 88,
                },
                "airbyte": {
                    "overall_score": 75,
                    "god_module_count": 14,
                    "god_class_count": 9,
                    "god_function_count": 22,
                    "interface_density": 0.09,
                    "responsibility_spread": 18,
                    "direct_implementation_imports": 12,
                    "dry_kiss_score": 63,
                    "solid_di_score": 70,
                    "boundary_score": 66,
                },
            }
        },
    }

    calibration = tool.build_scoring_calibration(payload)
    dpone = calibration["summary"]["dpone"]
    airbyte = calibration["summary"]["airbyte"]
    guardrail_ids = {item["id"] for item in calibration["guardrails"]["dpone"]}
    card = calibration["explanation_cards"]["dpone"]

    assert calibration["schema_version"] == 1
    assert dpone["normalization_profile"] == "python-framework"
    assert dpone["repo_scale"] == "focused-framework"
    assert 0 <= dpone["normalized_score"] <= 100
    assert dpone["normalized_score"] >= dpone["raw_score"] - 10
    assert airbyte["repo_scale"] in {"large-monorepo", "very-large-monorepo"}
    assert calibration["sensitivity"]["dpone"]["threshold_10_percent_swing"] >= 0
    assert calibration["sensitivity"]["dpone"]["rank_stability"] in {"stable", "watch", "volatile"}
    assert {"micro_module_pressure", "hollow_interface_pressure", "score_gaming_resistance"} <= guardrail_ids
    assert card["positive_drivers"]
    assert card["negative_drivers"]
    assert card["next_best_action"]


def test_scoring_calibration_markdown_svg_and_pr_summary() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-12T12:00:00+00:00",
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 7, "failed": 0, "checks": []},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
                "loc_with_tests": {"total_lines": 100, "total_sloc": 90, "files": 3},
                "loc_without_tests": {"total_lines": 70, "total_sloc": 60, "files": 2, "max_lines": 420},
                "top_loc_with_tests": [],
                "top_sloc_with_tests": [],
                "top_loc_without_tests": [],
                "top_sloc_without_tests": [],
                "coupling": {"avg_ce": 2.0, "p90_ce": 5.0, "cohesion_ratio": 0.64, "avg_clustering": 0.16},
                "quality": {"solid": 5.0, "clean_oop": 5.0, "evidence": ["small modules"]},
                "coverage_confidence": {"confidence": "high", "score": 100},
                "architecture_risk": {"score": 0, "level": "low", "hotspots": []},
                "industrial_maintainability": {"score": 96, "band": "excellent", "components": {"quality": 35.0}},
                "metric_groups": {"loc_sloc": {"status": "fresh", "last_updated_at": "2026-06-12T12:00:00+00:00"}},
            }
        ],
        "scoring_calibration": {
            "schema_version": 1,
            "summary": {
                "dpone": {
                    "name": "dpone",
                    "raw_score": 82,
                    "normalized_score": 86,
                    "normalization_profile": "python-framework",
                    "repo_scale": "focused-framework",
                    "calibration_band": "stable",
                    "threshold_10_percent_swing": 3,
                    "language_mix": {"python": 0.92, "jvm": 0.0, "ts_js": 0.0, "other": 0.08},
                    "top_positive_drivers": ["No god modules"],
                    "top_negative_drivers": ["DRY/KISS branch pressure"],
                    "next_best_action": "Reduce class hotspots.",
                }
            },
            "sensitivity": {
                "dpone": {
                    "threshold_10_percent_swing": 3,
                    "rank_stability": "stable",
                    "most_sensitive_metric": "semantic_maintainability",
                }
            },
            "guardrails": {
                "dpone": [
                    {
                        "id": "micro_module_pressure",
                        "label": "Micro-module pressure",
                        "status": "passed",
                        "message": "No micro-module gaming pressure detected.",
                    }
                ]
            },
            "explanation_cards": {
                "dpone": {
                    "headline": "dpone normalizes above raw score because the codebase is focused and Python-heavy.",
                    "positive_drivers": ["No god modules"],
                    "negative_drivers": ["DRY/KISS branch pressure"],
                    "next_best_action": "Reduce class hotspots.",
                }
            },
        },
    }

    markdown = tool.render_markdown(payload)
    calibration_svg = tool.render_score_calibration_svg(payload)
    sensitivity_svg = tool.render_score_sensitivity_svg(payload)
    raw_svg = tool.render_normalized_vs_raw_svg(payload)
    summary = tool.render_pr_summary(payload)

    assert "## Scoring Validity & Calibration" in markdown
    assert "assets/oss-score-calibration.svg" in markdown
    assert "assets/oss-score-sensitivity.svg" in markdown
    assert "assets/oss-normalized-vs-raw.svg" in markdown
    assert "Language/repo normalization" in markdown
    assert "Sensitivity analysis" in markdown
    assert "Anti-gaming guardrails" in markdown
    assert "Score explanation cards" in markdown
    assert "<svg" in calibration_svg
    assert "Score calibration" in calibration_svg
    assert "<svg" in sensitivity_svg
    assert "Sensitivity analysis" in sensitivity_svg
    assert "<svg" in raw_svg
    assert "Normalized vs raw" in raw_svg
    assert "Scoring Validity & Calibration" in summary
    assert "normalized" in summary


def test_scale_readiness_growth_simulates_headroom_and_runway() -> None:
    tool = _load_tool()
    payload = {
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
                "loc_without_tests": {"files": 360, "total_sloc": 120000, "max_lines": 430},
                "coupling": {"p90_ce": 5.0, "max_ce": 13, "cohesion_ratio": 0.64},
                "industrial_maintainability": {"score": 92},
                "coverage_confidence": {"score": 88, "test_footprint_ratio": 0.42},
            },
            {
                "spec": {"name": "dlt", "slug": "dlt", "kind": "oss-core"},
                "loc_without_tests": {"files": 470, "total_sloc": 160000, "max_lines": 780},
                "coupling": {"p90_ce": 7.0, "max_ce": 24, "cohesion_ratio": 0.58},
                "industrial_maintainability": {"score": 78},
            },
            {
                "spec": {"name": "Airbyte", "slug": "airbyte", "kind": "oss-core"},
                "loc_without_tests": {"files": 5000, "total_sloc": 800000, "max_lines": 41035},
                "coupling": {"p90_ce": 12.0, "max_ce": 85, "cohesion_ratio": 0.42},
                "industrial_maintainability": {"score": 70},
            },
            {
                "spec": {"name": "Apache Hop", "slug": "apache-hop", "kind": "oss-core"},
                "loc_without_tests": {"files": 3100, "total_sloc": 520000, "max_lines": 1700},
                "coupling": {"p90_ce": 10.0, "max_ce": 60, "cohesion_ratio": 0.48},
                "industrial_maintainability": {"score": 73},
            },
        ],
        "semantic_maintainability": {
            "summary": {
                "dpone": {
                    "overall_score": 86,
                    "god_module_count": 0,
                    "god_class_count": 1,
                    "god_function_count": 3,
                    "solid_di_score": 88,
                    "dry_kiss_score": 82,
                    "boundary_score": 90,
                }
            }
        },
        "scoring_calibration": {
            "summary": {
                "dpone": {
                    "normalized_score": 82,
                    "raw_score": 92,
                    "normalization_profile": "python-framework",
                    "repo_scale": "product-codebase",
                }
            }
        },
    }

    readiness = tool.build_scale_readiness(payload)
    dpone = readiness["summary"]["dpone"]
    scenarios = readiness["scale_scenarios"]["dpone"]
    scenario_ids = {scenario["id"] for scenario in scenarios}
    headroom = readiness["quality_headroom"]["dpone"]

    assert readiness["schema_version"] == 1
    assert dpone["architecture_runway_score"] >= 0
    assert dpone["growth_ceiling_sloc"] >= 120000
    assert dpone["quality_headroom"]["connector_slots_before_yellow"] >= 0
    assert dpone["overall_status"] in {"ready", "watch", "constrained"}
    assert {"dlt-scale", "airbyte-scale", "hop-scale"} <= scenario_ids
    for scenario in scenarios:
        assert scenario["projected_sloc"] >= 120000
        assert scenario["projected_max_module_loc"] >= 430
        assert scenario["projected_p90_fan_out"] >= 5.0
        assert 0 <= scenario["projected_maintainability"] <= 100
        assert scenario["risk_level"] in {"low", "moderate", "high"}
    assert headroom["connector_slots_before_yellow"] >= 0
    assert headroom["connector_slots_before_red"] >= headroom["connector_slots_before_yellow"]
    assert "warnings" in readiness


def test_scale_readiness_markdown_svg_pr_and_regression_gate_warning() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-12T12:00:00+00:00",
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 7, "failed": 0, "checks": []},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
                "loc_with_tests": {"total_lines": 100, "total_sloc": 90, "files": 3},
                "loc_without_tests": {"total_lines": 70, "total_sloc": 60, "files": 2, "max_lines": 420},
                "top_loc_with_tests": [],
                "top_sloc_with_tests": [],
                "top_loc_without_tests": [],
                "top_sloc_without_tests": [],
                "coupling": {"avg_ce": 2.0, "p90_ce": 5.0, "cohesion_ratio": 0.64, "avg_clustering": 0.16},
                "quality": {"solid": 5.0, "clean_oop": 5.0, "evidence": ["small modules"]},
                "coverage_confidence": {"confidence": "high", "score": 100},
                "architecture_risk": {"score": 0, "level": "low", "hotspots": []},
                "industrial_maintainability": {"score": 96, "band": "excellent", "components": {"quality": 35.0}},
                "metric_groups": {"loc_sloc": {"status": "fresh", "last_updated_at": "2026-06-12T12:00:00+00:00"}},
            }
        ],
        "scale_readiness": {
            "schema_version": 1,
            "summary": {
                "dpone": {
                    "name": "dpone",
                    "architecture_runway_score": 64,
                    "overall_status": "watch",
                    "growth_ceiling_sloc": 380000,
                    "quality_headroom": {
                        "connector_slots_before_yellow": 4,
                        "connector_slots_before_red": 12,
                    },
                    "worst_scenario": "airbyte-scale",
                }
            },
            "quality_headroom": {
                "dpone": {
                    "connector_slots_before_yellow": 4,
                    "connector_slots_before_red": 12,
                    "max_module_loc_headroom": 180,
                    "p90_fan_out_headroom": 3.0,
                    "semantic_score_headroom": 11,
                    "normalized_score_headroom": 7,
                }
            },
            "architecture_runway": {
                "dpone": {
                    "score": 64,
                    "status": "watch",
                    "growth_ceiling_sloc": 380000,
                    "primary_constraint": "p90 fan-out",
                }
            },
            "scale_scenarios": {
                "dpone": [
                    {
                        "id": "dlt-scale",
                        "label": "dlt scale",
                        "growth_multiplier": 1.3,
                        "projected_sloc": 156000,
                        "projected_max_module_loc": 520,
                        "projected_p90_fan_out": 6.0,
                        "projected_maintainability": 82,
                        "risk_level": "low",
                    },
                    {
                        "id": "airbyte-scale",
                        "label": "Airbyte scale",
                        "growth_multiplier": 6.7,
                        "projected_sloc": 800000,
                        "projected_max_module_loc": 1200,
                        "projected_p90_fan_out": 14.0,
                        "projected_maintainability": 62,
                        "risk_level": "high",
                    },
                ]
            },
            "warnings": [
                {
                    "id": "architecture_runway_low",
                    "project": "dpone",
                    "label": "Architecture runway score",
                    "severity": "warning",
                    "message": "Runway below warning threshold.",
                    "current": 64,
                    "threshold": 70,
                }
            ],
        },
    }

    markdown = tool.render_markdown(payload)
    readiness_svg = tool.render_scale_readiness_svg(payload)
    runway_svg = tool.render_architecture_runway_svg(payload)
    headroom_svg = tool.render_quality_headroom_svg(payload)
    summary = tool.render_pr_summary(payload)
    gate = tool.build_pr_regression_gate(payload)

    assert "## Scale Readiness & Growth Simulation" in markdown
    assert "assets/oss-scale-readiness.svg" in markdown
    assert "assets/oss-architecture-runway.svg" in markdown
    assert "assets/oss-quality-headroom.svg" in markdown
    assert "Quality headroom" in markdown
    assert "Architecture runway" in markdown
    assert "Scale scenarios" in markdown
    assert "Comparator-scale projection" in markdown
    assert "<svg" in readiness_svg
    assert "Scale readiness" in readiness_svg
    assert "<svg" in runway_svg
    assert "Architecture runway" in runway_svg
    assert "<svg" in headroom_svg
    assert "Quality headroom" in headroom_svg
    assert "Scale Readiness & Growth Simulation" in summary
    assert any(check["source"] == "scale_readiness" and check["severity"] == "warning" for check in gate["checks"])


def test_independent_validation_builds_analyzer_audit_pack() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-12T12:00:00+00:00",
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
                "loc_with_tests": {"total_lines": 160000, "total_sloc": 120000, "files": 420},
                "loc_without_tests": {"total_lines": 125000, "total_sloc": 90000, "max_lines": 430},
                "coupling": {"p90_ce": 5.0, "max_ce": 13},
                "industrial_maintainability": {"score": 92},
            }
        ],
        "external_analyzer_results": [
            {
                "tool": "tokei",
                "project": "dpone",
                "status": "fresh",
                "command": "tokei --output json .",
                "version": "13.0.0",
                "exit_code": 0,
                "generated_at": "2026-06-12T12:00:00+00:00",
                "metrics": {"total_sloc": 118800, "total_lines": 159100},
            },
            {
                "tool": "radon",
                "project": "dpone",
                "status": "fresh",
                "command": "radon cc -j src",
                "version": "6.0.1",
                "exit_code": 0,
                "generated_at": "2026-06-12T12:00:00+00:00",
                "metrics": {"avg_complexity": 4.1, "max_complexity": 18, "files": 180},
            },
            {
                "tool": "lizard",
                "project": "dpone",
                "status": "unavailable",
                "command": "lizard src",
                "version": None,
                "exit_code": None,
                "generated_at": "2026-06-12T12:00:00+00:00",
                "error": "lizard not installed",
                "metrics": {},
            },
        ],
    }

    validation = tool.build_independent_validation(payload)
    dpone = validation["summary"]["dpone"]
    commands = validation["analyzer_commands"]
    loc_check = validation["loc_sloc_cross_checks"]["dpone"][0]
    complexity_check = validation["complexity_cross_checks"]["dpone"][0]
    coverage = validation["analyzer_coverage"]["dpone"]

    assert validation["schema_version"] == 1
    assert dpone["confidence_score"] >= 80
    assert dpone["validation_band"] in {"audit-ready", "high"}
    assert any(command["tool"] == "tokei" and command["exit_code"] == 0 for command in commands)
    assert all({"tool", "status", "command", "generated_at"} <= set(command) for command in commands)
    assert loc_check["benchmark_sloc"] == 120000
    assert loc_check["external_sloc"] == 118800
    assert loc_check["delta_percent"] <= 2
    assert loc_check["status"] == "passed"
    assert complexity_check["external_avg_complexity"] == 4.1
    assert complexity_check["status"] == "passed"
    assert coverage["unavailable_tools"] == ["lizard"]
    assert coverage["validated_tool_count"] == 2


def test_external_analyzer_parsers_normalize_tool_outputs() -> None:
    tool = _load_tool()

    tokei = tool.TokeiParser().parse(json.dumps({"Total": {"code": 1188, "comments": 44, "blanks": 68, "files": 12}}))
    cloc = tool.ClocParser().parse(json.dumps({"SUM": {"code": 1195, "comment": 40, "blank": 65, "nFiles": 13}}))
    radon = tool.RadonParser().parse(
        json.dumps(
            {
                "src/a.py": [{"complexity": 4}, {"complexity": 12}],
                "src/b.py": [{"complexity": 2}],
            }
        )
    )
    lizard = tool.LizardParser().parse(
        """
        <cppncss>
          <measure type="Function">
            <item name="alpha" cyclomatic_complexity="3" />
            <item name="beta" cyclomatic_complexity="11" />
          </measure>
        </cppncss>
        """
    )
    lizard_table_xml = tool.LizardParser().parse(
        """
        <cppncss>
          <measure type="Function">
            <labels>
              <label>Nr.</label>
              <label>NCSS</label>
              <label>CCN</label>
            </labels>
            <item name="alpha">
              <value>1</value>
              <value>9</value>
              <value>3</value>
            </item>
            <item name="beta">
              <value>2</value>
              <value>12</value>
              <value>11</value>
            </item>
          </measure>
        </cppncss>
        """
    )

    assert tokei == {"total_sloc": 1188, "total_lines": 1300, "files": 12}
    assert cloc == {"total_sloc": 1195, "total_lines": 1300, "files": 13}
    assert radon == {"avg_complexity": 6.0, "max_complexity": 12.0, "files": 2}
    assert lizard == {"avg_complexity": 7.0, "max_complexity": 11.0, "files": 2}
    assert lizard_table_xml == {"avg_complexity": 7.0, "max_complexity": 11.0, "files": 2}


def test_independent_validation_marks_fresh_zero_complexity_as_not_applicable() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-12T12:00:00+00:00",
        "projects": [
            {
                "spec": {"name": "Apache Hop", "slug": "apache-hop", "kind": "oss-core"},
                "loc_with_tests": {"total_lines": 1000, "total_sloc": 800, "files": 20},
                "loc_without_tests": {"total_lines": 900, "total_sloc": 700, "max_lines": 120},
            }
        ],
        "external_analyzer_results": [
            {
                "tool": "radon",
                "project": "apache-hop",
                "status": "fresh",
                "command": "radon cc -j .",
                "exit_code": 0,
                "generated_at": "2026-06-12T12:00:00+00:00",
                "metrics": {"avg_complexity": 0.0, "max_complexity": 0.0, "files": 0},
            },
            {
                "tool": "lizard",
                "project": "apache-hop",
                "status": "fresh",
                "command": "lizard --xml .",
                "exit_code": 0,
                "generated_at": "2026-06-12T12:00:00+00:00",
                "metrics": {"avg_complexity": 4.0, "max_complexity": 22.0, "files": 40},
            },
        ],
    }

    validation = tool.build_independent_validation(payload)
    checks = {check["tool"]: check for check in validation["complexity_cross_checks"]["apache-hop"]}

    assert checks["radon"]["status"] == "not_applicable"
    assert checks["lizard"]["status"] == "passed"
    assert validation["summary"]["apache-hop"]["complexity_status"] == "passed"
    assert validation["summary"]["apache-hop"]["unavailable_analyzers"] == 0


def test_external_analyzer_collection_executes_tools_and_preserves_stale_values(tmp_path: Path) -> None:
    tool = _load_tool()
    project_path = tmp_path / "project"
    project_path.mkdir()
    project = {
        "spec": {"name": "dpone", "slug": "dpone", "path": str(project_path)},
        "loc_with_tests": {"total_sloc": 1200, "total_lines": 1300},
    }
    previous_payload = {
        "independent_validation": {
            "loc_sloc_cross_checks": {
                "dpone": [
                    {
                        "tool": "cloc",
                        "status": "passed",
                        "benchmark_sloc": 1200,
                        "external_sloc": 1194,
                        "benchmark_lines": 1300,
                        "external_lines": 1294,
                        "delta_percent": 0.5,
                        "last_updated_at": "2026-06-12T12:00:00+00:00",
                    }
                ]
            },
            "complexity_cross_checks": {
                "dpone": [
                    {
                        "tool": "radon",
                        "status": "passed",
                        "external_avg_complexity": 4.2,
                        "external_max_complexity": 13,
                        "files": 7,
                        "last_updated_at": "2026-06-12T12:00:00+00:00",
                    }
                ]
            },
        }
    }

    class FakeRunner:
        def run(self, args: list[str], *, cwd: Path, timeout_seconds: int) -> tool.AnalyzerProcessResult:
            assert cwd == project_path
            assert timeout_seconds == 45
            if args[0].endswith("tokei"):
                return tool.AnalyzerProcessResult(
                    exit_code=0,
                    stdout=json.dumps({"Total": {"code": 1188, "comments": 44, "blanks": 68, "files": 12}}),
                    stderr="",
                )
            raise AssertionError(f"unexpected command: {args}")

    def resolver(name: str) -> str | None:
        return "/usr/bin/tokei" if name == "tokei" else None

    results = tool.collect_external_analyzer_results(
        [project],
        generated_at="2026-06-14T00:00:00+00:00",
        previous_payload=previous_payload,
        command_runner=FakeRunner(),
        tool_resolver=resolver,
        timeout_seconds=45,
    )
    validation = tool.build_independent_validation(
        {
            "generated_at": "2026-06-14T00:00:00+00:00",
            "projects": [project],
            "external_analyzer_results": results,
        }
    )

    by_tool = {result["tool"]: result for result in results}
    assert by_tool["tokei"]["status"] == "fresh"
    assert by_tool["tokei"]["metrics"]["total_sloc"] == 1188
    assert by_tool["cloc"]["status"] == "stale"
    assert by_tool["cloc"]["metrics"]["total_sloc"] == 1194
    assert by_tool["cloc"]["last_updated_at"] == "2026-06-12T12:00:00+00:00"
    assert by_tool["radon"]["status"] == "stale"
    assert by_tool["radon"]["metrics"]["avg_complexity"] == 4.2
    assert by_tool["lizard"]["status"] == "unavailable"
    assert validation["loc_sloc_cross_checks"]["dpone"][1]["status"] == "stale"
    assert validation["complexity_cross_checks"]["dpone"][0]["status"] == "stale"
    assert validation["analyzer_coverage"]["dpone"]["stale_tools"] == ["cloc", "radon"]


def test_independent_validation_markdown_svg_and_pr_summary() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-12T12:00:00+00:00",
        "run_context": {"generated_at": "2026-06-12T12:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 7, "failed": 0, "checks": []},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
                "loc_with_tests": {"total_lines": 100, "total_sloc": 90, "files": 3},
                "loc_without_tests": {"total_lines": 70, "total_sloc": 60, "files": 2, "max_lines": 420},
                "top_loc_with_tests": [],
                "top_sloc_with_tests": [],
                "top_loc_without_tests": [],
                "top_sloc_without_tests": [],
                "coupling": {"avg_ce": 2.0, "p90_ce": 5.0, "cohesion_ratio": 0.64, "avg_clustering": 0.16},
                "quality": {"solid": 5.0, "clean_oop": 5.0, "evidence": ["small modules"]},
                "coverage_confidence": {"confidence": "high", "score": 100},
                "architecture_risk": {"score": 0, "level": "low", "hotspots": []},
                "industrial_maintainability": {"score": 96, "band": "excellent", "components": {"quality": 35.0}},
                "metric_groups": {"loc_sloc": {"status": "fresh", "last_updated_at": "2026-06-12T12:00:00+00:00"}},
            }
        ],
        "independent_validation": {
            "schema_version": 1,
            "summary": {
                "dpone": {
                    "name": "dpone",
                    "confidence_score": 92,
                    "validation_band": "audit-ready",
                    "loc_sloc_status": "passed",
                    "complexity_status": "passed",
                    "unavailable_analyzers": 1,
                }
            },
            "analyzer_commands": [
                {
                    "tool": "tokei",
                    "project": "dpone",
                    "status": "fresh",
                    "command": "tokei --output json .",
                    "version": "13.0.0",
                    "exit_code": 0,
                    "generated_at": "2026-06-12T12:00:00+00:00",
                }
            ],
            "loc_sloc_cross_checks": {
                "dpone": [
                    {
                        "tool": "tokei",
                        "status": "passed",
                        "benchmark_sloc": 90000,
                        "external_sloc": 89500,
                        "delta_percent": 0.56,
                    }
                ]
            },
            "complexity_cross_checks": {
                "dpone": [
                    {
                        "tool": "radon",
                        "status": "passed",
                        "external_avg_complexity": 4.1,
                        "external_max_complexity": 18,
                    }
                ]
            },
            "analyzer_coverage": {
                "dpone": {
                    "validated_tool_count": 2,
                    "unavailable_tools": ["lizard"],
                    "coverage_status": "high",
                }
            },
            "warnings": [],
        },
    }

    markdown = tool.render_markdown(payload)
    validation_svg = tool.render_independent_validation_svg(payload)
    confidence_svg = tool.render_analyzer_confidence_svg(payload)
    summary = tool.render_pr_summary(payload)

    assert "## Independent Analyzer Cross-Validation & Audit Pack" in markdown
    assert "assets/oss-independent-validation.svg" in markdown
    assert "assets/oss-analyzer-confidence.svg" in markdown
    assert "Analyzer command ledger" in markdown
    assert "LOC/SLOC cross-check" in markdown
    assert "Complexity cross-check" in markdown
    assert "Validation confidence" in markdown
    assert "<svg" in validation_svg
    assert "Independent analyzer validation" in validation_svg
    assert "<svg" in confidence_svg
    assert "Analyzer confidence" in confidence_svg
    assert "Independent Analyzer Cross-Validation" in summary


def test_public_evidence_integrity_sanitizes_machine_specific_paths() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-14T00:00:00+00:00",
        "projects": [
            {
                "spec": {
                    "slug": "dpone",
                    "name": "dpone",
                    "path": "/Users/example-user/data-platform-dpone",
                }
            },
            {
                "spec": {
                    "slug": "airbyte",
                    "name": "Airbyte",
                    "path": "/Users/example-user/data-platform-dpone/.cache/oss-code-quality-benchmark/airbyte",
                }
            },
        ],
        "external_analyzer_results": [
            {"command": "tokei --output json /Users/example-user/data-platform-dpone", "status": "fresh"}
        ],
        "debug": {"tmp": "/private/tmp/data-platform-dpone/cache"},
    }

    sanitized = tool.sanitize_public_payload(payload)
    encoded = json.dumps(sanitized, sort_keys=True)
    violations = tool.find_public_redaction_violations(sanitized)

    assert "/Users/" not in encoded
    assert "/private/tmp" not in encoded
    assert "data-platform-dpone" not in encoded
    assert sanitized["projects"][0]["spec"]["path"] == "$WORKSPACE"
    assert sanitized["projects"][1]["spec"]["path"] == "$BENCHMARK_CACHE/airbyte"
    assert sanitized["external_analyzer_results"][0]["command"] == "tokei --output json $WORKSPACE"
    assert violations == []


def test_public_evidence_integrity_builds_claim_ledger_and_score() -> None:
    tool = _load_tool()
    payload = tool.sanitize_public_payload(
        {
            "generated_at": "2026-06-14T00:00:00+00:00",
            "projects": [
                {"spec": {"slug": "dpone", "name": "dpone", "path": "/Users/example-user/data-platform-dpone"}}
            ],
            "feature_parity": {"tools": [{"slug": "dpone"}, {"slug": "airbyte"}], "entries": []},
            "closed_core_notes": [{"name": "Fivetran", "url": "https://github.com/fivetran"}],
            "trust_center": {"status": "verified"},
            "quality_gates": {"status": "passed"},
            "independent_validation": {"summary": {"dpone": {"confidence_score": 80}}},
            "external_analyzer_results": [{"tool": "tokei", "status": "fresh"}],
        }
    )

    integrity = tool.build_public_evidence_integrity(payload)
    claims = {item["claim_id"]: item for item in integrity["claim_evidence"]}

    assert integrity["schema_version"] == 1
    assert integrity["status"] == "verified"
    assert integrity["score"] >= 95
    assert integrity["redaction_violation_count"] == 0
    assert integrity["claim_coverage_percent"] == 100
    assert {
        "feature_parity_public_sources",
        "closed_core_comparator_scope",
        "customer_trust_center_verified",
        "quality_gates_passed",
        "external_analyzer_execution",
        "dpone_position_static_quality",
    } <= set(claims)
    assert all(claim["source"] for claim in claims.values())
    assert all(claim["last_checked_at"] == "2026-06-14T00:00:00+00:00" for claim in claims.values())


def test_public_evidence_integrity_markdown_svg_and_pr_summary() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-14T00:00:00+00:00",
        "run_context": {"generated_at": "2026-06-14T00:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 1, "failed": 0, "checks": []},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone", "kind": "local-framework", "path": "$WORKSPACE"},
                "loc_with_tests": {"total_lines": 10, "total_sloc": 8},
                "loc_without_tests": {"total_lines": 8, "total_sloc": 6, "max_lines": 120},
                "top_loc_with_tests": [],
                "top_sloc_with_tests": [],
                "top_loc_without_tests": [],
                "top_sloc_without_tests": [],
                "coupling": {"avg_ce": 1.0},
                "quality": {"solid": 5, "clean_oop": 5, "evidence": []},
                "coverage_confidence": {"confidence": "high", "score": 100},
                "architecture_risk": {"score": 0, "level": "low", "hotspots": []},
                "industrial_maintainability": {"score": 99, "band": "excellent", "components": {}},
                "metric_groups": {},
            }
        ],
        "public_evidence_integrity": {
            "schema_version": 1,
            "status": "verified",
            "score": 100,
            "redaction_violation_count": 0,
            "claim_coverage_percent": 100,
            "claim_evidence": [
                {
                    "claim_id": "feature_parity_public_sources",
                    "section": "Feature Parity Matrix",
                    "claim": "Feature claims are backed by public source references.",
                    "confidence": "vendor-public",
                    "source": "feature_parity.entries[].sources",
                    "last_checked_at": "2026-06-14T00:00:00+00:00",
                    "status": "covered",
                }
            ],
            "redaction_policy": {
                "forbidden_patterns": ["/Users/", "/private/tmp"],
                "public_path_tokens": ["$WORKSPACE"],
            },
        },
    }

    markdown = tool.render_markdown(payload)
    svg = tool.render_public_evidence_integrity_svg(payload)
    summary = tool.render_pr_summary(payload)

    assert "## Public Evidence Integrity" in markdown
    assert "claim evidence ledger" in markdown
    assert "redaction violations" in markdown
    assert "assets/oss-public-evidence-integrity.svg" in markdown
    assert "<svg" in svg
    assert "Public evidence integrity" in svg
    assert "Public Evidence Integrity" in summary
    assert "claim coverage" in summary


def test_source_verification_registry_links_claims_and_sources() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-14T00:00:00+00:00",
        "closed_core_notes": [{"name": "Fivetran", "url": "https://github.com/fivetran"}],
        "feature_parity": {
            "entries": [
                {
                    "tool": "airbyte",
                    "dimension": "cdc",
                    "sources": ["https://docs.airbyte.com/platform/understanding-airbyte/cdc"],
                },
                {"tool": "dpone", "dimension": "cdc", "sources": ["docs/cdc.md"]},
            ]
        },
        "public_evidence_integrity": {
            "claim_evidence": [
                {
                    "claim_id": "feature_parity_public_sources",
                    "source": "feature_parity.entries[].sources",
                    "status": "covered",
                },
                {
                    "claim_id": "closed_core_comparator_scope",
                    "source": "closed_core_notes[].url",
                    "status": "covered",
                },
            ]
        },
    }

    registry = tool.build_source_registry(payload)

    source_values = {source["value"]: source for source in registry}
    assert "https://docs.airbyte.com/platform/understanding-airbyte/cdc" in source_values
    assert "docs/cdc.md" in source_values
    assert "https://github.com/fivetran" in source_values
    assert source_values["docs/cdc.md"]["source_type"] == "local-doc"
    assert (
        "feature_parity_public_sources"
        in source_values["https://docs.airbyte.com/platform/understanding-airbyte/cdc"]["linked_claim_ids"]
    )
    assert "closed_core_comparator_scope" in source_values["https://github.com/fivetran"]["linked_claim_ids"]


def test_source_verification_preserves_previous_source_when_refresh_fails() -> None:
    tool = _load_tool()

    class FakeVerifier:
        def verify(
            self,
            source: dict[str, object],
            *,
            checked_at: str,
            verify_urls: bool,
        ) -> object:
            if str(source["value"]).endswith("/ok"):
                return tool.SourceCheckResult(
                    status="verified",
                    last_checked_at=checked_at,
                    content_hash="sha256:ok",
                    verification_mode="live-url",
                )
            return tool.SourceCheckResult(
                status="unavailable",
                last_checked_at=checked_at,
                verification_mode="live-url",
                last_error="404",
            )

    payload = {
        "generated_at": "2026-06-14T00:00:00+00:00",
        "feature_parity": {
            "entries": [
                {"tool": "dpone", "dimension": "ok", "sources": ["https://example.com/ok"]},
                {"tool": "dpone", "dimension": "bad", "sources": ["https://example.com/bad"]},
            ]
        },
    }
    previous_id = tool.source_id_for("https://example.com/bad")
    previous = {
        "source_verification": {
            "sources": [
                {
                    "source_id": previous_id,
                    "value": "https://example.com/bad",
                    "status": "verified",
                    "last_updated_at": "2026-06-01T00:00:00+00:00",
                    "last_checked_at": "2026-06-01T00:00:00+00:00",
                    "content_hash": "sha256:old",
                    "linked_claim_ids": ["feature:dpone:bad"],
                }
            ]
        }
    }

    verification = tool.build_source_verification(
        payload,
        previous_payload=previous,
        verifier=FakeVerifier(),
        verify_urls=True,
    )

    by_value = {item["value"]: item for item in verification["sources"]}
    ok = by_value["https://example.com/ok"]
    bad = by_value["https://example.com/bad"]
    assert ok["status"] == "verified"
    assert ok["content_hash"] == "sha256:ok"
    assert bad["status"] == "stale"
    assert bad["last_updated_at"] == "2026-06-01T00:00:00+00:00"
    assert bad["content_hash"] == "sha256:old"
    assert bad["refresh_attempted_at"] == "2026-06-14T00:00:00+00:00"
    assert bad["last_error"] == "404"
    assert verification["summary"]["stale_count"] == 1


def test_source_verification_marks_unavailable_without_previous_evidence() -> None:
    tool = _load_tool()

    class FailingVerifier:
        def verify(
            self,
            source: dict[str, object],
            *,
            checked_at: str,
            verify_urls: bool,
        ) -> object:
            return tool.SourceCheckResult(
                status="unavailable",
                last_checked_at=checked_at,
                verification_mode="live-url",
                last_error=f"{source['value']} did not respond",
            )

    verification = tool.build_source_verification(
        {
            "generated_at": "2026-06-14T00:00:00+00:00",
            "feature_parity": {
                "entries": [{"tool": "dlt", "dimension": "schema", "sources": ["https://example.com/missing"]}]
            },
        },
        verifier=FailingVerifier(),
        verify_urls=True,
    )

    source = verification["sources"][0]
    assert source["status"] == "unavailable"
    assert source["last_updated_at"] is None
    assert "did not respond" in source["last_error"]
    assert verification["summary"]["unavailable_count"] == 1


def test_source_verification_markdown_svg_and_pr_summary() -> None:
    tool = _load_tool()
    payload = {
        "run_context": {"generated_at": "2026-06-14T00:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 1, "failed": 0, "checks": []},
        "projects": [],
        "source_verification": {
            "schema_version": 1,
            "status": "verified",
            "summary": {
                "source_count": 2,
                "verified_count": 2,
                "stale_count": 0,
                "unavailable_count": 0,
                "claim_traceability_percent": 100,
                "source_health_score": 100,
            },
            "claim_matrix": [
                {
                    "claim_id": "feature_parity_public_sources",
                    "source_count": 2,
                    "source_ids": ["src_airbyte", "src_dpone"],
                    "status": "verified",
                }
            ],
            "sources": [
                {
                    "source_id": "src_airbyte",
                    "value": "https://docs.airbyte.com/platform/understanding-airbyte/cdc",
                    "source_type": "url",
                    "status": "verified",
                    "linked_claim_ids": ["feature_parity_public_sources"],
                    "last_checked_at": "2026-06-14T00:00:00+00:00",
                    "verification_mode": "metadata-url",
                }
            ],
        },
    }

    markdown = tool.render_source_verification_section(payload)
    svg = tool.render_source_verification_svg(payload)
    summary = tool.render_pr_summary(payload)

    assert "Source Citation Verification" in markdown
    assert "claim-to-source matrix" in markdown
    assert "Source health" in markdown
    assert "<svg" in svg
    assert "Source citation verification" in svg
    assert "Source Citation Verification" in summary


def test_benchmark_release_readiness_seals_verified_v3_evidence() -> None:
    tool = _load_tool()
    payload = {
        "generated_at": "2026-06-14T00:00:00+00:00",
        "run_context": {"updated_by": "codex-local", "git_sha": "abc123", "branch": "benchmark"},
        "quality_gates": {"status": "passed"},
        "public_evidence_integrity": {"status": "verified", "score": 100, "redaction_violation_count": 0},
        "source_verification": {"status": "verified", "summary": {"source_health_score": 100}},
        "evidence_trust": {"overall_confidence_score": 96, "overall_band": "audit-ready"},
        "trust_center": {"status": "verified"},
        "claims_ledger": {"summary": {"verified": 4, "unverified": 0}},
        "runtime_certification": {"summary": {"passed": 4, "failed": 0}, "scenarios": [{"scenario_id": "release"}]},
        "quality_budgets": {"status": "warning"},
        "evidence_exports": {"files": ["projects.csv"]},
        "projects": [{"spec": {"slug": "dpone"}}],
    }

    readiness = tool.build_benchmark_release_readiness(payload)

    assert readiness["schema_version"] == 1
    assert readiness["release_stage"] == "benchmark-v3"
    assert readiness["status"] == "release-ready"
    assert readiness["evidence_seal"]["label"] == "Benchmark v3 verified"
    assert readiness["evidence_seal"]["score"] == 100
    assert "loc_sloc" in readiness["freeze_policy"]["stable_metric_groups"]
    assert "scale_readiness" in readiness["freeze_policy"]["experimental_metric_groups"]
    assert "claims_ledger" in readiness["freeze_policy"]["stable_metric_groups"]
    assert all(check["status"] == "passed" for check in readiness["release_checks"])


def test_benchmark_release_readiness_blocks_red_public_evidence() -> None:
    tool = _load_tool()

    readiness = tool.build_benchmark_release_readiness(
        {
            "generated_at": "2026-06-14T00:00:00+00:00",
            "quality_gates": {"status": "passed"},
            "public_evidence_integrity": {"status": "blocked", "score": 45, "redaction_violation_count": 2},
            "source_verification": {"status": "verified", "summary": {"source_health_score": 100}},
            "evidence_trust": {"overall_confidence_score": 96},
            "trust_center": {"status": "verified"},
        }
    )

    checks = {check["id"]: check for check in readiness["release_checks"]}
    assert readiness["status"] == "blocked"
    assert checks["public_evidence_integrity"]["status"] == "failed"
    assert readiness["evidence_seal"]["label"] == "Benchmark v3 blocked"


def test_release_readiness_markdown_svg_and_pr_summary() -> None:
    tool = _load_tool()
    payload = {
        "run_context": {"generated_at": "2026-06-14T00:00:00+00:00", "updated_by": "paul"},
        "freshness_summary": {"fresh": 1, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 1, "failed": 0, "checks": []},
        "projects": [],
        "benchmark_release_readiness": {
            "schema_version": 1,
            "status": "release-ready",
            "release_stage": "benchmark-v3",
            "recommended_action": "Release the benchmark as v3.",
            "evidence_seal": {
                "label": "Benchmark v3 verified",
                "score": 100,
                "generated_at": "2026-06-14T00:00:00+00:00",
            },
            "freeze_policy": {
                "stable_metric_groups": ["loc_sloc"],
                "experimental_metric_groups": ["scale_readiness"],
            },
            "release_checks": [
                {"id": "quality_gates", "label": "Quality gates", "status": "passed", "evidence": "passed"}
            ],
        },
    }

    markdown = tool.render_release_readiness_section(payload)
    standalone = tool.render_release_readiness_markdown(payload["benchmark_release_readiness"])
    svg = tool.render_release_readiness_svg(payload)
    summary = tool.render_pr_summary(payload)

    assert "Benchmark v3 Release Readiness" in markdown
    assert "Benchmark v3 verified" in markdown
    assert "stable metric groups" in markdown
    assert "Benchmark v3 Release Readiness" in standalone
    assert "<svg" in svg
    assert "Benchmark v3 release readiness" in svg
    assert "Benchmark v3 Release Readiness" in summary


def test_architecture_delta_svg_renders_governance_changes() -> None:
    tool = _load_tool()
    payload = {
        "architecture_delta": {
            "project": "dpone",
            "status": "improved",
            "items": [
                {
                    "id": "max_ce",
                    "label": "Max fan-out",
                    "previous": 18,
                    "current": 13,
                    "delta": -5,
                    "direction": "lower_is_better",
                    "status": "improved",
                },
                {
                    "id": "largest_module_loc",
                    "label": "Largest module LOC",
                    "previous": 520,
                    "current": 453,
                    "delta": -67,
                    "direction": "lower_is_better",
                    "status": "improved",
                },
            ],
        }
    }

    svg = tool.render_architecture_delta_svg(payload)

    assert "<svg" in svg
    assert "Architecture delta" in svg
    assert "Max fan-out" in svg
    assert "-5" in svg
    assert "Largest module LOC" in svg


def test_trust_center_export_summarizes_customer_ready_certification() -> None:
    tool = _load_tool()
    payload = {
        "run_context": {
            "generated_at": "2026-06-12T12:00:00+00:00",
            "updated_by": "paul",
            "run_url": "https://github.example/run/123",
            "git_sha": "abc123",
            "branch": "feature/benchmark",
        },
        "freshness_summary": {"fresh": 5, "stale": 0, "unavailable": 0},
        "quality_gates": {"status": "passed", "passed": 7, "failed": 0},
        "projects": [
            {
                "spec": {"name": "dpone", "slug": "dpone"},
                "industrial_maintainability": {"score": 95, "band": "excellent"},
                "coverage_confidence": {"score": 92, "confidence": "high"},
                "architecture_risk": {"score": 16, "level": "low"},
                "quality": {"solid": 5.0, "clean_oop": 5.0},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
            }
        ],
        "governance_compliance": {"summary": {"dpone": {"score": 100, "band": "leader"}}},
        "security_supply_chain": {"summary": {"dpone": {"score": 100, "band": "excellent"}}},
        "operational_reliability": {"summary": {"dpone": {"score": 95, "band": "excellent"}}},
        "operability_tco": {"summary": {"dpone": {"score": 100, "band": "excellent"}}},
    }

    export = tool.build_trust_center_export(payload)

    assert export["schema_version"] == 1
    assert export["status"] == "verified"
    assert export["badge"]["label"] == "dpone verified"
    assert export["badge"]["score"] == 95
    assert export["run_context"]["updated_by"] == "paul"
    assert {control["slug"] for control in export["controls"]} == {
        "maintainability",
        "governance_compliance",
        "security_supply_chain",
        "operational_reliability",
        "operability_tco",
        "coverage_confidence",
        "architecture_risk",
    }
    assert any("oss-code-quality-benchmark-2026-06-12.json" in link["href"] for link in export["evidence_links"])
    assert "static maintainability" in export["limitations"][0].lower()


def test_trust_center_renderers_create_markdown_and_badge() -> None:
    tool = _load_tool()
    export = {
        "schema_version": 1,
        "title": "dpone Trust Center Snapshot",
        "status": "verified",
        "summary": "Customer-ready certification snapshot for dpone.",
        "run_context": {
            "generated_at": "2026-06-12T12:00:00+00:00",
            "updated_by": "paul",
            "run_url": "https://github.example/run/123",
            "git_sha": "abc123",
            "branch": "feature/benchmark",
        },
        "badge": {"label": "dpone verified", "score": 95, "band": "excellent"},
        "quality_gates": {"status": "passed", "passed": 7, "failed": 0},
        "freshness": {"fresh": 5, "stale": 0, "unavailable": 0},
        "controls": [
            {"slug": "governance_compliance", "label": "Governance & Compliance", "score": 100, "band": "leader"},
            {"slug": "security_supply_chain", "label": "Security & Supply Chain", "score": 100, "band": "excellent"},
        ],
        "evidence_links": [
            {"label": "Open benchmark evidence", "href": "data/oss-code-quality-benchmark-2026-06-12.json"}
        ],
        "limitations": ["Static maintainability proxies, not runtime performance claims."],
    }

    markdown = tool.render_trust_center_markdown(export)
    svg = tool.render_trust_center_badge_svg(export)

    assert "dpone Trust Center Snapshot" in markdown
    assert "Customer-ready certification" in markdown
    assert "Last refresh" in markdown
    assert "Generated by" in markdown
    assert "Quality gates: `passed`" in markdown
    assert "Governance & Compliance" in markdown
    assert "[Open benchmark evidence](data/oss-code-quality-benchmark-2026-06-12.json)" in markdown
    assert "<svg" in svg
    assert "dpone verified" in svg
    assert "95" in svg


def test_feature_parity_matrix_scores_enterprise_capabilities() -> None:
    tool = _load_tool()

    matrix = tool.build_feature_parity_matrix()

    assert matrix["schema_version"] == 1
    assert len(matrix["dimensions"]) >= 10
    slugs = {tool_entry["slug"] for tool_entry in matrix["tools"]}
    assert slugs == {
        "dpone",
        "airbyte",
        "dlt",
        "pentaho-kettle",
        "apache-hop",
        "sling",
        "fivetran",
        "informatica",
    }
    assert matrix["summary"]["dpone"]["score"] >= matrix["summary"]["airbyte"]["score"]
    assert matrix["summary"]["fivetran"]["code_comparable"] is False
    assert matrix["summary"]["informatica"]["code_comparable"] is False
    assert all(entry["sources"] for entry in matrix["entries"])
    assert any(
        entry["dimension"] == "certification_evidence" and entry["tool"] == "dpone" for entry in matrix["entries"]
    )
    assert "evidence-bounded" in matrix["methodology"].lower()


def test_feature_parity_renderers_include_closed_core_and_sources() -> None:
    tool = _load_tool()
    matrix = tool.build_feature_parity_matrix()

    markdown = tool.render_feature_parity_section(matrix)
    svg = tool.render_feature_parity_svg({"feature_parity": matrix})

    assert "Feature Parity Matrix" in markdown
    assert "Fivetran" in markdown
    assert "Informatica" in markdown
    assert "closed-core feature comparator" in markdown
    assert "Certification evidence" in markdown
    assert "https://fivetran.com/docs/core-concepts/features" in markdown
    assert "<svg" in svg
    assert "Feature parity coverage" in svg


def test_security_supply_chain_posture_detects_repo_controls(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "LICENSE", "Apache License 2.0\n")
    _write(tmp_path / "pyproject.toml", '[project]\nlicense = "Apache-2.0"\n')
    _write(tmp_path / "uv.lock", "# lockfile\n")
    _write(tmp_path / "SECURITY.md", "# Security policy\n")
    _write(tmp_path / ".github" / "dependabot.yml", "version: 2\nupdates: []\n")
    _write(
        tmp_path / ".github" / "workflows" / "codeql.yml",
        "permissions:\n  security-events: write\nsteps:\n  - uses: github/codeql-action/init@v3\n",
    )
    _write(
        tmp_path / ".github" / "workflows" / "scorecard.yml",
        "permissions: read-all\nsteps:\n  - uses: ossf/scorecard-action@v2\n",
    )
    _write(
        tmp_path / ".github" / "workflows" / "secret-scan.yml",
        "permissions: read-all\nsteps:\n  - uses: gitleaks/gitleaks-action@v2\n",
    )
    _write(
        tmp_path / ".github" / "workflows" / "release.yml",
        "permissions:\n  id-token: write\nsteps:\n  - run: echo provenance attestation\n",
    )
    _write(tmp_path / "docs" / "supply-chain.md", "SBOM inventory is exported as CycloneDX.\n")
    project = {"spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework", "path": str(tmp_path)}}

    matrix = tool.build_security_supply_chain_matrix([project])

    assert matrix["schema_version"] == 1
    assert matrix["summary"]["dpone"]["score"] >= 90
    assert matrix["summary"]["dpone"]["code_comparable"] is True
    assert {dimension["slug"] for dimension in matrix["dimensions"]} >= {
        "license_declared",
        "secret_scanning",
        "sast_codeql",
        "ossf_scorecard",
        "dependency_updates",
        "lockfile_reproducibility",
        "sbom_inventory",
        "security_policy",
        "least_privilege_permissions",
        "release_provenance",
    }
    assert any(
        entry["tool"] == "dpone" and entry["dimension"] == "sast_codeql" and entry["status"] == "present"
        for entry in matrix["entries"]
    )
    assert any("codeql.yml" in source for entry in matrix["entries"] for source in entry["sources"])


def test_security_supply_chain_renderers_include_scores_and_evidence(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "LICENSE", "Apache-2.0\n")
    _write(tmp_path / ".github" / "workflows" / "codeql.yml", "uses: github/codeql-action/analyze@v3\n")
    _write(tmp_path / "docs" / "supply-chain.md", "SBOM and provenance evidence.\n")
    project = {"spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework", "path": str(tmp_path)}}
    matrix = tool.build_security_supply_chain_matrix([project])

    markdown = tool.render_security_supply_chain_section(matrix)
    svg = tool.render_security_supply_chain_svg({"security_supply_chain": matrix})

    assert "Security & Supply Chain" in markdown
    assert "OSSF Scorecard" in markdown
    assert "SBOM" in markdown
    assert "CodeQL" in markdown
    assert "Fivetran" in markdown
    assert "closed-core posture note" in markdown
    assert "<svg" in svg
    assert "Security supply-chain posture" in svg


def test_operational_reliability_matrix_scores_measured_dpone_evidence(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "docs" / "orchestration.md", "Retry handoff and resume policy are documented.\n")
    _write(tmp_path / "docs" / "cdc.md", "CDC replay, commit gates, and state advancement are documented.\n")
    _write(tmp_path / "docs" / "observability.md", "Runtime observability evidence.\n")
    _write(tmp_path / "docs" / "schema-evolution.md", "Schema drift evidence.\n")
    _write(tmp_path / "docs" / "physical-ddl-apply.md", "Physical DDL apply evidence.\n")
    _write(tmp_path / "docs" / "release-evidence.md", "Release evidence and checkpoint replay.\n")
    _write(
        tmp_path / "test_artifacts" / "resume" / "native_transfer_resume_after_export.json",
        json.dumps(
            {
                "schema_version": "dpone.native_transfer.resume_evidence.v1",
                "passed": True,
                "safe_to_resume": True,
                "request": {
                    "expected_rows": 10000,
                    "actual_rows_after_retry": 10000,
                    "duplicate_rows_after_retry": 0,
                },
            }
        ),
    )
    _write(
        tmp_path / "test_artifacts" / "benchmark-slo" / "benchmark_slo_gate.json",
        json.dumps({"passed": True, "slo_passed": True, "benchmark_passed": True, "metric_count": 4}),
    )
    _write(
        tmp_path / "test_artifacts" / "live-state-reconciliation" / "live_state_reconciliation.json",
        json.dumps({"passed": True, "delete_reconciliation_checked": True, "state_checks": ["cdc_offsets"]}),
    )
    _write(
        tmp_path / "test_artifacts" / "release-evidence" / "release_evidence_pack.json",
        json.dumps({"passed": True, "blockers": [], "artifacts": [{"name": "certification_pack", "passed": True}]}),
    )
    project = {"spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework", "path": str(tmp_path)}}

    matrix = tool.build_operational_reliability_matrix([project])

    assert matrix["schema_version"] == 1
    assert matrix["summary"]["dpone"]["score"] >= 85
    assert matrix["summary"]["dpone"]["evidence_mode"] == "measured"
    assert {dimension["slug"] for dimension in matrix["dimensions"]} >= {
        "retry_resume",
        "checkpoint_safety",
        "idempotency",
        "fault_injection",
        "data_reconciliation",
        "cdc_recovery",
        "observability_evidence",
        "performance_slo",
        "schema_drift",
        "release_evidence",
    }
    assert any(
        entry["tool"] == "dpone" and entry["dimension"] == "idempotency" and entry["level"] == "measured"
        for entry in matrix["entries"]
    )
    assert any("native_transfer_resume" in source for entry in matrix["entries"] for source in entry["sources"])


def test_operational_reliability_renderers_include_recovery_and_evidence(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "docs" / "orchestration.md", "Retry resume checkpoint evidence.\n")
    _write(
        tmp_path / "test_artifacts" / "resume" / "native_transfer_resume.json",
        json.dumps(
            {
                "passed": True,
                "safe_to_resume": True,
                "request": {"expected_rows": 5, "actual_rows_after_retry": 5, "duplicate_rows_after_retry": 0},
            }
        ),
    )
    project = {"spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework", "path": str(tmp_path)}}
    matrix = tool.build_operational_reliability_matrix([project])

    markdown = tool.render_operational_reliability_section(matrix)
    svg = tool.render_operational_reliability_svg({"operational_reliability": matrix})

    assert "Operational Reliability" in markdown
    assert "Retry / resume" in markdown
    assert "zero duplicate" in markdown
    assert "measured" in markdown
    assert "Fivetran" in markdown
    assert "<svg" in svg
    assert "Operational reliability posture" in svg


def test_operability_tco_matrix_scores_self_service_dpone_evidence(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "pyproject.toml", '[project]\nlicense = "Apache-2.0"\n')
    _write(tmp_path / "docs" / "ci-cd.md", "Manual CI/CD workflow, rollback, artifacts, and release gates.\n")
    _write(tmp_path / "docs" / "ops-cli.md", "Self-service ops CLI for evidence bundles and security audit.\n")
    _write(tmp_path / "docs" / "observability.md", "Metrics, logs, and operational diagnostics.\n")
    _write(tmp_path / "docs" / "supply-chain.md", "SBOM and secrets guidance.\n")
    _write(tmp_path / "docs" / "route-certification-pack.md", "Route certification runbook and artifact pack.\n")
    _write(tmp_path / "src" / "dpone" / "ops" / "deploy_profiles.py", "PROFILE = 'local'\n")
    _write(tmp_path / ".github" / "workflows" / "ci.yml", "name: CI\n")
    _write(tmp_path / ".github" / "workflows" / "release.yml", "name: Release\n")
    project = {"spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework", "path": str(tmp_path)}}

    matrix = tool.build_operability_tco_matrix([project])

    assert matrix["schema_version"] == 1
    assert matrix["summary"]["dpone"]["score"] >= 85
    assert matrix["summary"]["dpone"]["evidence_mode"] == "repo-evidence"
    assert {dimension["slug"] for dimension in matrix["dimensions"]} >= {
        "deployment_footprint",
        "infra_prerequisites",
        "configuration_surface",
        "secrets_operations",
        "self_service_docs",
        "ci_cd_automation",
        "observability_ops",
        "upgrade_rollback",
        "operator_toil",
        "lock_in_transparency",
    }
    assert any(
        entry["tool"] == "dpone" and entry["dimension"] == "ci_cd_automation" and entry["level"] == "strong"
        for entry in matrix["entries"]
    )
    assert any("ops-cli.md" in source for entry in matrix["entries"] for source in entry["sources"])


def test_operability_tco_renderers_include_surface_area_and_lock_in(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "pyproject.toml", '[project]\nlicense = "Apache-2.0"\n')
    _write(tmp_path / "docs" / "ops-cli.md", "Self-service operations and runbook.\n")
    _write(tmp_path / ".github" / "workflows" / "ci.yml", "name: CI\n")
    project = {"spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework", "path": str(tmp_path)}}
    matrix = tool.build_operability_tco_matrix([project])

    markdown = tool.render_operability_tco_section(matrix)
    svg = tool.render_operability_tco_svg({"operability_tco": matrix})

    assert "TCO & Operability" in markdown
    assert "operator toil" in markdown
    assert "vendor lock-in" in markdown
    assert "Fivetran" in markdown
    assert "managed platform trade-off" in markdown
    assert "<svg" in svg
    assert "TCO and operability posture" in svg


def test_governance_compliance_matrix_scores_repo_evidence(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "docs" / "lineage.md", "OpenLineage export and load lineage catalog.\n")
    _write(tmp_path / "docs" / "load-lineage.md", "Load lineage audit and catalog mapping.\n")
    _write(tmp_path / "docs" / "schema-contracts.md", "Schema contracts and compatibility policy.\n")
    _write(tmp_path / "docs" / "data-contract-runtime.md", "Runtime data contract enforcement.\n")
    _write(tmp_path / "docs" / "release-evidence.md", "Release evidence pack and audit evidence chain.\n")
    _write(tmp_path / "docs" / "unified-run-evidence.md", "Unified run audit trail.\n")
    _write(tmp_path / "docs" / "route-certification-pack.md", "Route certification and compliance runbook.\n")
    _write(tmp_path / "docs" / "quality-tooling.md", "Data quality reconciliation gates.\n")
    _write(tmp_path / "docs" / "supply-chain.md", "Secret scanning and least privilege controls.\n")
    _write(tmp_path / "SECURITY.md", "Security policy and vulnerability intake.\n")
    _write(
        tmp_path / "src" / "dpone" / "services" / "ops" / "command_handlers_release.py",
        "def cmd_policy_evaluate():\n    return 'ok'\n",
    )
    _write(tmp_path / "src" / "dpone" / "runtime" / "schema_evolution.py", "class SchemaChangeLedger:\n    pass\n")
    _write(tmp_path / "src" / "dpone" / "runtime" / "lineage" / "audit.py", "class LineageAudit:\n    pass\n")
    _write(
        tmp_path / "test_artifacts" / "live" / "evidence-chain" / "evidence_chain_index.json",
        json.dumps({"entry_count": 3, "latest_chain_hash": "sha256:demo"}),
    )
    _write(
        tmp_path / "test_artifacts" / "live" / "release-evidence" / "release_evidence_pack.json",
        json.dumps({"passed": True, "artifacts": [{"name": "certification_pack", "passed": True}]}),
    )
    _write(
        tmp_path / "test_artifacts" / "live" / "live-state-reconciliation" / "live_state_reconciliation.json",
        json.dumps({"passed": True, "state_checks": ["cdc_offsets"]}),
    )
    project = {"spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework", "path": str(tmp_path)}}

    matrix = tool.build_governance_compliance_matrix([project])

    assert matrix["schema_version"] == 1
    assert matrix["summary"]["dpone"]["score"] >= 85
    assert matrix["summary"]["dpone"]["evidence_mode"] == "repo-evidence"
    assert {dimension["slug"] for dimension in matrix["dimensions"]} >= {
        "auditability",
        "lineage_catalog",
        "data_contracts",
        "schema_governance",
        "policy_gates",
        "evidence_chain",
        "access_secrets",
        "release_certification",
        "compliance_runbooks",
        "data_quality_reconciliation",
    }
    assert any(
        entry["tool"] == "dpone" and entry["dimension"] == "policy_gates" and entry["level"] == "strong"
        for entry in matrix["entries"]
    )
    assert any("evidence_chain_index" in source for entry in matrix["entries"] for source in entry["sources"])


def test_governance_compliance_renderers_include_audit_lineage_and_closed_core(tmp_path: Path) -> None:
    tool = _load_tool()
    _write(tmp_path / "docs" / "lineage.md", "Lineage catalog.\n")
    _write(tmp_path / "docs" / "release-evidence.md", "Audit evidence chain.\n")
    _write(
        tmp_path / "src" / "dpone" / "services" / "ops" / "command_handlers_release.py",
        "def cmd_policy_evaluate():\n    return 'ok'\n",
    )
    project = {"spec": {"slug": "dpone", "name": "dpone", "kind": "local-framework", "path": str(tmp_path)}}
    matrix = tool.build_governance_compliance_matrix([project])

    markdown = tool.render_governance_compliance_section(matrix)
    svg = tool.render_governance_compliance_svg({"governance_compliance": matrix})

    assert "Governance & Compliance" in markdown
    assert "Auditability" in markdown
    assert "lineage" in markdown
    assert "evidence chain" in markdown
    assert "Informatica" in markdown
    assert "managed governance posture" in markdown
    assert "<svg" in svg
    assert "Governance and compliance posture" in svg


def test_benchmark_tooling_has_thin_collector_and_renderer_taxonomy() -> None:
    expected_modules = {
        "tools/oss_benchmark/models.py": 220,
        "tools/oss_benchmark/config.py": 180,
        "tools/oss_benchmark/collectors.py": 700,
        "tools/oss_benchmark/feature_parity.py": 700,
        "tools/oss_benchmark/governance_compliance.py": 700,
        "tools/oss_benchmark/operability_tco.py": 620,
        "tools/oss_benchmark/operational_reliability.py": 620,
        "tools/oss_benchmark/security_supply_chain.py": 520,
        "tools/oss_benchmark/trust_center.py": 360,
        "tools/oss_benchmark/architecture_taxonomy.py": 360,
        "tools/oss_benchmark/benchmark_intelligence.py": 420,
        "tools/oss_benchmark/candidate_delta.py": 260,
        "tools/oss_benchmark/complexity_boundary.py": 520,
        "tools/oss_benchmark/evidence_trust.py": 520,
        "tools/oss_benchmark/public_evidence_integrity.py": 420,
        "tools/oss_benchmark/semantic_maintainability.py": 620,
        "tools/oss_benchmark/scoring_calibration.py": 520,
        "tools/oss_benchmark/scale_readiness.py": 560,
        "tools/oss_benchmark/external_analyzers.py": 520,
        "tools/oss_benchmark/independent_validation.py": 560,
        "tools/oss_benchmark/source_verification.py": 560,
        "tools/oss_benchmark/release_readiness.py": 360,
        "tools/oss_benchmark/payload_utils.py": 220,
        "tools/oss_benchmark/pr_complexity.py": 120,
        "tools/oss_benchmark/pr_evidence.py": 120,
        "tools/oss_benchmark/pr_roi.py": 120,
        "tools/oss_benchmark/pr_semantic.py": 120,
        "tools/oss_benchmark/pr_calibration.py": 120,
        "tools/oss_benchmark/pr_scale.py": 120,
        "tools/oss_benchmark/pr_validation.py": 120,
        "tools/oss_benchmark/pr_public_integrity.py": 120,
        "tools/oss_benchmark/pr_source_verification.py": 120,
        "tools/oss_benchmark/pr_release_readiness.py": 120,
        "tools/oss_benchmark/pr_summary.py": 240,
        "tools/oss_benchmark/renderers/markdown.py": 700,
        "tools/oss_benchmark/renderers/architecture.py": 180,
        "tools/oss_benchmark/renderers/complexity_boundary.py": 220,
        "tools/oss_benchmark/renderers/evidence_trust.py": 240,
        "tools/oss_benchmark/renderers/evidence_svg.py": 220,
        "tools/oss_benchmark/renderers/public_evidence_integrity.py": 220,
        "tools/oss_benchmark/renderers/public_integrity_svg.py": 220,
        "tools/oss_benchmark/renderers/source_verification.py": 240,
        "tools/oss_benchmark/renderers/source_verification_svg.py": 220,
        "tools/oss_benchmark/renderers/release_readiness.py": 240,
        "tools/oss_benchmark/renderers/release_readiness_svg.py": 220,
        "tools/oss_benchmark/renderers/semantic_maintainability.py": 280,
        "tools/oss_benchmark/renderers/semantic_svg.py": 260,
        "tools/oss_benchmark/renderers/scoring_calibration.py": 260,
        "tools/oss_benchmark/renderers/calibration_svg.py": 280,
        "tools/oss_benchmark/renderers/scale_readiness.py": 280,
        "tools/oss_benchmark/renderers/scale_svg.py": 280,
        "tools/oss_benchmark/renderers/independent_validation.py": 280,
        "tools/oss_benchmark/renderers/validation_svg.py": 280,
        "tools/oss_benchmark/renderers/intelligence.py": 260,
        "tools/oss_benchmark/renderers/methodology.py": 120,
        "tools/oss_benchmark/renderers/governance.py": 360,
        "tools/oss_benchmark/renderers/operability.py": 340,
        "tools/oss_benchmark/renderers/reliability.py": 340,
        "tools/oss_benchmark/renderers/security.py": 320,
        "tools/oss_benchmark/renderers/svg.py": 420,
        "tools/oss_benchmark/renderers/refactor_roi.py": 240,
        "tools/oss_benchmark/renderers/roi_svg.py": 220,
        "tools/oss_benchmark/renderers/trends.py": 160,
        "tools/oss_benchmark/refactor_roi.py": 520,
    }

    missing = [path for path in expected_modules if not (ROOT / path).exists()]
    assert missing == []

    oversized = []
    for path, max_lines in expected_modules.items():
        line_count = len((ROOT / path).read_text(encoding="utf-8").splitlines())
        if line_count > max_lines:
            oversized.append(f"{path}: {line_count} > {max_lines}")
    assert oversized == []

    core_text = (ROOT / "tools" / "oss_benchmark" / "core.py").read_text(encoding="utf-8")
    assert "def collect_project_metrics(" not in core_text
    assert "def render_markdown(" not in core_text
    assert "def render_scorecard_svg(" not in core_text
    assert "def build_feature_parity_matrix(" not in core_text
    assert "def build_operability_tco_matrix(" not in core_text
    assert "def build_operational_reliability_matrix(" not in core_text
    assert "def build_security_supply_chain_matrix(" not in core_text
    assert "def build_governance_compliance_matrix(" not in core_text
    assert "def build_trust_center_export(" not in core_text
    assert len(core_text.splitlines()) <= 950


def test_benchmark_document_contract() -> None:
    page = ROOT / "docs" / "benchmarks" / "oss-code-quality-benchmark-2026-06-12.md"
    text = page.read_text(encoding="utf-8")
    evidence = json.loads(
        (ROOT / "docs" / "benchmarks" / "data" / "oss-code-quality-benchmark-2026-06-12.json").read_text(
            encoding="utf-8"
        )
    )
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    required_fragments = [
        "Airbyte",
        "dlt",
        "Pentaho Kettle",
        "Apache Hop",
        "Sling",
        "Fivetran",
        "Informatica",
        "84c2d165ef11293b11e2321062e89996908d2f63",
        "f5999614b613254961a9073310a5db6b13dc279d",
        "89db5885a1e92e81eb01149cb3e536cc23482c39",
        "6aad589666c8458216b8685bb749d2b6dbd1b2f0",
        "6c4ca04c3328eb32da480780c9957bf17f80ffb5",
        "Top 15 modules by LOC",
        "Top 15 modules by SLOC",
        "closed-core / not code-comparable",
        "docs/benchmarks/assets/oss-quality-scorecard.svg",
        "docs/benchmarks/assets/oss-loc-sloc.svg",
        "docs/benchmarks/assets/oss-coupling-cohesion-quadrant.svg",
        "docs/benchmarks/assets/oss-module-hotspots.svg",
        "docs/benchmarks/assets/oss-architecture-risk-heatmap.svg",
        "docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json",
        "Last refresh",
        "Last manual CI runner",
        "Freshness",
        "Data freshness policy",
        "Table of contents",
        "General comparisons",
        "Detailed project drill-downs",
        "Tool overview and total corpus",
        "Feature Parity Matrix",
        "docs/benchmarks/assets/oss-feature-parity.svg",
        "Security & Supply Chain",
        "docs/benchmarks/assets/oss-security-supply-chain.svg",
        "Operational Reliability",
        "docs/benchmarks/assets/oss-operational-reliability.svg",
        "TCO & Operability",
        "docs/benchmarks/assets/oss-operability-tco.svg",
        "Governance & Compliance",
        "docs/benchmarks/assets/oss-governance-compliance.svg",
        "Architecture Taxonomy & Contract Discipline",
        "docs/benchmarks/assets/oss-architecture-taxonomy.svg",
        "Contract conformance",
        "Approved facades",
        "Complexity & Boundary Discipline",
        "docs/benchmarks/assets/oss-complexity-boundary.svg",
        "Maintainability risk register",
        "Refactor ROI Roadmap",
        "docs/benchmarks/assets/oss-refactor-roi-roadmap.svg",
        "Quality debt estimate",
        "ROI-ranked refactor backlog",
        "Target architecture recommendations",
        "Evidence Trust & Auditability",
        "Evidence confidence",
        "Measured vs derived vs inferred",
        "Metric provenance ledger",
        "Reproducibility manifest",
        "docs/benchmarks/assets/oss-evidence-confidence.svg",
        "docs/benchmarks/data/oss-benchmark-provenance.json",
        "Public Evidence Integrity",
        "claim evidence ledger",
        "redaction violations",
        "docs/benchmarks/assets/oss-public-evidence-integrity.svg",
        "Source Citation Verification",
        "claim-to-source matrix",
        "source health",
        "docs/benchmarks/assets/oss-source-verification.svg",
        "Benchmark v3 Release Readiness",
        "Benchmark v3 verified",
        "Claims Ledger",
        "Runtime Certification Matrix",
        "Executable Certification",
        "Golden Dataset Evidence",
        "Run Ledger",
        "Certification Gates",
        "Quality Budget As Code",
        "Evidence Warehouse Export",
        "runtime-certification/latest/run-ledger.json",
        "runtime-certification/latest/contract-checks.json",
        "stable metric groups",
        "experimental metric groups",
        "docs/benchmarks/assets/oss-release-readiness-seal.svg",
        "docs/benchmarks/oss-benchmark-release-readiness-2026-06-12.md",
        "docs/benchmarks/data/oss-benchmark-release-readiness-2026-06-12.json",
        "Semantic Maintainability Deep Scan",
        "God object radar",
        "SOLID/DI/Clean Code evidence",
        "DRY/KISS responsibility signals",
        "docs/benchmarks/assets/oss-semantic-maintainability.svg",
        "docs/benchmarks/assets/oss-god-object-radar.svg",
        "Scoring Validity & Calibration",
        "Language/repo normalization",
        "Sensitivity analysis",
        "Anti-gaming guardrails",
        "Score explanation cards",
        "docs/benchmarks/assets/oss-score-calibration.svg",
        "docs/benchmarks/assets/oss-score-sensitivity.svg",
        "docs/benchmarks/assets/oss-normalized-vs-raw.svg",
        "Scale Readiness & Growth Simulation",
        "Quality headroom",
        "Architecture runway",
        "Scale scenarios",
        "Comparator-scale projection",
        "docs/benchmarks/assets/oss-scale-readiness.svg",
        "docs/benchmarks/assets/oss-architecture-runway.svg",
        "docs/benchmarks/assets/oss-quality-headroom.svg",
        "Independent Analyzer Cross-Validation & Audit Pack",
        "External analyzer execution",
        "stale analyzer values",
        "Analyzer command ledger",
        "LOC/SLOC cross-check",
        "Complexity cross-check",
        "Validation confidence",
        "docs/benchmarks/assets/oss-independent-validation.svg",
        "docs/benchmarks/assets/oss-analyzer-confidence.svg",
        "Customer Trust Center Snapshot",
        "docs/benchmarks/dpone-trust-center-snapshot-2026-06-12.md",
        "docs/benchmarks/data/dpone-trust-center-snapshot-2026-06-12.json",
        "docs/benchmarks/assets/dpone-trust-center-badge.svg",
        "Test footprint",
        "Total LOC",
        "with tests",
        "without tests",
        "Industrial Maintainability Index",
        "Trend history",
        "Coverage Confidence Matrix",
        "Architecture Risk Heatmap",
        "Approved high fan-in contracts",
        "Quality Gates",
        "PR benchmark summary",
        "docs/benchmarks/assets/oss-quality-trend.svg",
        "docs/benchmarks/data/oss-code-quality-benchmark-history.json",
        "test_artifacts/oss-code-quality-benchmark/pr-comment.md",
    ]

    missing = [fragment for fragment in required_fragments if fragment not in text]
    assert missing == []
    assert "OSS code quality benchmark: benchmarks/oss-code-quality-benchmark-2026-06-12.md" in mkdocs
    evidence_slugs = {project["spec"]["slug"] for project in evidence["projects"]}
    assert "sling" in evidence_slugs
    assert evidence["run_context"]["generated_at"]
    assert evidence["run_context"]["updated_by"]
    assert all("metric_groups" in project for project in evidence["projects"])
    assert all("industrial_maintainability" in project for project in evidence["projects"])
    assert all("coverage_confidence" in project for project in evidence["projects"])
    assert all("architecture_risk" in project for project in evidence["projects"])
    dpone = next(project for project in evidence["projects"] if project["spec"]["slug"] == "dpone")
    assert isinstance(dpone["architecture_risk"].get("approved_fan_in_contracts"), list)
    assert "feature_parity" in evidence
    assert {"sling", "fivetran", "informatica"} <= {
        tool_entry["slug"] for tool_entry in evidence["feature_parity"]["tools"]
    }
    assert "security_supply_chain" in evidence
    assert evidence["security_supply_chain"]["summary"]["dpone"]["score"] >= 0
    assert "operational_reliability" in evidence
    assert evidence["operational_reliability"]["summary"]["dpone"]["score"] >= 0
    assert "operability_tco" in evidence
    assert evidence["operability_tco"]["summary"]["dpone"]["score"] >= 0
    assert "governance_compliance" in evidence
    assert evidence["governance_compliance"]["summary"]["dpone"]["score"] >= 0
    assert "architecture_taxonomy" in evidence
    assert evidence["architecture_taxonomy"]["summary"]["dpone"]["score"] >= 0
    assert "compatibility_facades" in evidence["architecture_taxonomy"]["summary"]["dpone"]["contract_conformance"]
    assert "complexity_boundary" in evidence
    assert evidence["complexity_boundary"]["summary"]["dpone"]["overall_score"] >= 0
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-complexity-boundary.svg").exists()
    assert "refactor_roi" in evidence
    assert evidence["refactor_roi"]["summary"]["dpone"]["debt_points"] >= 0
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-refactor-roi-roadmap.svg").exists()
    assert "evidence_trust" in evidence
    assert evidence["evidence_trust"]["overall_confidence_score"] >= 0
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-evidence-confidence.svg").exists()
    assert "public_evidence_integrity" in evidence
    assert evidence["public_evidence_integrity"]["redaction_violation_count"] == 0
    assert evidence["public_evidence_integrity"]["claim_coverage_percent"] == 100
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-public-evidence-integrity.svg").exists()
    assert "source_verification" in evidence
    assert evidence["source_verification"]["summary"]["source_health_score"] >= 0
    assert evidence["source_verification"]["summary"]["claim_traceability_percent"] >= 0
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-source-verification.svg").exists()
    assert "benchmark_release_readiness" in evidence
    assert evidence["benchmark_release_readiness"]["status"] in {"release-ready", "watch", "blocked"}
    assert evidence["benchmark_release_readiness"]["evidence_seal"]["score"] >= 0
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-release-readiness-seal.svg").exists()
    assert (ROOT / "docs" / "benchmarks" / "oss-benchmark-release-readiness-2026-06-12.md").exists()
    assert (ROOT / "docs" / "benchmarks" / "data" / "oss-benchmark-release-readiness-2026-06-12.json").exists()
    serialized_evidence = json.dumps(evidence, sort_keys=True)
    assert "/Users/" not in serialized_evidence
    assert "/private/tmp" not in serialized_evidence
    assert "data-platform-dpone" not in serialized_evidence
    assert "semantic_maintainability" in evidence
    assert evidence["semantic_maintainability"]["summary"]["dpone"]["overall_score"] >= 0
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-semantic-maintainability.svg").exists()
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-god-object-radar.svg").exists()
    assert "scoring_calibration" in evidence
    assert evidence["scoring_calibration"]["summary"]["dpone"]["normalized_score"] >= 0
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-score-calibration.svg").exists()
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-score-sensitivity.svg").exists()
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-normalized-vs-raw.svg").exists()
    assert "scale_readiness" in evidence
    assert evidence["scale_readiness"]["summary"]["dpone"]["architecture_runway_score"] >= 0
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-scale-readiness.svg").exists()
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-architecture-runway.svg").exists()
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-quality-headroom.svg").exists()
    assert "independent_validation" in evidence
    assert evidence["independent_validation"]["summary"]["dpone"]["confidence_score"] >= 0
    unavailable_analyzers = [
        f"{result.get('project')}/{result.get('tool')}: {result.get('error')}"
        for result in evidence.get("external_analyzer_results", [])
        if result.get("status") == "unavailable"
    ]
    assert unavailable_analyzers == []
    analyzer_summary_gaps = {
        slug: summary.get("unavailable_analyzers")
        for slug, summary in evidence["independent_validation"]["summary"].items()
        if summary.get("unavailable_analyzers")
    }
    assert analyzer_summary_gaps == {}
    unavailable_cross_checks = [
        f"{project}/{check.get('tool')}: {check.get('error')}"
        for section in ("loc_sloc_cross_checks", "complexity_cross_checks")
        for project, checks in evidence["independent_validation"][section].items()
        for check in checks
        if check.get("status") == "unavailable"
    ]
    assert unavailable_cross_checks == []
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-independent-validation.svg").exists()
    assert (ROOT / "docs" / "benchmarks" / "assets" / "oss-analyzer-confidence.svg").exists()
    provenance = json.loads(
        (ROOT / "docs" / "benchmarks" / "data" / "oss-benchmark-provenance.json").read_text(encoding="utf-8")
    )
    assert provenance["reproducibility"]["artifact_checksums"]
    assert provenance["metric_provenance"]
    assert "trust_center" in evidence
    assert evidence["trust_center"]["status"] in {"verified", "watch", "blocked"}
    assert evidence["quality_gates"]["status"] == "passed"
    assert "runtime_certification_v2" in evidence
    assert evidence["runtime_certification_v2"]["gates"]["status"] == "passed"
    assert evidence["runtime_certification_v2"]["summary"]["passed"] >= 5
    assert {scenario["scenario_id"] for scenario in evidence["runtime_certification_v2"]["scenarios"]} >= {
        "nested-lineage",
        "incremental",
        "cdc-replay",
        "schema-evolution",
        "artifact-contract",
    }
    assert evidence["runtime_certification_v2"]["run_ledger"]
    assert evidence["runtime_certification_v2"]["contract_checks"]
    assert (ROOT / "docs" / "benchmarks" / "data" / "runtime-certification" / "latest" / "run-ledger.json").exists()
    assert (
        ROOT / "docs" / "benchmarks" / "data" / "runtime-certification" / "latest" / "contract-checks.json"
    ).exists()
    assert evidence["trust_center"]["status"] == "verified"
    trust_center = json.loads(
        (ROOT / "docs" / "benchmarks" / "data" / "dpone-trust-center-snapshot-2026-06-12.json").read_text(
            encoding="utf-8"
        )
    )
    assert trust_center["badge"]["label"] == f"dpone {trust_center['status']}"
    assert (ROOT / "docs" / "benchmarks" / "dpone-trust-center-snapshot-2026-06-12.md").exists()
    assert (ROOT / "docs" / "benchmarks" / "assets" / "dpone-trust-center-badge.svg").exists()
    assert evidence["quality_gates"]["status"] in {"passed", "failed"}
    assert "trend_summary" in evidence
    assert (ROOT / "test_artifacts" / "oss-code-quality-benchmark" / "pr-comment.md").exists()
