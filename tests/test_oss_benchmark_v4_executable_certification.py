from __future__ import annotations

import csv
import time
from pathlib import Path

from tools.oss_benchmark.certification_runtime_catalog import ScenarioCatalog
from tools.oss_benchmark.certification_runtime_gates import evaluate_certification_gates
from tools.oss_benchmark.certification_runtime_golden import run_nested_lineage_golden
from tools.oss_benchmark.certification_runtime_merge import merge_runtime_certification_v2
from tools.oss_benchmark.certification_runtime_runner import LocalScenarioRunner
from tools.oss_benchmark.cli import parse_args
from tools.oss_benchmark.exports import build_evidence_export_manifest, write_evidence_exports
from tools.oss_benchmark.quality_gates import evaluate_quality_gates
from tools.oss_benchmark.renderers.certification_runtime import render_executable_certification_section


def test_scenario_catalog_exposes_required_industrial_certification_contracts() -> None:
    catalog = ScenarioCatalog.default()

    scenarios = catalog.select("all")
    scenario_ids = {scenario.scenario_id for scenario in scenarios}

    assert scenario_ids == {
        "nested-lineage",
        "incremental",
        "cdc-replay",
        "schema-evolution",
        "artifact-contract",
    }
    assert catalog.select("nested-lineage")[0].runner == "python_api"
    assert catalog.select("artifact-contract")[0].runner == "cli"
    assert all(scenario.required for scenario in scenarios)


def test_golden_nested_lineage_checks_ids_parent_root_order_and_determinism(tmp_path: Path) -> None:
    scenario = ScenarioCatalog.default().select("nested-lineage")[0]

    record = run_nested_lineage_golden(scenario, tmp_path)

    assert record["status"] == "passed"
    assert record["input_hash"]
    assert record["output_hash"]
    assert record["row_counts"]["orders"] == 2
    assert record["row_counts"]["orders__items"] == 3
    assert {check["status"] for check in record["contract_checks"]} == {"passed"}
    assert {check["check_id"] for check in record["contract_checks"]} >= {
        "root_ids_stable",
        "children_have_parent_and_root",
        "no_orphan_children",
        "array_order_preserved",
        "rerun_output_hash_deterministic",
    }
    assert all(Path(path).exists() for path in record["artifact_paths"])


def test_runner_reports_timeout_with_normalized_error_class(tmp_path: Path) -> None:
    scenario = ScenarioCatalog.default().select("nested-lineage")[0]

    def slow_handler(*_: object) -> dict:
        time.sleep(0.05)
        return {"status": "passed"}

    runner = LocalScenarioRunner(handler_map={scenario.scenario_id: slow_handler})

    record = runner.run(scenario, tmp_path, timeout_seconds=0.001)

    assert record["status"] == "failed"
    assert record["error_class"] == "timeout"
    assert record["contract_checks"][0]["check_id"] == "scenario_execution"
    assert record["contract_checks"][0]["status"] == "failed"


def test_failed_scenario_refresh_preserves_previous_result_as_stale() -> None:
    previous = {
        "runtime_certification_v2": {
            "scenarios": [
                {
                    "scenario_id": "nested-lineage",
                    "category": "nested_lineage",
                    "runner": "python_api",
                    "required": True,
                    "status": "passed",
                    "last_error": None,
                    "row_counts": {"orders": 2},
                    "contract_checks": [],
                    "freshness": {
                        "status": "fresh",
                        "last_updated_at": "2026-06-01T00:00:00+00:00",
                        "refresh_attempted_at": "2026-06-01T00:00:00+00:00",
                        "last_error": None,
                        "stale_age_days": 0,
                    },
                }
            ]
        }
    }
    failed = {
        "scenario_id": "nested-lineage",
        "category": "nested_lineage",
        "runner": "python_api",
        "required": True,
        "status": "failed",
        "error_class": "timeout",
        "last_error": "timed out",
        "contract_checks": [],
    }

    merged = merge_runtime_certification_v2(
        [failed],
        previous_payload=previous,
        requested_scenarios={"nested-lineage"},
        attempted_at="2026-06-11T00:00:00+00:00",
        allow_stale=True,
    )

    scenario = merged["scenarios"][0]
    assert scenario["status"] == "passed"
    assert scenario["freshness"]["status"] == "stale"
    assert scenario["freshness"]["last_updated_at"] == "2026-06-01T00:00:00+00:00"
    assert scenario["freshness"]["stale_age_days"] == 10
    assert scenario["freshness"]["last_error"] == "timed out"


def test_failed_scenario_without_previous_result_becomes_unavailable() -> None:
    failed = {
        "scenario_id": "nested-lineage",
        "category": "nested_lineage",
        "runner": "python_api",
        "required": True,
        "status": "failed",
        "error_class": "exception",
        "last_error": "boom",
        "contract_checks": [],
    }

    merged = merge_runtime_certification_v2(
        [failed],
        previous_payload=None,
        requested_scenarios={"nested-lineage"},
        attempted_at="2026-06-11T00:00:00+00:00",
        allow_stale=True,
    )

    scenario = merged["scenarios"][0]
    assert scenario["status"] == "unavailable"
    assert scenario["freshness"]["status"] == "unavailable"
    assert scenario["freshness"]["last_error"] == "boom"


def test_certification_gate_and_quality_gate_fail_required_executable_failures() -> None:
    certification = {
        "scenarios": [
            {
                "scenario_id": "nested-lineage",
                "required": True,
                "status": "failed",
                "freshness": {"status": "fresh"},
                "contract_checks": [{"check_id": "root_ids_stable", "status": "failed"}],
            }
        ]
    }

    certification_gate = evaluate_certification_gates(certification)
    payload = _quality_gate_payload(certification | {"gates": certification_gate})
    quality_gate = evaluate_quality_gates(payload)

    assert certification_gate["status"] == "failed"
    assert quality_gate["status"] == "failed"
    assert "executable_certification" in {check["id"] for check in quality_gate["failed_checks"]}


def test_cli_options_rendering_and_exports_cover_v4_runtime_certification(tmp_path: Path) -> None:
    args = parse_args(
        [
            "--run-certification",
            "--certification-mode",
            "local",
            "--scenario",
            "nested-lineage",
            "--max-scenario-seconds",
            "7",
            "--fail-on-certification",
        ]
    )
    payload = _quality_gate_payload(
        {
            "summary": {"passed": 1, "failed": 0, "stale": 0, "unavailable": 0},
            "scenarios": [
                {
                    "scenario_id": "nested-lineage",
                    "category": "nested_lineage",
                    "runner": "python_api",
                    "status": "passed",
                    "freshness": {"status": "fresh", "last_updated_at": "2026-06-30T00:00:00+00:00"},
                    "duration_ms": 12,
                    "row_counts": {"orders": 2},
                    "contract_checks": [{"check_id": "root_ids_stable", "status": "passed"}],
                    "artifact_paths": ["docs/benchmarks/data/runtime-certification/latest/nested-lineage.json"],
                }
            ],
            "run_ledger": [{"scenario_id": "nested-lineage", "status": "passed"}],
            "contract_checks": [{"scenario_id": "nested-lineage", "check_id": "root_ids_stable", "status": "passed"}],
            "gates": {"status": "passed", "failed_checks": []},
        }
    )
    payload["evidence_exports"] = build_evidence_export_manifest()

    written = write_evidence_exports(payload, output_dir=tmp_path)
    rendered = render_executable_certification_section(payload)

    assert args.run_certification is True
    assert args.certification_mode == "local"
    assert args.scenario == "nested-lineage"
    assert args.max_scenario_seconds == 7
    assert args.fail_on_certification is True
    assert {path.name for path in written} >= {
        "certification_scenarios.csv",
        "contract_checks.csv",
        "run_ledger.csv",
    }
    assert "## Executable Certification" in rendered
    assert "## Golden Dataset Evidence" in rendered
    assert "## Run Ledger" in rendered
    assert "## Certification Gates" in rendered
    with (tmp_path / "certification_scenarios.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["scenario_id"] == "nested-lineage"


def _quality_gate_payload(certification: dict) -> dict:
    return {
        "schema_version": 2,
        "release_context": {
            "release_tag": "v0.62.1",
            "release_sha": "abc123",
            "dpone_version": "0.62.1",
        },
        "runtime_certification_v2": certification,
        "projects": [
            {
                "project_id": "dpone",
                "display_name": "dpone",
                "spec": {"slug": "dpone", "name": "dpone"},
                "metric_groups": {"loc_sloc": {"status": "fresh"}},
                "industrial_maintainability": {"score": 96},
                "architecture_risk": {"score": 12},
                "coverage_confidence": {"score": 90},
                "loc_without_tests": {"max_lines": 390, "max_sloc": 379},
                "coupling": {"max_ce": 20, "cohesion_ratio": 0.62, "avg_clustering": 0.17},
            }
        ],
    }
