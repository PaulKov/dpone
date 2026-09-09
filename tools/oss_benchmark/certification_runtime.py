"""Executable release certification orchestration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tools.oss_benchmark.certification_runtime_catalog import ScenarioCatalog
from tools.oss_benchmark.certification_runtime_gates import evaluate_certification_gates
from tools.oss_benchmark.certification_runtime_merge import merge_runtime_certification_v2
from tools.oss_benchmark.certification_runtime_models import relative_artifact_path
from tools.oss_benchmark.certification_runtime_runner import LocalScenarioRunner
from tools.oss_benchmark.config import ROOT

RUNTIME_CERTIFICATION_DIR = ROOT / "docs" / "benchmarks" / "data" / "runtime-certification" / "latest"


def build_runtime_certification_v2(
    *,
    previous_payload: dict[str, Any] | None,
    generated_at: str,
    mode: str,
    scenario: str,
    run_certification: bool,
    allow_stale: bool,
    max_scenario_seconds: int,
    output_dir: Path = RUNTIME_CERTIFICATION_DIR,
    root: Path = ROOT,
) -> dict[str, Any]:
    """Build or reuse executable certification evidence."""

    catalog = ScenarioCatalog.default()
    selected = catalog.select(scenario)
    requested = {item.scenario_id for item in selected}
    if mode == "skip":
        return {}
    if mode == "reuse" and not run_certification:
        return _reuse_previous(previous_payload, generated_at=generated_at, scenario=scenario)
    records = [
        _public_artifact_paths(
            LocalScenarioRunner().run(item, output_dir, timeout_seconds=float(max_scenario_seconds)),
            root=root,
        )
        for item in selected
    ]
    merged = merge_runtime_certification_v2(
        records,
        previous_payload=previous_payload,
        requested_scenarios=requested,
        attempted_at=generated_at,
        allow_stale=allow_stale,
    )
    merged["mode"] = mode
    merged["scenario_filter"] = scenario
    merged["gates"] = evaluate_certification_gates(merged)
    _write_runtime_artifacts(merged, output_dir=output_dir)
    return merged


def _reuse_previous(
    previous_payload: dict[str, Any] | None,
    *,
    generated_at: str,
    scenario: str,
) -> dict[str, Any]:
    previous = dict((previous_payload or {}).get("runtime_certification_v2") or {})
    if not previous:
        return {}
    previous["mode"] = "reuse"
    previous["scenario_filter"] = scenario
    previous["refresh_attempted_at"] = generated_at
    previous["gates"] = evaluate_certification_gates(previous)
    return previous


def _public_artifact_paths(record: dict[str, Any], *, root: Path) -> dict[str, Any]:
    updated = dict(record)
    updated["artifact_paths"] = [
        relative_artifact_path(Path(path), root=root) for path in record.get("artifact_paths") or []
    ]
    return updated


def _write_runtime_artifacts(certification: dict[str, Any], *, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "run-ledger.json").write_text(
        json.dumps(certification.get("run_ledger") or [], indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "contract-checks.json").write_text(
        json.dumps(certification.get("contract_checks") or [], indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
