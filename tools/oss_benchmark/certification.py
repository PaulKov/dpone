"""Runtime certification matrix for benchmark release evidence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tools.oss_benchmark.config import ROOT

CheckFn = Callable[[dict[str, Any], Path], bool]


@dataclass(frozen=True)
class CertificationScenario:
    scenario_id: str
    source: str
    strategy: str
    sink: str
    description: str
    checks: tuple[tuple[str, CheckFn], ...]
    artifacts: tuple[str, ...] = ()


def build_runtime_certification(payload: dict[str, Any], *, root: Path = ROOT) -> dict[str, Any]:
    """Build a deterministic release certification matrix from benchmark evidence."""

    scenarios = [_evaluate_scenario(scenario, payload, root=root) for scenario in _scenarios()]
    summary = _status_summary(scenarios)
    return {
        "schema_version": 1,
        "policy": "Runtime certification scenarios must expose check results and artifact evidence.",
        "summary": summary,
        "scenarios": scenarios,
    }


def _scenarios() -> tuple[CertificationScenario, ...]:
    return (
        CertificationScenario(
            scenario_id="release_gate_certification",
            source="dpone",
            strategy="release_gate",
            sink="benchmark evidence",
            description="Release context, benchmark quality gates and readiness evidence are present.",
            checks=(
                ("release tag resolved", _has_release_tag),
                ("quality gates passed", _quality_gates_passed),
            ),
        ),
        CertificationScenario(
            scenario_id="python_api_payload_contract",
            source="dpone",
            strategy="python_api",
            sink="raw evidence JSON",
            description="The generated payload is schema-versioned and exposes project identity records.",
            checks=(
                ("schema v2", _schema_v2),
                ("project identity", _has_project_identity),
            ),
        ),
        CertificationScenario(
            scenario_id="nested_lineage_contract",
            source="nested objects",
            strategy="nested_normalization",
            sink="lineage tables",
            description="Nested normalization evidence documents parent/root identity and has contract tests.",
            checks=(
                ("nested docs mention parent/root identity", _nested_docs_have_lineage),
                ("nested contract tests exist", _nested_tests_exist),
            ),
            artifacts=("docs/nested-normalization.md", "tests/test_nested_normalization_contracts.py"),
        ),
        CertificationScenario(
            scenario_id="benchmark_artifact_contract",
            source="benchmark",
            strategy="artifact_generation",
            sink="docs/benchmarks",
            description="Raw JSON, markdown, provenance and release readiness artifacts are generated.",
            checks=(
                ("raw benchmark JSON", _raw_json_exists),
                ("benchmark markdown", _benchmark_doc_exists),
                ("provenance JSON", _provenance_exists),
            ),
            artifacts=(
                "docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json",
                "docs/benchmarks/oss-code-quality-benchmark-2026-06-12.md",
                "docs/benchmarks/data/oss-benchmark-provenance.json",
            ),
        ),
    )


def _evaluate_scenario(
    scenario: CertificationScenario,
    payload: dict[str, Any],
    *,
    root: Path,
) -> dict[str, Any]:
    checks = [
        {"name": name, "status": "passed" if check(payload, root) else "failed"} for name, check in scenario.checks
    ]
    status = "passed" if all(check["status"] == "passed" for check in checks) else "failed"
    return {
        "scenario_id": scenario.scenario_id,
        "source": scenario.source,
        "strategy": scenario.strategy,
        "sink": scenario.sink,
        "description": scenario.description,
        "status": status,
        "duration_ms": 0,
        "checks": checks,
        "artifact_paths": list(scenario.artifacts),
        "last_error": None if status == "passed" else _failed_check_summary(checks),
    }


def _has_release_tag(payload: dict[str, Any], root: Path) -> bool:
    return bool((payload.get("release_context") or {}).get("release_tag"))


def _quality_gates_passed(payload: dict[str, Any], root: Path) -> bool:
    return (payload.get("quality_gates") or {}).get("status") == "passed"


def _schema_v2(payload: dict[str, Any], root: Path) -> bool:
    return int(payload.get("schema_version") or 0) >= 2


def _has_project_identity(payload: dict[str, Any], root: Path) -> bool:
    return all(project.get("project_id") and project.get("display_name") for project in payload.get("projects") or [])


def _nested_docs_have_lineage(payload: dict[str, Any], root: Path) -> bool:
    path = root / "docs" / "nested-normalization.md"
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8").lower()
    return "parent" in text and "root" in text


def _nested_tests_exist(payload: dict[str, Any], root: Path) -> bool:
    return (root / "tests" / "test_nested_normalization_contracts.py").exists()


def _raw_json_exists(payload: dict[str, Any], root: Path) -> bool:
    return (root / "docs" / "benchmarks" / "data" / "oss-code-quality-benchmark-2026-06-12.json").exists()


def _benchmark_doc_exists(payload: dict[str, Any], root: Path) -> bool:
    return (root / "docs" / "benchmarks" / "oss-code-quality-benchmark-2026-06-12.md").exists()


def _provenance_exists(payload: dict[str, Any], root: Path) -> bool:
    return (root / "docs" / "benchmarks" / "data" / "oss-benchmark-provenance.json").exists()


def _failed_check_summary(checks: list[dict[str, str]]) -> str:
    failed = [check["name"] for check in checks if check["status"] == "failed"]
    return ", ".join(failed)


def _status_summary(scenarios: list[dict[str, Any]]) -> dict[str, int]:
    summary = {"passed": 0, "failed": 0}
    for scenario in scenarios:
        status = str(scenario.get("status") or "failed")
        summary[status] = summary.get(status, 0) + 1
    return summary
