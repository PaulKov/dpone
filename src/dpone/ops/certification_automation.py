"""Self-service plan for recurring full certification automation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class CertificationAutomationStep:
    name: str
    command: str
    artifacts: tuple[str, ...]
    required: bool = True

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["artifacts"] = list(self.artifacts)
        return payload


@dataclass(frozen=True, slots=True)
class CertificationAutomationPlanReport:
    profile: str
    row_count: int
    passed: bool
    step_count: int
    required_artifacts: tuple[str, ...]
    steps: tuple[CertificationAutomationStep, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "row_count": self.row_count,
            "passed": self.passed,
            "step_count": self.step_count,
            "required_artifacts": list(self.required_artifacts),
            "steps": [step.to_dict() for step in self.steps],
            "output_dir": self.output_dir,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone full certification automation plan",
            "",
            f"- Profile: `{self.profile}`",
            f"- Row count: `{self.row_count}`",
            f"- Passed: `{self.passed}`",
            f"- Step count: `{self.step_count}`",
            "",
            "| step | required | artifacts | command |",
            "|---|---|---|---|",
        ]
        for step in self.steps:
            artifacts = ", ".join(f"`{artifact}`" for artifact in step.artifacts)
            lines.append(f"| `{step.name}` | `{step.required}` | {artifacts} | `{step.command}` |")
        lines.extend(
            [
                "",
                "## Runbook",
                "",
                "1. Run steps in order; do not build the suite before benchmark, lineage, evidence, and strategy artifacts exist.",
                "2. If a required artifact is missing, re-run the producing step instead of editing the suite by hand.",
                "3. Verify the evidence chain before promoting the certification result.",
                "",
            ]
        )
        return "\n".join(lines)


class CertificationAutomationPlanService:
    """Builds a deterministic plan for scheduled/manual full certification runs."""

    def build(
        self,
        *,
        output_dir: str | Path,
        profile: str = "mock_contract",
        row_count: int = 10000,
    ) -> CertificationAutomationPlanReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        normalized_profile = profile if profile in {"mock_contract", "mock_local", "vendor_live"} else "mock_contract"
        normalized_row_count = max(1, min(int(row_count), 100000))
        steps = self._steps(profile=normalized_profile, row_count=normalized_row_count)
        required_artifacts = tuple(dict.fromkeys(artifact for step in steps for artifact in step.artifacts))
        report = CertificationAutomationPlanReport(
            profile=normalized_profile,
            row_count=normalized_row_count,
            passed=True,
            step_count=len(steps),
            required_artifacts=required_artifacts,
            steps=steps,
            output_dir=str(directory),
        )
        (directory / "certification_automation_plan.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "certification_automation_plan.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    @staticmethod
    def _steps(*, profile: str, row_count: int) -> tuple[CertificationAutomationStep, ...]:
        matrix_env = (
            "DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_MATRIX=1 "
            f"DPONE_MATRIX_RUN_MODE={profile} DPONE_MATRIX_ROW_COUNT={row_count}"
        )
        return (
            CertificationAutomationStep(
                name="source_sink_matrix",
                command=f"{matrix_env} uv run pytest -m integration_matrix tests/integration/matrix -q",
                artifacts=("junit.xml",),
            ),
            CertificationAutomationStep(
                name="matrix_report",
                command="uv run dpone ops integration-matrix-report --artifact-dir test_artifacts/integration_matrix --output-dir test_artifacts/integration_matrix --format json",
                artifacts=("certification_report.json", "certification_report.md"),
            ),
            CertificationAutomationStep(
                name="benchmark_baseline",
                command="uv run dpone ops benchmark-baseline --metrics-json <metrics> --baseline-json <baseline> --format json",
                artifacts=("benchmark_baseline.json", "benchmark_baseline.md"),
            ),
            CertificationAutomationStep(
                name="run_registry",
                command="uv run dpone ops run-registry --run-result <run_result.json> --artifact certification=<certification_report.json> --format json",
                artifacts=("run_registry_index.json",),
            ),
            CertificationAutomationStep(
                name="lineage_export",
                command="uv run dpone ops lineage-export --run-registry-entry <run_registry.json> --format json",
                artifacts=("openlineage.json", "openlineage.md"),
            ),
            CertificationAutomationStep(
                name="evidence_bundle",
                command="uv run dpone ops evidence-bundle --artifact-dir <dir> --run-id <run_id> --format json",
                artifacts=("ops_evidence_bundle.json", "ops_evidence_bundle.md"),
            ),
            CertificationAutomationStep(
                name="strategy_certification_bundle",
                command="uv run dpone strategy certification-bundle --matrix-artifact <certification_report.json> --benchmark-artifact <benchmark_baseline.json> --format json",
                artifacts=("strategy_certification_bundle.json", "strategy_certification_bundle.md"),
            ),
            CertificationAutomationStep(
                name="certification_suite",
                command="uv run dpone ops certification-suite --require-benchmark --require-lineage --require-evidence --require-strategy-certification --format json",
                artifacts=("certification_suite.json", "certification_suite.md", "certification_suite_index.json"),
            ),
            CertificationAutomationStep(
                name="artifact_index",
                command="uv run dpone ops artifact-index --root test_artifacts --format json",
                artifacts=("artifact_index.json", "artifact_index.md"),
            ),
            CertificationAutomationStep(
                name="evidence_chain",
                command="uv run dpone ops evidence-chain --artifact-index artifact_index.json --format json",
                artifacts=("evidence_chain_index.json",),
            ),
            CertificationAutomationStep(
                name="evidence_chain_verify",
                command="uv run dpone ops evidence-chain-verify --chain-dir <chain_dir> --format json",
                artifacts=("evidence_chain_index.json",),
            ),
        )
