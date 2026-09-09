"""Native/local certification harness contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX, IntegrationMatrixCase


@dataclass(frozen=True, slots=True)
class CertificationCaseResult:
    case_id: str
    passed: bool
    artifact_path: str
    expected_row_count: int
    actual_row_count: int
    expected_checksum: str
    actual_checksum: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CertificationRunReport:
    profile: str
    case_count: int
    passed: bool
    results: tuple[CertificationCaseResult, ...]
    evidence_status: str = "UNVERIFIED"

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "case_count": self.case_count,
            "passed": self.passed,
            "evidence_status": self.evidence_status,
            "results": [item.to_dict() for item in self.results],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone certification run",
            "",
            f"- Profile: `{self.profile}`",
            f"- Cases: `{self.case_count}`",
            f"- Passed: `{self.passed}`",
            f"- Evidence status: `{self.evidence_status}`",
            "",
            "| case | status | rows | checksum | artifact |",
            "|---|---|---:|---|---|",
        ]
        for item in self.results:
            status = "pass" if item.passed else "fail"
            rows = f"{item.actual_row_count}/{item.expected_row_count}"
            checksum = f"{item.actual_checksum}/{item.expected_checksum}"
            lines.append(f"| `{item.case_id}` | {status} | {rows} | `{checksum}` | `{item.artifact_path}` |")
        return "\n".join(lines) + "\n"


class CertificationHarnessService:
    """Runs credential-free and local certification layers from the canonical matrix."""

    def run_mock_contract(
        self,
        *,
        artifact_dir: str | Path,
        source: str | None = None,
        sink: str | None = None,
        strategy: str | None = None,
        row_count: int | None = None,
    ) -> CertificationRunReport:
        directory = Path(artifact_dir)
        directory.mkdir(parents=True, exist_ok=True)
        selected = self._select_cases(source=source, sink=sink, strategy=strategy)
        results: list[CertificationCaseResult] = []
        for case in selected:
            behavior = case.simulate_mock_strategy_behavior(row_count=row_count)
            artifact_path = directory / f"{case.case_id}__behavior.json"
            artifact_path.write_text(behavior.to_json(), encoding="utf-8")
            results.append(
                CertificationCaseResult(
                    case_id=case.case_id,
                    passed=behavior.passed,
                    artifact_path=str(artifact_path),
                    expected_row_count=behavior.expected_row_count,
                    actual_row_count=behavior.actual_row_count,
                    expected_checksum=behavior.expected_checksum,
                    actual_checksum=behavior.actual_checksum,
                )
            )
        report = CertificationRunReport(
            profile="mock_contract",
            case_count=len(results),
            passed=bool(results) and all(item.passed for item in results),
            results=tuple(results),
            evidence_status="UNVERIFIED",
        )
        (directory / "certification_report.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "certification_report.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def plan_local_services(self) -> dict[str, object]:
        return {
            "profile": "local",
            "services": ["postgres", "mssql", "clickhouse", "kafka", "schema-registry", "minio"],
            "compose": "docker/docker-compose.integration.yml",
            "command": (
                "DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_MATRIX=1 "
                "DPONE_MATRIX_RUN_MODE=mock_local uv run pytest -m integration_matrix_mock tests/integration/matrix -q"
            ),
            "credentials_required": False,
        }

    def _select_cases(
        self,
        *,
        source: str | None,
        sink: str | None,
        strategy: str | None,
    ) -> tuple[IntegrationMatrixCase, ...]:
        cases = DEFAULT_INTEGRATION_MATRIX.cases
        return tuple(
            case
            for case in cases
            if (source is None or case.source == source)
            and (sink is None or case.sink == sink)
            and (strategy is None or case.strategy == strategy)
        )
