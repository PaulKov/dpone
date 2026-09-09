"""Full certification suite gate across matrix, benchmark, lineage, strategy, and evidence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from dpone.ops.certification_artifacts import CertificationArtifactReader, CertificationEvidenceItem


@dataclass(frozen=True, slots=True)
class CertificationSuiteReport:
    suite_id: str
    passed: bool
    blockers: tuple[str, ...]
    case_count: int
    evidence_count: int
    items: tuple[CertificationEvidenceItem, ...]
    output_dir: str
    evidence_status: str = "UNVERIFIED"
    release_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "dpone.certification_suite.v1",
            "suite_id": self.suite_id,
            "release_id": self.release_id,
            "passed": self.passed,
            "evidence_status": self.evidence_status,
            "blockers": list(self.blockers),
            "case_count": self.case_count,
            "evidence_count": self.evidence_count,
            "items": [item.to_dict() for item in self.items],
            "output_dir": self.output_dir,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone certification suite",
            "",
            f"- Suite ID: `{self.suite_id}`",
            f"- Passed: `{self.passed}`",
            f"- Evidence status: `{self.evidence_status}`",
            f"- Case count: `{self.case_count}`",
            f"- Evidence count: `{self.evidence_count}`",
            "",
            "| evidence | status | required | sha256 | summary | path |",
            "|---|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(
                f"| `{item.name}` | {status} | `{item.required}` | `{item.sha256}` | {item.summary} | `{item.path}` |"
            )
        if self.blockers:
            lines.extend(["", "Certification suite blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Certification suite runbook",
                "",
                "1. Run the same certification profile again before changing expected behavior.",
                "2. Fix red source -> sink matrix cases before publishing connector badges.",
                "3. Re-run benchmark baseline on the same hardware/profile before updating baselines.",
                "4. Re-export lineage and dbt lineage after runtime or transformation changes.",
                "5. Inspect strategy certification bundles before promoting replay/matrix/connector evidence.",
                "6. Attach `certification_suite.json` to release, PR, or manual CI evidence.",
                "",
            ]
        )
        return "\n".join(lines)


class CertificationSuiteService:
    """Evaluates a full certification evidence bundle for release/matrix readiness."""

    def __init__(self, *, artifact_reader: CertificationArtifactReader | None = None) -> None:
        self._artifact_reader = artifact_reader or CertificationArtifactReader()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        suite_id: str,
        release_id: str | None = None,
        certification_report_path: str | Path,
        benchmark_baseline_path: str | Path | None = None,
        lineage_report_path: str | Path | None = None,
        dbt_lineage_report_path: str | Path | None = None,
        evidence_bundle_path: str | Path | None = None,
        strategy_certification_bundle_path: str | Path | None = None,
        require_benchmark: bool = False,
        require_lineage: bool = False,
        require_dbt_lineage: bool = False,
        require_evidence: bool = False,
        require_strategy_certification: bool = False,
    ) -> CertificationSuiteReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        items = self._items(
            release_id=release_id,
            certification_report_path=certification_report_path,
            benchmark_baseline_path=benchmark_baseline_path,
            lineage_report_path=lineage_report_path,
            dbt_lineage_report_path=dbt_lineage_report_path,
            evidence_bundle_path=evidence_bundle_path,
            strategy_certification_bundle_path=strategy_certification_bundle_path,
            require_benchmark=require_benchmark,
            require_lineage=require_lineage,
            require_dbt_lineage=require_dbt_lineage,
            require_evidence=require_evidence,
            require_strategy_certification=require_strategy_certification,
        )
        blockers = tuple(self._blocker(item) for item in items if not item.passed)
        certification_item = next(item for item in items if item.name == "certification_report")
        evidence_status = certification_item.evidence_status or "UNVERIFIED"
        report = CertificationSuiteReport(
            suite_id=suite_id,
            passed=not blockers,
            blockers=blockers,
            case_count=self._artifact_reader.case_count(certification_report_path),
            evidence_count=len(items),
            items=items,
            output_dir=str(directory),
            evidence_status="PASS" if not blockers and evidence_status == "PASS" else evidence_status,
            release_id=release_id,
        )
        (directory / "certification_suite.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "certification_suite.md").write_text(report.to_markdown(), encoding="utf-8")
        self._write_index(directory, report)
        return report

    def _items(
        self,
        *,
        release_id: str | None,
        certification_report_path: str | Path,
        benchmark_baseline_path: str | Path | None,
        lineage_report_path: str | Path | None,
        dbt_lineage_report_path: str | Path | None,
        evidence_bundle_path: str | Path | None,
        strategy_certification_bundle_path: str | Path | None,
        require_benchmark: bool,
        require_lineage: bool,
        require_dbt_lineage: bool,
        require_evidence: bool,
        require_strategy_certification: bool,
    ) -> tuple[CertificationEvidenceItem, ...]:
        raw_items = (
            self._artifact_reader.read(
                name="certification_report",
                path=certification_report_path,
                required=True,
                expected_release_id=release_id,
            ),
            self._artifact_reader.read(
                name="benchmark_baseline",
                path=benchmark_baseline_path,
                required=require_benchmark,
                expected_release_id=release_id,
            ),
            self._artifact_reader.read(
                name="lineage_report",
                path=lineage_report_path,
                required=require_lineage,
                expected_release_id=release_id,
            ),
            self._artifact_reader.read(
                name="dbt_lineage_report",
                path=dbt_lineage_report_path,
                required=require_dbt_lineage,
                expected_release_id=release_id,
            ),
            self._artifact_reader.read(
                name="evidence_bundle",
                path=evidence_bundle_path,
                required=require_evidence,
                expected_release_id=release_id,
            ),
            self._artifact_reader.read(
                name="strategy_certification_bundle",
                path=strategy_certification_bundle_path,
                required=require_strategy_certification,
                expected_release_id=release_id,
            ),
        )
        return tuple(item for item in raw_items if item is not None)

    @staticmethod
    def _blocker(item: CertificationEvidenceItem) -> str:
        if item.missing:
            suffix = "missing"
        elif item.identity_status == "MISSING":
            suffix = "release_id_missing"
        elif item.identity_status == "MISMATCHED":
            suffix = "release_id_mismatch"
        else:
            suffix = "not_passed"
        return f"{item.name}.{suffix}"

    @staticmethod
    def _write_index(directory: Path, report: CertificationSuiteReport) -> None:
        index_path = directory / "certification_suite_index.json"
        index_path.write_text(
            json.dumps(
                {
                    "suite_count": 1,
                    "suites": [report.to_dict()],
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
