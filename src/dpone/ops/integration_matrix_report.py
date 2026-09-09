"""Integration matrix artifact aggregation for certification-suite gates."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class IntegrationMatrixCaseResult:
    case_id: str
    source: str
    sink: str
    strategy: str
    passed: bool
    preflight_present: bool
    behavior_present: bool
    expected_row_count: int | None
    actual_row_count: int | None
    quality_checks: tuple[str, ...]
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class IntegrationMatrixCertificationReport:
    passed: bool
    total_cases: int
    passed_cases: int
    failed_cases: int
    blockers: tuple[str, ...]
    results: tuple[IntegrationMatrixCaseResult, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "total_cases": self.total_cases,
            "passed_cases": self.passed_cases,
            "failed_cases": self.failed_cases,
            "blockers": list(self.blockers),
            "results": [item.to_dict() for item in self.results],
            "output_dir": self.output_dir,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Source/Sink integration matrix certification report",
            "",
            f"- Passed: `{self.passed}`",
            f"- Total cases: `{self.total_cases}`",
            f"- Passed cases: `{self.passed_cases}`",
            f"- Failed cases: `{self.failed_cases}`",
            "",
            "| case | source | sink | strategy | status | rows | blockers |",
            "|---|---|---|---|---|---|---|",
        ]
        for result in self.results:
            status = "passed" if result.passed else "failed"
            rows = _rows_summary(result.expected_row_count, result.actual_row_count)
            blockers = ", ".join(result.blockers) if result.blockers else "-"
            lines.append(
                f"| `{result.case_id}` | `{result.source}` | `{result.sink}` | `{result.strategy}` | {status} | {rows} | {blockers} |"
            )
        if self.blockers:
            lines.extend(["", "## Blockers", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Runbook",
                "",
                "1. Re-run the focused matrix case with `DPONE_MATRIX_CASE_ID=<case_id>`.",
                "2. If behavior is missing, inspect the pytest output before trusting row-count evidence.",
                "3. If checksums or row counts differ, open the `__behavior.json` artifact for that case.",
                "4. Feed this report into `dpone ops certification-suite` as `--certification-report`.",
                "",
            ]
        )
        return "\n".join(lines)


class IntegrationMatrixReportService:
    """Builds a certification-suite-compatible report from matrix case artifacts."""

    def build(self, *, artifact_dir: str | Path, output_dir: str | Path) -> IntegrationMatrixCertificationReport:
        source_dir = Path(artifact_dir)
        target_dir = Path(output_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        case_payloads = _read_case_payloads(source_dir)
        behavior_payloads = _read_behavior_payloads(source_dir)
        results = tuple(
            _result_for_case(
                case_id=case_id, case_payload=case_payload, behavior_payload=behavior_payloads.get(case_id)
            )
            for case_id, case_payload in sorted(case_payloads.items())
        )
        blockers = tuple(blocker for result in results for blocker in result.blockers)
        report = IntegrationMatrixCertificationReport(
            passed=bool(results) and not blockers,
            total_cases=len(results),
            passed_cases=sum(1 for result in results if result.passed),
            failed_cases=sum(1 for result in results if not result.passed),
            blockers=blockers,
            results=results,
            output_dir=str(target_dir),
        )
        (target_dir / "certification_report.json").write_text(report.to_json(), encoding="utf-8")
        (target_dir / "certification_report.md").write_text(report.to_markdown(), encoding="utf-8")
        return report


def _read_case_payloads(directory: Path) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.glob("*.json")):
        if path.name.endswith("__behavior.json") or path.name in {"junit.json", "certification_report.json"}:
            continue
        payload = _read_json(path)
        case_id = str(payload.get("case_id") or path.stem)
        payloads[case_id] = payload
    return payloads


def _read_behavior_payloads(directory: Path) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.glob("*__behavior.json")):
        payload = _read_json(path)
        case_id = str(payload.get("case_id") or path.name.removesuffix("__behavior.json"))
        payloads[case_id] = payload
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"passed": False, "blockers": ["artifact.invalid_json"]}
    return payload if isinstance(payload, dict) else {"passed": False, "blockers": ["artifact.non_object"]}


def _result_for_case(
    *, case_id: str, case_payload: dict[str, Any], behavior_payload: dict[str, Any] | None
) -> IntegrationMatrixCaseResult:
    source = str(
        case_payload.get("source") or behavior_payload.get("source")
        if behavior_payload
        else case_payload.get("source") or "unknown"
    )
    sink = str(
        case_payload.get("sink") or behavior_payload.get("sink")
        if behavior_payload
        else case_payload.get("sink") or "unknown"
    )
    strategy = str(
        case_payload.get("strategy") or behavior_payload.get("strategy")
        if behavior_payload
        else case_payload.get("strategy") or "unknown"
    )
    blockers = list(_payload_blockers(case_payload))
    if behavior_payload is None:
        blockers.append(f"matrix_behavior.missing:{case_id}")
        passed = False
        expected = None
        actual = None
        quality_checks: tuple[str, ...] = tuple()
    else:
        blockers.extend(_payload_blockers(behavior_payload))
        passed = bool(behavior_payload.get("passed", False)) and not blockers
        expected = _optional_int(behavior_payload.get("expected_row_count"))
        actual = _optional_int(behavior_payload.get("actual_row_count"))
        quality_checks = tuple(str(item) for item in behavior_payload.get("quality_checks", []) if str(item))
        if expected is not None and actual is not None and expected != actual:
            blockers.append(f"matrix_rows.mismatch:{case_id}")
            passed = False
    return IntegrationMatrixCaseResult(
        case_id=case_id,
        source=source,
        sink=sink,
        strategy=strategy,
        passed=passed,
        preflight_present=bool(case_payload),
        behavior_present=behavior_payload is not None,
        expected_row_count=expected,
        actual_row_count=actual,
        quality_checks=quality_checks,
        blockers=tuple(blockers),
    )


def _payload_blockers(payload: dict[str, Any]) -> tuple[str, ...]:
    blockers = payload.get("blockers", [])
    if isinstance(blockers, list):
        return tuple(str(item) for item in blockers if str(item))
    return tuple()


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _rows_summary(expected: int | None, actual: int | None) -> str:
    if expected is None and actual is None:
        return "-"
    return f"`{actual}` / `{expected}`"
