"""Certification history and regression trend artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import certification_trust


@dataclass(frozen=True, slots=True)
class CertificationHistoryReport:
    release: str
    passed: bool
    status: str
    badge: str
    current_case_count: int
    previous_case_count: int
    new_failures: tuple[str, ...]
    fixed_failures: tuple[str, ...]
    unchanged_failures: tuple[str, ...]
    history_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone certification history",
            "",
            f"- Release: `{self.release}`",
            f"- Status: `{self.status}`",
            f"- Badge: `{self.badge}`",
            f"- Passed: `{self.passed}`",
            f"- Current cases: `{self.current_case_count}`",
            f"- Previous cases: `{self.previous_case_count}`",
            "",
            "| group | cases |",
            "|---|---|",
            f"| `new_failures` | {self._join_cases(self.new_failures)} |",
            f"| `fixed_failures` | {self._join_cases(self.fixed_failures)} |",
            f"| `unchanged_failures` | {self._join_cases(self.unchanged_failures)} |",
        ]
        if self.new_failures:
            lines.extend(["", "Fix certification regressions before publishing a stronger connector badge."])
        return "\n".join(lines) + "\n"

    @staticmethod
    def _join_cases(cases: tuple[str, ...]) -> str:
        return ", ".join(f"`{case}`" for case in cases) if cases else "-"


class CertificationHistoryService:
    """Records immutable certification trend artifacts."""

    def record(
        self,
        *,
        history_dir: str | Path,
        release: str,
        current_report_path: str | Path,
        previous_report_path: str | Path | None = None,
    ) -> CertificationHistoryReport:
        directory = Path(history_dir)
        directory.mkdir(parents=True, exist_ok=True)
        current, current_trusted = self._load_case_statuses(Path(current_report_path))
        previous = self._load_case_statuses(Path(previous_report_path))[0] if previous_report_path else {}
        new_failures = tuple(
            sorted(case for case, passed in current.items() if not passed and previous.get(case, True))
        )
        fixed_failures = tuple(
            sorted(case for case, passed in current.items() if passed and previous.get(case) is False)
        )
        unchanged_failures = tuple(
            sorted(case for case, passed in current.items() if not passed and previous.get(case) is False)
        )
        passed = current_trusted and all(current.values()) and not new_failures
        status = self._status(passed=passed, new_failures=new_failures, unchanged_failures=unchanged_failures)
        history_path = directory / f"{self._safe_release(release)}__certification_history.json"
        markdown_path = directory / f"{self._safe_release(release)}__certification_history.md"
        report = CertificationHistoryReport(
            release=release,
            passed=passed,
            status=status,
            badge=status,
            current_case_count=len(current),
            previous_case_count=len(previous),
            new_failures=new_failures,
            fixed_failures=fixed_failures,
            unchanged_failures=unchanged_failures,
            history_path=str(history_path),
            markdown_path=str(markdown_path),
        )
        history_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        self._write_index(directory, report)
        return report

    def _load_case_statuses(self, path: Path) -> tuple[dict[str, bool], bool]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError(f"Certification report must be a JSON object: {path}")
        results = payload.get("results", [])
        statuses: dict[str, bool] = {}
        if isinstance(results, list):
            for item in results:
                if isinstance(item, Mapping) and "case_id" in item:
                    statuses[str(item["case_id"])] = bool(item.get("passed", False))
        return statuses, certification_trust(payload).passed

    def _write_index(self, directory: Path, report: CertificationHistoryReport) -> None:
        index_path = directory / "certification_history_index.json"
        existing = self._load_index(index_path)
        entries = [entry for entry in existing if entry.get("release") != report.release]
        entries.append(report.to_dict())
        entries.sort(key=lambda item: str(item.get("release", "")))
        index_path.write_text(
            json.dumps({"releases": entries}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _load_index(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        releases = payload.get("releases", []) if isinstance(payload, Mapping) else []
        return [dict(item) for item in releases if isinstance(item, Mapping)]

    @staticmethod
    def _status(
        *,
        passed: bool,
        new_failures: tuple[str, ...],
        unchanged_failures: tuple[str, ...],
    ) -> str:
        if new_failures:
            return "regression"
        if passed:
            return "passing"
        if unchanged_failures:
            return "failing"
        return "incomplete"

    @staticmethod
    def _safe_release(release: str) -> str:
        return "".join(char if char.isalnum() or char in {"-", "_", "."} else "_" for char in release)
