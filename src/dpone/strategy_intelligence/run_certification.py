"""Certification for the real ``dpone run`` native-transfer resume path.

The service is deliberately runner-neutral: production tooling can pass the
public Python API, CLI wrapper, or a test harness as ``run_once``. The
certification logic only validates run reports and checkpoint-store evidence.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone._compat import UTC
from dpone.runtime.lineage.partition_checkpoint_store import PartitionCheckpointStore
from dpone.services.run_manifest import RunManifestResult

SCHEMA_VERSION = "dpone.native_transfer.run_certification.v1"

RunOnce = Callable[[str], RunManifestResult]


@dataclass(frozen=True)
class NativeTransferRunCertificationRequest:
    """Input for certifying real repeated ``dpone run`` behavior."""

    manifest_path: Path
    run_once: RunOnce
    runtime_report_dir: Path
    checkpoint_store: PartitionCheckpointStore
    first_run_id: str = "dpone-native-transfer-first"
    second_run_id: str = "dpone-native-transfer-second"


@dataclass(frozen=True)
class NativeTransferRunCertificationResult:
    """Audit-friendly result for repeated-run native transfer certification."""

    request: NativeTransferRunCertificationRequest
    first_run: RunManifestResult
    second_run: RunManifestResult
    first_runtime_report: dict[str, Any]
    second_runtime_report: dict[str, Any]
    checkpoint_summary: dict[str, int]
    checks: tuple[dict[str, Any], ...]

    @property
    def passed(self) -> bool:
        return all(bool(check["passed"]) for check in self.checks)

    @property
    def checks_by_name(self) -> dict[str, dict[str, Any]]:
        return {str(check["name"]): check for check in self.checks}

    @property
    def first_resume_summary(self) -> dict[str, int]:
        return _resume_summary(self.first_runtime_report)

    @property
    def second_resume_summary(self) -> dict[str, int]:
        return _resume_summary(self.second_runtime_report)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "manifest_path": str(self.request.manifest_path),
            "passed": self.passed,
            "first_run": self.first_run.to_dict(),
            "second_run": self.second_run.to_dict(),
            "checkpoint_summary": self.checkpoint_summary,
            "first_runtime_report": self.first_runtime_report,
            "second_runtime_report": self.second_runtime_report,
            "checks": list(self.checks),
        }

    def to_markdown(self) -> str:
        lines = [
            "# Native transfer dpone run certification",
            "",
            f"- schema_version: `{SCHEMA_VERSION}`",
            f"- manifest: `{self.request.manifest_path}`",
            f"- passed: `{self.passed}`",
            f"- first_run_id: `{self.first_run.run_id}`",
            f"- second_run_id: `{self.second_run.run_id}`",
            f"- second_skip: `{self.second_resume_summary.get('skip', 0)}`",
            f"- second_retry: `{self.second_resume_summary.get('retry', 0)}`",
            "",
            "| check | passed | details |",
            "|---|---:|---|",
        ]
        for check in self.checks:
            lines.append(
                f"| `{check['name']}` | `{check['passed']}` | `{json.dumps(check['details'], sort_keys=True)}` |"
            )
        lines.append("")
        return "\n".join(lines)


class NativeTransferRunCertificationService:
    """Certify that a repeated ``dpone run`` is checkpoint-idempotent."""

    def certify(self, request: NativeTransferRunCertificationRequest) -> NativeTransferRunCertificationResult:
        before_first = _report_paths(request.runtime_report_dir)
        first = request.run_once(request.first_run_id)
        first_report = _load_runtime_report(request.runtime_report_dir, request.first_run_id, baseline=before_first)
        before_second = _report_paths(request.runtime_report_dir)
        second = request.run_once(request.second_run_id)
        second_report = _load_runtime_report(request.runtime_report_dir, request.second_run_id, baseline=before_second)
        checkpoint_summary = request.checkpoint_store.summary()
        committed = int(checkpoint_summary.get("committed", 0))
        second_summary = _resume_summary(second_report)
        checks = (
            _check("first_run_passed", first.passed, status=first.result.status),
            _check("second_run_passed", second.passed, status=second.result.status),
            _check("committed_checkpoints_exist", committed > 0, committed=committed),
            _check(
                "runtime_reports_exist",
                bool(first_report) and bool(second_report),
                first_run_id=request.first_run_id,
                second_run_id=request.second_run_id,
            ),
            _check(
                "second_run_noop_skip",
                second_summary.get("skip", 0) == committed
                and second_summary.get("retry", 0) == 0
                and second.result.inserted_rows == 0,
                committed=committed,
                second_summary=second_summary,
                second_inserted_rows=second.result.inserted_rows,
            ),
        )
        return NativeTransferRunCertificationResult(
            request=request,
            first_run=first,
            second_run=second,
            first_runtime_report=first_report,
            second_runtime_report=second_report,
            checkpoint_summary=checkpoint_summary,
            checks=checks,
        )


class NativeTransferRunEvidenceWriter:
    """Persist repeated-run certification evidence as JSON and Markdown."""

    def __init__(self, output_dir: str | Path) -> None:
        self._output_dir = Path(output_dir)

    def write(self, result: NativeTransferRunCertificationResult) -> tuple[Path, Path]:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        json_path = self._output_dir / "native_transfer_run_certification.json"
        md_path = self._output_dir / "native_transfer_run_certification.md"
        json_path.write_text(json.dumps(result.to_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        md_path.write_text(result.to_markdown(), encoding="utf-8")
        return json_path, md_path


def _report_paths(report_dir: Path) -> set[Path]:
    return set(report_dir.glob("native_transfer_runtime_*.json")) if report_dir.exists() else set()


def _load_runtime_report(report_dir: Path, run_id: str, *, baseline: set[Path] | None = None) -> dict[str, Any]:
    expected = report_dir / f"native_transfer_runtime_{_safe_name(run_id)}.json"
    if expected.exists():
        payload = json.loads(expected.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    candidates = sorted(_report_paths(report_dir) - (baseline or set()), key=lambda item: item.stat().st_mtime_ns)
    if not candidates:
        return {}
    payload = json.loads(candidates[-1].read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _resume_summary(report: dict[str, Any]) -> dict[str, int]:
    summary = ((report.get("resume_plan") or {}).get("summary") or {}) if report else {}
    return {"skip": int(summary.get("skip", 0)), "retry": int(summary.get("retry", 0))}


def _check(name: str, passed: bool, **details: Any) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "details": details}


def _safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
