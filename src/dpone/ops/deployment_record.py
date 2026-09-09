"""Deployment audit records for dpone release operations."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file

_SUCCESS_STATUSES = frozenset({"succeeded", "completed"})


@dataclass(frozen=True, slots=True)
class DeploymentRecordReport:
    deployment_id: str
    change_id: str
    release: str
    environment: str
    actor: str
    approval_record_path: str
    approval_record_sha256: str
    status: str
    passed: bool
    blockers: tuple[str, ...]
    started_at: str
    finished_at: str
    rollback_artifact_path: str | None
    post_checks: dict[str, bool]
    post_checks_count: int
    failed_post_checks: tuple[str, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "deployment_id": self.deployment_id,
            "change_id": self.change_id,
            "release": self.release,
            "environment": self.environment,
            "actor": self.actor,
            "approval_record_path": self.approval_record_path,
            "approval_record_sha256": self.approval_record_sha256,
            "status": self.status,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "rollback_artifact_path": self.rollback_artifact_path,
            "post_checks": self.post_checks,
            "post_checks_count": self.post_checks_count,
            "failed_post_checks": list(self.failed_post_checks),
            "output_dir": self.output_dir,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone deployment record",
            "",
            f"- Deployment ID: `{self.deployment_id}`",
            f"- Change ID: `{self.change_id}`",
            f"- Release: `{self.release}`",
            f"- Environment: `{self.environment}`",
            f"- Actor: `{self.actor}`",
            f"- Status: `{self.status}`",
            f"- Passed: `{self.passed}`",
            f"- Blockers: `{len(self.blockers)}`",
            f"- Started at: `{self.started_at}`",
            f"- Finished at: `{self.finished_at}`",
            f"- Rollback artifact: `{self.rollback_artifact_path or 'not set'}`",
            "",
            "| post check | status |",
            "|---|---|",
        ]
        for name, passed in sorted(self.post_checks.items()):
            lines.append(f"| `{name}` | `{'pass' if passed else 'fail'}` |")
        if self.blockers:
            lines.extend(["", "Deployment record blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Deployment runbook",
                "",
                "1. Verify the approval record checksum before accepting the deployment record.",
                "2. Investigate every failed post-deploy check before advancing release state.",
                "3. Attach rollback evidence when deployment status is failed or partial.",
                "4. Keep `deployment_record.json` immutable for release audit.",
                "",
            ]
        )
        return "\n".join(lines)


class DeploymentRecordService:
    """Records the factual outcome of applying a release to an environment."""

    def record(
        self,
        *,
        output_dir: str | Path,
        deployment_id: str,
        environment: str,
        actor: str,
        approval_record_path: str | Path,
        status: str,
        post_checks: Mapping[str, bool],
        rollback_artifact_path: str | Path | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
    ) -> DeploymentRecordReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        approval_path = Path(approval_record_path)
        approval_payload = self._payload(approval_path)
        normalized_checks = {name: bool(value) for name, value in sorted(post_checks.items())}
        failed_post_checks = tuple(name for name, passed in normalized_checks.items() if not passed)
        blockers = self._blockers(
            approval_payload=approval_payload,
            status=status,
            failed_post_checks=failed_post_checks,
        )
        timestamp = datetime.now(UTC).isoformat()
        report = DeploymentRecordReport(
            deployment_id=deployment_id,
            change_id=str(approval_payload.get("change_id", "unknown")),
            release=str(approval_payload.get("release", "unknown")),
            environment=environment,
            actor=actor,
            approval_record_path=str(approval_path),
            approval_record_sha256=sha256_file(approval_path),
            status=status,
            passed=not blockers,
            blockers=blockers,
            started_at=started_at or timestamp,
            finished_at=finished_at or timestamp,
            rollback_artifact_path=str(rollback_artifact_path) if rollback_artifact_path is not None else None,
            post_checks=normalized_checks,
            post_checks_count=len(normalized_checks),
            failed_post_checks=failed_post_checks,
            output_dir=str(directory),
        )
        (directory / "deployment_record.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "deployment_record.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    @staticmethod
    def _blockers(
        *,
        approval_payload: Mapping[str, Any],
        status: str,
        failed_post_checks: tuple[str, ...],
    ) -> tuple[str, ...]:
        blockers: list[str] = []
        if not bool(approval_payload.get("passed", False)):
            blockers.append("deployment.approval_not_passed")
        if status not in _SUCCESS_STATUSES:
            blockers.append(f"deployment.status_{status}")
        blockers.extend(f"post_check.{name}" for name in failed_post_checks)
        return tuple(blockers)

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}
