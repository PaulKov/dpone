"""Final immutable release closure evidence for dpone operations."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class ReleaseCloseReport:
    release: str
    status: str
    passed: bool
    blockers: tuple[str, ...]
    deployment_id: str
    environment: str
    closed_by: str
    closed_at: str
    notes: str
    post_deploy_verify_path: str
    post_deploy_verify_sha256: str
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "status": self.status,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "deployment_id": self.deployment_id,
            "environment": self.environment,
            "closed_by": self.closed_by,
            "closed_at": self.closed_at,
            "notes": self.notes,
            "post_deploy_verify_path": self.post_deploy_verify_path,
            "post_deploy_verify_sha256": self.post_deploy_verify_sha256,
            "output_dir": self.output_dir,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone release close",
            "",
            f"- Release: `{self.release}`",
            f"- Status: `{self.status}`",
            f"- Passed: `{self.passed}`",
            f"- Deployment ID: `{self.deployment_id}`",
            f"- Environment: `{self.environment}`",
            f"- Closed by: `{self.closed_by}`",
            f"- Closed at: `{self.closed_at}`",
            f"- Notes: {self.notes or 'none'}",
            f"- Post-deploy checksum: `{self.post_deploy_verify_sha256}`",
        ]
        if self.blockers:
            lines.extend(["", "Release close blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Release close runbook",
                "",
                "1. Close the release only when post-deploy verification is green.",
                "2. If status is `rollback_required`, attach rollback and incident artifacts.",
                "3. Keep `release_close.json` immutable as the final release audit record.",
                "4. Do not regenerate closure evidence without reviewing artifact checksum drift.",
                "",
            ]
        )
        return "\n".join(lines)


class ReleaseCloseService:
    """Creates the final close-or-rollback release record."""

    def close(
        self,
        *,
        output_dir: str | Path,
        release: str,
        closed_by: str,
        post_deploy_verify_path: str | Path,
        notes: str = "",
    ) -> ReleaseCloseReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        post_deploy_path = Path(post_deploy_verify_path)
        payload = self._payload(post_deploy_path)
        blockers = self._blockers(post_deploy_path, payload)
        report = ReleaseCloseReport(
            release=release,
            status="closed" if not blockers else "rollback_required",
            passed=not blockers,
            blockers=blockers,
            deployment_id=str(payload.get("deployment_id", "unknown")),
            environment=str(payload.get("environment", "unknown")),
            closed_by=closed_by,
            closed_at=datetime.now(UTC).isoformat(),
            notes=notes,
            post_deploy_verify_path=str(post_deploy_path),
            post_deploy_verify_sha256=sha256_file(post_deploy_path) if post_deploy_path.exists() else "0" * 64,
            output_dir=str(directory),
        )
        (directory / "release_close.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "release_close.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    @staticmethod
    def _blockers(path: Path, payload: Mapping[str, Any]) -> tuple[str, ...]:
        if not path.exists():
            return ("release_close.post_deploy_missing",)
        if not bool(payload.get("passed", False)):
            return ("release_close.post_deploy_not_passed",)
        return tuple()

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        if not path.exists() or path.suffix.lower() != ".json":
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}
