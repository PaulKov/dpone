"""Final post-deploy verification gate for dpone release operations."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import (
    production_artifact_payload_passed,
)
from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class PostDeployVerifyItem:
    name: str
    path: str
    sha256: str
    passed: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PostDeployVerifyReport:
    deployment_id: str
    release: str
    environment: str
    status: str
    passed: bool
    blockers: tuple[str, ...]
    deployment_record_path: str
    deployment_record_sha256: str
    checks_count: int
    items: tuple[PostDeployVerifyItem, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "deployment_id": self.deployment_id,
            "release": self.release,
            "environment": self.environment,
            "status": self.status,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "deployment_record_path": self.deployment_record_path,
            "deployment_record_sha256": self.deployment_record_sha256,
            "checks_count": self.checks_count,
            "output_dir": self.output_dir,
            "items": [item.to_dict() for item in self.items],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone post-deploy verification",
            "",
            f"- Deployment ID: `{self.deployment_id}`",
            f"- Release: `{self.release}`",
            f"- Environment: `{self.environment}`",
            f"- Status: `{self.status}`",
            f"- Passed: `{self.passed}`",
            f"- Blockers: `{len(self.blockers)}`",
            "",
            "| check | status | sha256 | summary | path |",
            "|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(f"| `{item.name}` | {status} | `{item.sha256}` | {item.summary} | `{item.path}` |")
        if self.blockers:
            lines.extend(["", "Post-deploy verification blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Post-deploy runbook",
                "",
                "1. Review every failed check before closing the release.",
                "2. If deployment record is red, attach rollback evidence and open an incident pack.",
                "3. Re-run SLO, reconciliation diff, security, and data-contract artifacts after remediation.",
                "4. Close the release only when `status=release_closed` and `passed=true`.",
                "",
            ]
        )
        return "\n".join(lines)


class PostDeployVerifyService:
    """Aggregates post-deploy evidence into a final close-or-rollback gate."""

    def verify(
        self,
        *,
        output_dir: str | Path,
        deployment_record_path: str | Path,
        artifacts: Mapping[str, str | Path],
    ) -> PostDeployVerifyReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        deployment_path = Path(deployment_record_path)
        deployment_payload = self._payload(deployment_path)
        deployment_item = self._item("deployment_record", deployment_path)
        artifact_items = tuple(self._item(name, Path(path)) for name, path in sorted(artifacts.items()))
        items = (deployment_item, *artifact_items)
        blockers = tuple(item.name for item in items if not item.passed)
        passed = not blockers
        report = PostDeployVerifyReport(
            deployment_id=str(deployment_payload.get("deployment_id", "unknown")),
            release=str(deployment_payload.get("release", "unknown")),
            environment=str(deployment_payload.get("environment", "unknown")),
            status="release_closed" if passed else "rollback_required",
            passed=passed,
            blockers=blockers,
            deployment_record_path=str(deployment_path),
            deployment_record_sha256=sha256_file(deployment_path),
            checks_count=len(items),
            items=items,
            output_dir=str(directory),
        )
        (directory / "post_deploy_verify.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "post_deploy_verify.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _item(self, name: str, path: Path) -> PostDeployVerifyItem:
        if not path.exists():
            return PostDeployVerifyItem(
                name=name,
                path=str(path),
                sha256="0" * 64,
                passed=False,
                summary="artifact missing",
            )
        payload = self._payload(path)
        return PostDeployVerifyItem(
            name=name,
            path=str(path),
            sha256=sha256_file(path),
            passed=self._passed(name, payload),
            summary=self._summary(payload),
        )

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        if not path.exists() or path.suffix.lower() != ".json":
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}

    @staticmethod
    def _passed(name: str, payload: Mapping[str, Any]) -> bool:
        if payload.get("failed_post_checks"):
            return False
        return production_artifact_payload_passed(payload, name=name)

    @staticmethod
    def _summary(payload: Mapping[str, Any]) -> str:
        for key in (
            "blockers",
            "violations",
            "findings",
            "results",
            "samples",
            "items",
            "failed_post_checks",
        ):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        if "status" in payload:
            return f"status={payload['status']}"
        return "passed" if bool(payload.get("passed", True)) else "failed"
