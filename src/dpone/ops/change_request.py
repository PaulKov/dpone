"""Machine-readable change request evidence for dpone release approvals."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import (
    production_artifact_payload_passed,
)
from dpone.ops.checksums import sha256_file

_ALLOWED_RISK_LEVELS = frozenset({"low", "medium", "high", "critical"})


@dataclass(frozen=True, slots=True)
class ChangeRequestItem:
    name: str
    path: str
    sha256: str
    passed: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ChangeRequestReport:
    change_id: str
    release: str
    target_environment: str
    risk_level: str
    requested_by: str
    approvers: tuple[str, ...]
    requested_at: str
    expires_at: str | None
    passed: bool
    blockers: tuple[str, ...]
    items: tuple[ChangeRequestItem, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "change_id": self.change_id,
            "release": self.release,
            "target_environment": self.target_environment,
            "risk_level": self.risk_level,
            "requested_by": self.requested_by,
            "approvers": list(self.approvers),
            "requested_at": self.requested_at,
            "expires_at": self.expires_at,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "output_dir": self.output_dir,
            "items": [item.to_dict() for item in self.items],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone change request",
            "",
            f"- Change ID: `{self.change_id}`",
            f"- Release: `{self.release}`",
            f"- Target environment: `{self.target_environment}`",
            f"- Risk level: `{self.risk_level}`",
            f"- Requested by: `{self.requested_by}`",
            f"- Approvers: `{', '.join(self.approvers) or 'none'}`",
            f"- Requested at: `{self.requested_at}`",
            f"- Expires at: `{self.expires_at or 'not set'}`",
            f"- Passed: `{self.passed}`",
            f"- Blockers: `{len(self.blockers)}`",
            "",
            "| artifact | status | sha256 | summary | path |",
            "|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(f"| `{item.name}` | {status} | `{item.sha256}` | {item.summary} | `{item.path}` |")
        if self.blockers:
            lines.extend(["", "Change request blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Approval runbook",
                "",
                "1. Review every artifact checksum and status.",
                "2. Confirm risk level matches the release blast radius.",
                "3. Confirm all required approvers are present before promotion.",
                "4. Attach `change_request.json` to the release approval record.",
                "5. Regenerate the change request after any artifact changes.",
                "",
            ]
        )
        return "\n".join(lines)


class ChangeRequestService:
    """Creates approval evidence from release, drift, promotion, and rollback artifacts."""

    def create(
        self,
        *,
        output_dir: str | Path,
        change_id: str,
        release: str,
        target_environment: str,
        risk_level: str,
        requested_by: str,
        approvers: Sequence[str],
        artifacts: Mapping[str, str | Path],
        expires_at: str | None = None,
    ) -> ChangeRequestReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        normalized_approvers = tuple(approver for approver in approvers if approver)
        items = tuple(self._item(name=name, path=Path(path)) for name, path in sorted(artifacts.items()))
        blockers = self._blockers(risk_level=risk_level, approvers=normalized_approvers, items=items)
        report = ChangeRequestReport(
            change_id=change_id,
            release=release,
            target_environment=target_environment,
            risk_level=risk_level,
            requested_by=requested_by,
            approvers=normalized_approvers,
            requested_at=datetime.now(UTC).isoformat(),
            expires_at=expires_at,
            passed=not blockers,
            blockers=blockers,
            items=items,
            output_dir=str(directory),
        )
        (directory / "change_request.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "change_request.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _item(self, *, name: str, path: Path) -> ChangeRequestItem:
        if not path.exists():
            return ChangeRequestItem(
                name=name,
                path=str(path),
                sha256="0" * 64,
                passed=False,
                summary="artifact missing",
            )
        payload = self._payload(path)
        return ChangeRequestItem(
            name=name,
            path=str(path),
            sha256=sha256_file(path),
            passed=self._passed(name, payload),
            summary=self._summary(payload),
        )

    @staticmethod
    def _blockers(
        *,
        risk_level: str,
        approvers: tuple[str, ...],
        items: tuple[ChangeRequestItem, ...],
    ) -> tuple[str, ...]:
        blockers: list[str] = []
        if not approvers:
            blockers.append("approval.approvers_missing")
        if risk_level not in _ALLOWED_RISK_LEVELS:
            blockers.append("approval.risk_level_invalid")
        blockers.extend(item.name for item in items if not item.passed)
        return tuple(blockers)

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        if path.suffix.lower() != ".json":
            return {"passed": True, "summary": "non-json artifact"}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"passed": False, "summary": "invalid json artifact"}
        return payload if isinstance(payload, Mapping) else {"passed": False, "summary": "invalid json artifact"}

    @staticmethod
    def _passed(name: str, payload: Mapping[str, Any]) -> bool:
        return production_artifact_payload_passed(payload, name=name)

    @staticmethod
    def _summary(payload: Mapping[str, Any]) -> str:
        for key in ("blockers", "violations", "findings", "results", "items", "steps", "differences"):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        if "risk_level" in payload:
            return f"risk_level={payload['risk_level']}"
        if "status" in payload:
            return f"status={payload['status']}"
        return "passed" if bool(payload.get("passed", True)) else "failed"
