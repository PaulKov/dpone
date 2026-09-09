"""Approval audit records for dpone change requests."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class ApprovalDecision:
    actor: str
    decision: str
    comment: str
    decided_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ApprovalRecordReport:
    change_id: str
    release: str
    target_environment: str
    change_request_path: str
    change_request_sha256: str
    status: str
    passed: bool
    blockers: tuple[str, ...]
    quorum_required: int
    approvals_count: int
    rejections_count: int
    allowed_approvers: tuple[str, ...]
    decisions: tuple[ApprovalDecision, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "change_id": self.change_id,
            "release": self.release,
            "target_environment": self.target_environment,
            "change_request_path": self.change_request_path,
            "change_request_sha256": self.change_request_sha256,
            "status": self.status,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "quorum_required": self.quorum_required,
            "approvals_count": self.approvals_count,
            "rejections_count": self.rejections_count,
            "allowed_approvers": list(self.allowed_approvers),
            "output_dir": self.output_dir,
            "decisions": [decision.to_dict() for decision in self.decisions],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone approval record",
            "",
            f"- Change ID: `{self.change_id}`",
            f"- Release: `{self.release}`",
            f"- Target environment: `{self.target_environment}`",
            f"- Status: `{self.status}`",
            f"- Passed: `{self.passed}`",
            f"- Quorum required: `{self.quorum_required}`",
            f"- Approvals: `{self.approvals_count}`",
            f"- Rejections: `{self.rejections_count}`",
            f"- Blockers: `{len(self.blockers)}`",
            "",
            "| actor | decision | decided_at | comment |",
            "|---|---|---|---|",
        ]
        for decision in self.decisions:
            lines.append(
                f"| `{decision.actor}` | `{decision.decision}` | `{decision.decided_at}` | {decision.comment} |"
            )
        if self.blockers:
            lines.extend(["", "Approval record blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Approval runbook",
                "",
                "1. Confirm the actor is listed in the change request approvers.",
                "2. Confirm the change request has not expired.",
                "3. Rejecting decisions block approval until a new change request is opened.",
                "4. Promotion can continue only when quorum is met and `passed=true`.",
                "",
            ]
        )
        return "\n".join(lines)


class ApprovalRecordService:
    """Records approval decisions and evaluates approval quorum."""

    def record(
        self,
        *,
        output_dir: str | Path,
        change_request_path: str | Path,
        actor: str,
        decision: str,
        comment: str = "",
        quorum_required: int = 1,
    ) -> ApprovalRecordReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        change_request_file = Path(change_request_path)
        change_request = self._payload(change_request_file)
        decisions = (
            *self._existing_decisions(directory),
            self._decision(actor=actor, decision=decision, comment=comment),
        )
        allowed_approvers = tuple(str(item) for item in change_request.get("approvers", []) if item)
        approvals_count = sum(1 for item in decisions if item.decision == "approved")
        rejections_count = sum(1 for item in decisions if item.decision == "rejected")
        blockers = self._blockers(
            actor=actor,
            allowed_approvers=allowed_approvers,
            change_request=change_request,
            approvals_count=approvals_count,
            rejections_count=rejections_count,
            quorum_required=quorum_required,
        )
        report = ApprovalRecordReport(
            change_id=str(change_request.get("change_id", "unknown")),
            release=str(change_request.get("release", "unknown")),
            target_environment=str(change_request.get("target_environment", "unknown")),
            change_request_path=str(change_request_file),
            change_request_sha256=sha256_file(change_request_file),
            status="approved" if not blockers else "blocked",
            passed=not blockers,
            blockers=blockers,
            quorum_required=quorum_required,
            approvals_count=approvals_count,
            rejections_count=rejections_count,
            allowed_approvers=allowed_approvers,
            decisions=decisions,
            output_dir=str(directory),
        )
        (directory / "approval_record.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "approval_record.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    @staticmethod
    def _decision(*, actor: str, decision: str, comment: str) -> ApprovalDecision:
        return ApprovalDecision(
            actor=actor,
            decision=decision,
            comment=comment,
            decided_at=datetime.now(UTC).isoformat(),
        )

    def _existing_decisions(self, directory: Path) -> tuple[ApprovalDecision, ...]:
        path = directory / "approval_record.json"
        if not path.exists():
            return tuple()
        payload = self._payload(path)
        decisions = payload.get("decisions", [])
        if not isinstance(decisions, list):
            return tuple()
        return tuple(self._decision_from_payload(item) for item in decisions if isinstance(item, Mapping))

    @staticmethod
    def _decision_from_payload(payload: Mapping[str, Any]) -> ApprovalDecision:
        return ApprovalDecision(
            actor=str(payload.get("actor", "")),
            decision=str(payload.get("decision", "")),
            comment=str(payload.get("comment", "")),
            decided_at=str(payload.get("decided_at", "")),
        )

    def _blockers(
        self,
        *,
        actor: str,
        allowed_approvers: tuple[str, ...],
        change_request: Mapping[str, Any],
        approvals_count: int,
        rejections_count: int,
        quorum_required: int,
    ) -> tuple[str, ...]:
        blockers: list[str] = []
        if allowed_approvers and actor not in allowed_approvers:
            blockers.append("approval.actor_not_allowed")
        if self._is_expired(change_request.get("expires_at")):
            blockers.append("approval.change_request_expired")
        if not bool(change_request.get("passed", False)):
            blockers.append("approval.change_request_not_passed")
        if rejections_count > 0:
            blockers.append("approval.rejected")
        if approvals_count < quorum_required:
            blockers.append("approval.quorum_not_met")
        return tuple(blockers)

    @staticmethod
    def _is_expired(expires_at: object) -> bool:
        if not expires_at:
            return False
        try:
            parsed = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        except ValueError:
            return True
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed < datetime.now(UTC)

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}
