"""Fail-closed local Docker evidence campaign for semantic refresh V2.

The report intentionally distinguishes local execution evidence from production
route certification.  Command lines, environment variables and command output
are never persisted because they can contain infrastructure or credential
material.  Only a redacted output digest and bounded status are recorded.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from dpone.security_redaction import redact_absolute_paths, redact_text

CampaignStatus = Literal["PASS", "FAIL", "UNVERIFIED"]


@dataclass(frozen=True, slots=True)
class LocalCampaignCheck:
    """One closed, source-controlled check in the local evidence campaign."""

    check_id: str
    argv: tuple[str, ...]
    environment: Mapping[str, str]

    def __post_init__(self) -> None:
        if not self.check_id or not self.argv:
            raise ValueError("local campaign check requires a non-empty id and command")


@dataclass(frozen=True, slots=True)
class LocalCommandObservation:
    """Non-persisted command observation returned by an injected runner."""

    return_code: int
    stdout: str
    stderr: str
    duration_ms: int
    skipped: bool = False


class LocalCampaignCommandRunner(Protocol):
    """Execute one source-controlled local command without shell expansion."""

    def __call__(
        self,
        *,
        argv: Sequence[str],
        environment: Mapping[str, str],
        cwd: Path,
    ) -> LocalCommandObservation: ...


@dataclass(frozen=True, slots=True)
class LocalCampaignCheckResult:
    """Secret-free result for one local campaign check."""

    check_id: str
    status: CampaignStatus
    return_code: int
    duration_ms: int
    redacted_output_sha256: str

    def to_dict(self) -> dict[str, object]:
        return {
            "check_id": self.check_id,
            "status": self.status,
            "return_code": self.return_code,
            "duration_ms": self.duration_ms,
            "redacted_output_sha256": self.redacted_output_sha256,
        }


@dataclass(frozen=True, slots=True)
class LocalCampaignReport:
    """Machine-readable local evidence; never a production certification."""

    campaign_id: str
    created_at: str
    git_head_sha: str
    source_snapshot_sha256: str
    worktree_dirty: bool
    complete_campaign: bool
    status: CampaignStatus
    results: tuple[LocalCampaignCheckResult, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "dpone.semantic-refresh-local-docker-evidence.v1",
            "campaign_id": self.campaign_id,
            "created_at": self.created_at,
            "environment_class": "LOCAL_DOCKER",
            "production_certification": "UNVERIFIED",
            "production_certification_reason": (
                "local Docker evidence is not an approved production-route certification"
            ),
            "git_head_sha": self.git_head_sha,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "worktree_dirty": self.worktree_dirty,
            "complete_campaign": self.complete_campaign,
            "status": self.status,
            "results": [result.to_dict() for result in self.results],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


class SemanticRefreshLocalCampaign:
    """Run a closed local campaign and publish a secret-free evidence report."""

    def __init__(
        self,
        *,
        command_runner: LocalCampaignCommandRunner,
        now: Callable[[], str],
    ) -> None:
        self._command_runner = command_runner
        self._now = now

    def run(
        self,
        *,
        repo_root: Path,
        output_path: Path,
        campaign_id: str,
        git_head_sha: str,
        source_snapshot_sha256: str,
        worktree_dirty: bool,
        checks: Sequence[LocalCampaignCheck],
        expected_check_ids: frozenset[str],
        report_writer: Callable[[Path, str], None] | None = None,
    ) -> LocalCampaignReport:
        selected_ids = tuple(check.check_id for check in checks)
        if len(selected_ids) != len(set(selected_ids)):
            raise ValueError("local campaign check ids must be unique")
        if not set(selected_ids).issubset(expected_check_ids):
            raise ValueError("local campaign contains an unknown check id")

        results = tuple(self._run_check(repo_root, check) for check in checks)
        complete = set(selected_ids) == expected_check_ids
        status = _aggregate_status(
            results,
            complete=complete,
            worktree_dirty=worktree_dirty,
        )
        report = LocalCampaignReport(
            campaign_id=campaign_id,
            created_at=self._now(),
            git_head_sha=git_head_sha,
            source_snapshot_sha256=source_snapshot_sha256,
            worktree_dirty=worktree_dirty,
            complete_campaign=complete,
            status=status,
            results=results,
        )
        if report_writer is None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(report.to_json(), encoding="utf-8")
        else:
            report_writer(output_path, report.to_json())
        return report

    def _run_check(self, repo_root: Path, check: LocalCampaignCheck) -> LocalCampaignCheckResult:
        observation = self._command_runner(
            argv=check.argv,
            environment=check.environment,
            cwd=repo_root,
        )
        redacted = redact_absolute_paths(redact_text(f"{observation.stdout}\n{observation.stderr}"))
        if observation.return_code != 0:
            status: CampaignStatus = "FAIL"
        elif observation.skipped:
            status = "UNVERIFIED"
        else:
            status = "PASS"
        return LocalCampaignCheckResult(
            check_id=check.check_id,
            status=status,
            return_code=observation.return_code,
            duration_ms=observation.duration_ms,
            redacted_output_sha256="sha256:" + hashlib.sha256(redacted.encode()).hexdigest(),
        )


def _aggregate_status(
    results: Sequence[LocalCampaignCheckResult],
    *,
    complete: bool,
    worktree_dirty: bool,
) -> CampaignStatus:
    if any(result.status == "FAIL" for result in results):
        return "FAIL"
    if worktree_dirty or not complete or any(result.status == "UNVERIFIED" for result in results):
        return "UNVERIFIED"
    return "PASS"


__all__ = [
    "LocalCampaignCheck",
    "LocalCampaignCheckResult",
    "LocalCampaignCommandRunner",
    "LocalCampaignReport",
    "LocalCommandObservation",
    "SemanticRefreshLocalCampaign",
]
