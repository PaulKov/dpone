"""Stable release-candidate finalizer contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol

SCHEMA_VERSION = "dpone.release_rc_finalizer.v1"


class ReleaseEvidenceArtifact(Protocol):
    """Minimal artifact contract required by release-candidate reports."""

    @property
    def name(self) -> str: ...

    @property
    def path(self) -> str: ...

    @property
    def required(self) -> bool: ...

    @property
    def exists(self) -> bool: ...

    @property
    def passed(self) -> bool: ...

    @property
    def sha256(self) -> str: ...

    @property
    def summary(self) -> str: ...

    def to_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class ReleaseRcCheckRun:
    """One status check attached to a release-candidate PR."""

    name: str
    status: str
    conclusion: str
    details_url: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReleaseRcPullRequest:
    """One pull request in the expected stacked merge train."""

    number: int
    title: str
    base_ref: str
    head_ref: str
    state: str
    merge_state: str
    is_draft: bool
    checks: tuple[ReleaseRcCheckRun, ...]
    url: str = ""

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["checks"] = [item.to_dict() for item in self.checks]
        return payload


@dataclass(frozen=True, slots=True)
class ReleaseRcMergeTrain:
    """Ordered base-to-head PR train for one release candidate."""

    base_branch: str
    head_branch: str
    pull_requests: tuple[ReleaseRcPullRequest, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "base_branch": self.base_branch,
            "head_branch": self.head_branch,
            "pull_requests": [item.to_dict() for item in self.pull_requests],
        }


@dataclass(frozen=True, slots=True)
class ReleaseRcFinalizerCheck:
    """One release-candidate finalizer policy check."""

    name: str
    domain: str
    passed: bool
    required: bool
    summary: str
    blocker: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReleaseRcFinalizerDecision:
    """Pure go/no-go decision for a release candidate."""

    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    checks: tuple[ReleaseRcFinalizerCheck, ...]


@dataclass(frozen=True, slots=True)
class ReleaseRcFinalizerReport:
    """Stable JSON/Markdown receipt for final RC integration review."""

    release: str
    previous_release: str
    package_version: str
    mode: str
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    merge_train: ReleaseRcMergeTrain
    artifacts: tuple[ReleaseEvidenceArtifact, ...]
    checks: tuple[ReleaseRcFinalizerCheck, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "release": self.release,
            "previous_release": self.previous_release,
            "package_version": self.package_version,
            "mode": self.mode,
            "passed": self.passed,
            "level": self.level,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "merge_train": self.merge_train.to_dict(),
            "artifacts": [item.to_dict() for item in self.artifacts],
            "checks": [item.to_dict() for item in self.checks],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Release RC finalizer",
            "",
            f"- Release: `{self.release}`",
            f"- Previous release: `{self.previous_release}`",
            f"- Package version: `{self.package_version}`",
            f"- Mode: `{self.mode}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            "",
            "## Merge train",
            "",
            f"- Base branch: `{self.merge_train.base_branch}`",
            f"- Head branch: `{self.merge_train.head_branch}`",
            "",
            "| PR | base | head | state | merge state | checks |",
            "|---:|---|---|---|---|---:|",
        ]
        for item in self.merge_train.pull_requests:
            lines.append(
                f"| `#{item.number}` | `{item.base_ref}` | `{item.head_ref}` | "
                f"`{item.state}` | `{item.merge_state}` | `{len(item.checks)}` |"
            )
        lines.extend(
            [
                "",
                "## Checks",
                "",
                "| check | domain | required | status | summary |",
                "|---|---|---:|---|---|",
            ]
        )
        for check in self.checks:
            status = "pass" if check.passed else "fail"
            lines.append(f"| `{check.name}` | `{check.domain}` | `{check.required}` | {status} | {check.summary} |")
        lines.extend(
            [
                "",
                "## Evidence artifacts",
                "",
                "| artifact | required | exists | passed | sha256 | summary | path |",
                "|---|---:|---:|---:|---|---|---|",
            ]
        )
        for artifact in self.artifacts:
            lines.append(
                f"| `{artifact.name}` | `{artifact.required}` | `{artifact.exists}` | `{artifact.passed}` | "
                f"`{artifact.sha256}` | {artifact.summary} | `{artifact.path}` |"
            )
        lines.extend(["", "## Blockers", ""])
        if self.blockers:
            lines.extend(f"- `{item}`" for item in self.blockers)
        else:
            lines.append("- none")
        lines.extend(["", "## Runbook", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.extend(
                [
                    "- Merge the train in the recorded order or keep this receipt attached to RC review.",
                    "- Run the final post-merge mode before creating the release tag.",
                    "- Keep `release_rc_finalizer.json` immutable for the release candidate.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


__all__ = [
    "SCHEMA_VERSION",
    "ReleaseRcCheckRun",
    "ReleaseEvidenceArtifact",
    "ReleaseRcFinalizerCheck",
    "ReleaseRcFinalizerDecision",
    "ReleaseRcFinalizerReport",
    "ReleaseRcMergeTrain",
    "ReleaseRcPullRequest",
]
