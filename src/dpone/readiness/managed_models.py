"""Serializable managed-UX result models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dpone._compat import StrEnum


class ManagedFormat(StrEnum):
    TEXT = "text"
    JSON = "json"
    MD = "md"
    HTML = "html"


class QualityOutcomeStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    UNVERIFIED = "unverified"


@dataclass(frozen=True)
class InitBundleResult:
    manifest_path: Path
    env_example_path: Path
    smoke_command: str
    docs_url: str = "docs/MANAGED_UX.md"

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_path": str(self.manifest_path),
            "env_example_path": str(self.env_example_path),
            "smoke_command": self.smoke_command,
            "docs_url": self.docs_url,
        }


@dataclass(frozen=True)
class QualityCheckOutcome:
    check_type: str
    status: str
    mode: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class QualityRunResult:
    checks: tuple[QualityCheckOutcome, ...]

    @property
    def failed(self) -> int:
        return sum(1 for item in self.checks if item.status == QualityOutcomeStatus.FAILED and item.mode != "warn")

    @property
    def warned(self) -> int:
        return sum(1 for item in self.checks if item.status == QualityOutcomeStatus.FAILED and item.mode == "warn")

    @property
    def skipped(self) -> int:
        return self._count(QualityOutcomeStatus.SKIPPED)

    @property
    def unverified(self) -> int:
        return self._count(QualityOutcomeStatus.UNVERIFIED)

    @property
    def executed(self) -> int:
        return len(self.checks) - self.skipped - self.unverified

    @property
    def status(self) -> str:
        if self.failed:
            return QualityOutcomeStatus.FAILED
        if self.unverified or not self.checks:
            return QualityOutcomeStatus.UNVERIFIED
        if self.executed == 0:
            return QualityOutcomeStatus.SKIPPED
        if self.skipped:
            return QualityOutcomeStatus.UNVERIFIED
        return QualityOutcomeStatus.PASSED

    @property
    def passed(self) -> bool:
        return self.executed > 0 and self.failed == 0 and self.skipped == 0 and self.unverified == 0

    def _count(self, status: QualityOutcomeStatus) -> int:
        return sum(1 for item in self.checks if item.status == status)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "status": self.status,
            "configured": len(self.checks),
            "executed": self.executed,
            "failed": self.failed,
            "warned": self.warned,
            "skipped": self.skipped,
            "unverified": self.unverified,
            "checks": [item.to_dict() for item in self.checks],
        }


@dataclass(frozen=True)
class RunArtifactPaths:
    run_id: str
    json_path: Path
    markdown_path: Path
    html_path: Path

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "json_path": str(self.json_path),
            "markdown_path": str(self.markdown_path),
            "html_path": str(self.html_path),
        }


@dataclass(frozen=True)
class PerformanceRecommendation:
    code: str
    severity: str
    expected_impact: str
    message: str
    action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = [
    "InitBundleResult",
    "ManagedFormat",
    "PerformanceRecommendation",
    "QualityCheckOutcome",
    "QualityOutcomeStatus",
    "QualityRunResult",
    "RunArtifactPaths",
]
