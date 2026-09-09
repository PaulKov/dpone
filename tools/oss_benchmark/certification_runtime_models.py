"""Shared models for executable benchmark certification."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    """Declarative certification scenario definition."""

    scenario_id: str
    category: str
    runner: str
    description: str
    required: bool = True


@dataclass(frozen=True, slots=True)
class ContractCheck:
    """One executable contract check result."""

    check_id: str
    status: str
    expected: Any
    actual: Any
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now() -> str:
    """Return a stable UTC ISO-8601 timestamp for evidence records."""

    return datetime.now(UTC).replace(microsecond=0).isoformat()


def relative_artifact_path(path: Path, *, root: Path) -> str:
    """Return a public, repository-relative artifact path."""

    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def check_result(check_id: str, passed: bool, *, expected: Any, actual: Any, message: str) -> dict[str, Any]:
    """Build a normalized contract check dictionary."""

    return ContractCheck(
        check_id=check_id,
        status="passed" if passed else "failed",
        expected=expected,
        actual=actual,
        message=message,
    ).to_dict()


def scenario_status(checks: list[dict[str, Any]]) -> str:
    """Return passed only when every contract check passed."""

    return "passed" if checks and all(check.get("status") == "passed" for check in checks) else "failed"
