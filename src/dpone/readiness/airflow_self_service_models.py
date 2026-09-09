"""Shared result models for beginner Airflow self-service commands."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from dpone.readiness.error_contract import dpone_error, manual_fix


@dataclass(frozen=True)
class Change:
    action: str
    path: str
    message: str = ""
    diff: str = ""


@dataclass(frozen=True)
class SelfServiceResult:
    passed: bool
    changes: tuple[Change, ...] = ()
    errors: tuple[dict[str, Any], ...] = ()
    details: dict[str, Any] | None = None
    exit_code: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "passed": self.passed,
            "changes": [asdict(change) for change in self.changes],
            "errors": list(self.errors),
        }
        if self.details:
            payload.update(self.details)
        return payload


__all__ = ["Change", "SelfServiceResult", "dpone_error", "manual_fix"]
