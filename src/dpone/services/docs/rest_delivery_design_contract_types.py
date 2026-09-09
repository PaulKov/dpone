"""Shared immutable value objects for REST delivery design validation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True, order=True)
class DesignContractIssue:
    """One deterministic design-contract validation failure."""

    code: str
    path: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "path": self.path, "message": self.message}


@dataclass(frozen=True, slots=True)
class DesignContractReport:
    """Machine-readable validator result."""

    issues: tuple[DesignContractIssue, ...]

    @property
    def ok(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": "docs.rest_delivery_design_contract",
            "passed": self.ok,
            "issue_count": len(self.issues),
            "issues": [issue.to_dict() for issue in self.issues],
        }


def as_mapping(value: object) -> Mapping[str, Any]:
    """Return a typed mapping after structural validation has succeeded."""

    return value if isinstance(value, Mapping) else {}


def as_sequence(value: object) -> list[Any]:
    """Return a non-string sequence after structural validation has succeeded."""

    return list(value) if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else []


def contract_issue(code: str, path: str, message: str) -> DesignContractIssue:
    """Build one concise semantic issue."""

    return DesignContractIssue(code, path, message)


def build_contract_report(*issues: DesignContractIssue) -> DesignContractReport:
    """Deduplicate and deterministically order validation issues."""

    return DesignContractReport(tuple(sorted(set(issues))))


__all__ = [
    "DesignContractIssue",
    "DesignContractReport",
    "as_mapping",
    "as_sequence",
    "build_contract_report",
    "contract_issue",
]
