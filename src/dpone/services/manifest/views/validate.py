from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.manifest.validation import Severity, ValidationIssue

from .common import ManifestViewMeta


@dataclass(frozen=True, slots=True)
class ManifestValidateView:
    meta: ManifestViewMeta
    issues: tuple[ValidationIssue, ...]
    profile_name: str | None = None

    def has_errors(self) -> bool:
        return any(issue.severity == Severity.ERROR for issue in self.issues)

    def exit_code(self, *, warn_only: bool) -> int:
        return 0 if warn_only or not self.has_errors() else 1

    def to_jsonable(self) -> dict[str, Any]:
        data = self.meta.to_jsonable()
        data.update(
            {
                "profile_name": self.profile_name,
                "issue_count": len(self.issues),
                "issues": [
                    {
                        "severity": issue.severity.value,
                        "code": issue.code,
                        "message": issue.message,
                        "manifest_path": str(issue.manifest_path),
                        "selector": issue.selector,
                    }
                    for issue in self.issues
                ],
            }
        )
        return data
