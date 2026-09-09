from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.manifest.registry_lint import RegistryLintIssue
from dpone.manifest.validation import Severity

from .common import ManifestViewMeta


@dataclass(frozen=True, slots=True)
class ManifestRegistryLintView:
    meta: ManifestViewMeta
    issues: tuple[RegistryLintIssue, ...]

    def has_errors(self) -> bool:
        return any(issue.severity == Severity.ERROR for issue in self.issues)

    def exit_code(self, *, warn_only: bool) -> int:
        return 0 if warn_only or not self.has_errors() else 1

    def to_jsonable(self) -> dict[str, Any]:
        data = self.meta.to_jsonable()
        data.update(
            {
                "issue_count": len(self.issues),
                "issues": [
                    {
                        "severity": issue.severity.value,
                        "code": issue.code,
                        "message": issue.message,
                        "manifest_path": str(issue.manifest_path),
                        "src_system": issue.src_system,
                        "src_database": issue.src_database,
                    }
                    for issue in self.issues
                ],
            }
        )
        return data
