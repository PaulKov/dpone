"""Self-service linting for nested normalization configs."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Literal

from dpone.contracts.nested_paths import normalize_path

Severity = Literal["error", "warn", "info"]


@dataclass(frozen=True, slots=True)
class NestedLintIssue:
    code: str
    severity: Severity
    message: str
    path: str | None = None
    recommendation: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "path": self.path,
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True, slots=True)
class NestedLintReport:
    root_table: str
    issues: tuple[NestedLintIssue, ...]

    @property
    def has_errors(self) -> bool:
        return any(issue.severity == "error" for issue in self.issues)

    def to_dict(self) -> dict[str, object]:
        return {
            "root_table": self.root_table,
            "has_errors": self.has_errors,
            "issues": [issue.to_dict() for issue in self.issues],
        }


class NestedNormalizationLintService:
    """Find risky nested normalization config before running ETL."""

    def lint_config(self, config: dict[str, Any], *, root_table: str) -> NestedLintReport:
        nested = _nested_config(config)
        issues: list[NestedLintIssue] = []
        issues.extend(_table_collision_issues(nested, root_table=root_table))
        issues.extend(_identity_policy_issues(nested))
        issues.extend(_guardrail_issues(nested))
        issues.extend(_raw_landing_issues(nested))
        return NestedLintReport(root_table=root_table, issues=tuple(issues))


def _nested_config(config: dict[str, Any]) -> dict[str, Any]:
    nested = config.get("nested")
    if isinstance(nested, dict):
        return {**config, **nested}
    return dict(config)


def _table_collision_issues(config: dict[str, Any], *, root_table: str) -> list[NestedLintIssue]:
    table_to_paths: dict[str, list[str]] = defaultdict(list)
    for item in config.get("split_paths", []) or []:
        if not isinstance(item, dict) or not item.get("path"):
            continue
        path = normalize_path(str(item["path"]))
        table = str(item.get("table") or _default_table(root_table, path))
        table_to_paths[table].append(path)
    issues: list[NestedLintIssue] = []
    for table, paths in table_to_paths.items():
        if len(set(paths)) > 1:
            issues.append(
                NestedLintIssue(
                    code="nested.table_collision",
                    severity="error",
                    message=f"Nested paths {sorted(set(paths))} all write to table `{table}`.",
                    recommendation="Give every independent nested entity a unique split_paths.table value.",
                )
            )
    return issues


def _identity_policy_issues(config: dict[str, Any]) -> list[NestedLintIssue]:
    issues: list[NestedLintIssue] = []
    for item in config.get("split_paths", []) or []:
        if not isinstance(item, dict) or not item.get("path"):
            continue
        path = normalize_path(str(item["path"]))
        if not item.get("unique_key"):
            issues.append(
                NestedLintIssue(
                    code="nested.child_unique_key_missing",
                    severity="warn",
                    message=f"Nested split path `{path}` has no stable child unique_key.",
                    path=path,
                    recommendation="Set split_paths[].unique_key, for example [order_id, sku], to make child row identity reorder-safe.",
                )
            )
        if str(item.get("delete_policy", "none")) == "none":
            issues.append(
                NestedLintIssue(
                    code="nested.child_delete_policy_missing",
                    severity="info",
                    message=f"Nested split path `{path}` has no child delete policy.",
                    path=path,
                    recommendation="Use delete_policy: replace_parent_children or snapshot_reconcile when physical child deletes must be reflected.",
                )
            )
    return issues


def _guardrail_issues(config: dict[str, Any]) -> list[NestedLintIssue]:
    guardrails = config.get("guardrails")
    if not isinstance(guardrails, dict) or not any(
        guardrails.get(name) for name in ("max_child_tables", "max_rows_per_root", "max_array_length")
    ):
        return [
            NestedLintIssue(
                code="nested.guardrails_missing",
                severity="warn",
                message="Nested normalization has no explosion guardrails configured.",
                recommendation="Set max_array_length, max_child_tables, or max_rows_per_root before production rollout.",
            )
        ]
    return []


def _raw_landing_issues(config: dict[str, Any]) -> list[NestedLintIssue]:
    raw = config.get("raw_landing")
    if raw is True:
        return [
            NestedLintIssue(
                code="nested.raw_landing_retention_unspecified",
                severity="info",
                message="Raw landing is enabled with default table/payload names.",
                recommendation="Document retention and access policy for raw payload tables.",
            )
        ]
    return []


def _default_table(root_table: str, path: str) -> str:
    safe = path.split(".")[-1].replace("-", "_") or "nested"
    return f"{root_table}__{safe}"
