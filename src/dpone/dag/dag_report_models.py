from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml


def _severity_rank(sev: object) -> int:
    """Convert a severity-like value into an ordered rank."""

    s = str(sev).strip().upper()
    if s.endswith("ERROR"):
        return 2
    if s.endswith("WARNING"):
        return 1
    if s.endswith("INFO"):
        return 0
    return 0


@dataclass
class ReportEdge:
    upstream: str
    downstream: str
    upstream_ref: str | None = None
    downstream_ref: str | None = None
    upstream_group: str | None = None
    downstream_group: str | None = None
    reason_kinds: tuple[str, ...] = ()
    reasons: tuple[dict[str, Any], ...] = ()

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "upstream": self.upstream,
            "downstream": self.downstream,
            "upstream_ref": self.upstream_ref,
            "downstream_ref": self.downstream_ref,
            "upstream_group": self.upstream_group,
            "downstream_group": self.downstream_group,
            "reason_kinds": list(self.reason_kinds),
            "reasons": list(self.reasons) if self.reasons else None,
        }


@dataclass
class ReportAnomaly:
    code: str
    severity: str  # "ERROR"|"WARNING"|"INFO"
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class ReportLintIssue:
    tool: str  # "manifest.validate" | "registry.lint" | "dependencies"
    severity: str
    code: str
    message: str
    location: str
    selector: str | None = None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "tool": self.tool,
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "location": self.location,
            "selector": self.selector,
        }


@dataclass
class DagReport:
    root: str
    base_path: str
    generated_at: str

    task_count: int
    edge_count: int
    group_count: int

    roots: tuple[str, ...] = ()
    sinks: tuple[str, ...] = ()
    orphans: tuple[str, ...] = ()

    edges: tuple[ReportEdge, ...] = ()
    anomalies: tuple[ReportAnomaly, ...] = ()
    lint: tuple[ReportLintIssue, ...] = ()

    warnings: tuple[str, ...] = ()

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "root": self.root,
            "base_path": self.base_path,
            "generated_at": self.generated_at,
            "summary": {
                "task_count": self.task_count,
                "edge_count": self.edge_count,
                "group_count": self.group_count,
                "roots": list(self.roots),
                "sinks": list(self.sinks),
                "orphans": list(self.orphans),
            },
            "edges": [e.to_jsonable() for e in self.edges],
            "anomalies": [a.to_jsonable() for a in self.anomalies],
            "lint": [i.to_jsonable() for i in self.lint],
            "warnings": list(self.warnings),
        }

    def lint_issue_count(self, *, min_severity: str = "ERROR") -> int:
        """Number of lint issues with severity >= min_severity."""
        thr = _severity_rank(min_severity)
        return sum(1 for i in self.lint if _severity_rank(i.severity) >= thr)

    def anomaly_issue_count(self, *, min_severity: str = "ERROR") -> int:
        """Number of anomalies with severity >= min_severity."""
        thr = _severity_rank(min_severity)
        return sum(1 for a in self.anomalies if _severity_rank(a.severity) >= thr)

    def lint_error_count(self) -> int:
        """Number of lint issues with ERROR severity."""
        return self.lint_issue_count(min_severity="ERROR")

    def anomaly_error_count(self) -> int:
        """Number of anomalies with ERROR severity."""
        return self.anomaly_issue_count(min_severity="ERROR")

    def has_lint_errors(self) -> bool:
        return self.lint_error_count() > 0

    def has_anomaly_errors(self) -> bool:
        return self.anomaly_error_count() > 0

    def to_markdown(self, *, max_edges: int = 200) -> str:
        lines: list[str] = []
        lines.append("# dpone DAG report")
        lines.append("")
        lines.append(f"- Root: `{self.root}`")
        lines.append(f"- Base path: `{self.base_path}`")
        lines.append(f"- Generated at: `{self.generated_at}`")
        lines.append("")

        lines.append("## Summary")
        lines.append("")
        lines.append(f"- Tasks: **{self.task_count}**")
        lines.append(f"- Edges: **{self.edge_count}**")
        lines.append(f"- Task groups: **{self.group_count}**")
        lines.append(f"- Roots (no incoming): **{len(self.roots)}**")
        lines.append(f"- Sinks (no outgoing): **{len(self.sinks)}**")
        lines.append(f"- Orphans (isolated): **{len(self.orphans)}**")

        if self.roots:
            lines.append("")
            lines.append("### Roots")
            for n in list(self.roots)[:50]:
                lines.append(f"- `{n}`")
            if len(self.roots) > 50:
                lines.append(f"- ... truncated ({len(self.roots)} total)")

        if self.orphans:
            lines.append("")
            lines.append("### Orphans")
            for n in list(self.orphans)[:50]:
                lines.append(f"- `{n}`")
            if len(self.orphans) > 50:
                lines.append(f"- ... truncated ({len(self.orphans)} total)")

        if self.anomalies:
            lines.append("")
            lines.append("## Anomalies")
            lines.append("")
            for a in self.anomalies:
                lines.append(f"- **{a.severity}** `{a.code}`: {a.message}")
                if a.details:
                    # compact, diff-friendly YAML
                    y = yaml.safe_dump(a.details, sort_keys=True, allow_unicode=True).rstrip()
                    lines.append("  ```yaml")
                    for ln in y.splitlines():
                        lines.append("  " + ln)
                    lines.append("  ```")

        if self.lint:
            lines.append("")
            lines.append("## Lint")
            lines.append("")
            # group by tool
            by_tool: dict[str, list[ReportLintIssue]] = {}
            for i in self.lint:
                by_tool.setdefault(i.tool, []).append(i)
            for tool, issues in sorted(by_tool.items()):
                lines.append(f"### {tool}")
                lines.append("")
                # counts
                sev_counts: dict[str, int] = {}
                for it in issues:
                    sev_counts[it.severity] = sev_counts.get(it.severity, 0) + 1
                counts_str = ", ".join([f"{k}={v}" for k, v in sorted(sev_counts.items())])
                lines.append(f"- Issues: {len(issues)} ({counts_str})")
                lines.append("")
                # table
                lines.append("| severity | code | location | selector | message |")
                lines.append("|---|---|---|---|---|")
                for it in issues[:200]:
                    sel = it.selector or "-"
                    msg = it.message.replace("\n", " ")
                    lines.append(f"| {it.severity} | `{it.code}` | `{it.location}` | `{sel}` | {msg} |")
                if len(issues) > 200:
                    lines.append(f"\n_Truncated: showing first 200 of {len(issues)} issues._")
                lines.append("")

        if self.edges:
            lines.append("")
            lines.append("## Edges")
            lines.append("")
            stored_total = len(self.edges)
            actual_total = int(self.edge_count)

            if stored_total < actual_total:
                lines.append(
                    f"_Note: edges stored in this report were truncated to {stored_total} of {actual_total} (see Warnings / use --max-edges)._\n"
                )

            shown = min(max_edges, stored_total) if max_edges > 0 else stored_total
            if shown < stored_total:
                lines.append(
                    f"_Showing first {shown} edges of {stored_total} stored in report (use --md-max-edges to control Markdown output)._\n"
                )
            lines.append("| upstream | downstream | reason_kinds |")
            lines.append("|---|---|---|")
            for e in self.edges[:shown]:
                rk = ",".join(e.reason_kinds) if e.reason_kinds else "-"
                lines.append(f"| `{e.upstream}` | `{e.downstream}` | `{rk}` |")

        if self.warnings:
            lines.append("")
            lines.append("## Warnings")
            for w in self.warnings:
                lines.append(f"- {w}")

        lines.append("")
        return "\n".join(lines)
