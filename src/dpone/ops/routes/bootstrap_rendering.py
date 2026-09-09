"""Shared rendering helpers for route onboarding reports."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

OnboardingStatus = Literal["ready", "warning", "blocked"]


class _Check(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def kind(self) -> str: ...

    @property
    def required(self) -> bool: ...

    @property
    def status(self) -> OnboardingStatus: ...

    @property
    def summary(self) -> str: ...


@dataclass(frozen=True, slots=True)
class RenderedCheck:
    name: str
    kind: str
    required: bool
    status: OnboardingStatus
    summary: str


def json_text(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def write_report(output_dir: str, json_path: str, markdown_path: str, json_payload: str, markdown: str) -> None:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(json_path).write_text(json_payload, encoding="utf-8")
    Path(markdown_path).write_text(markdown, encoding="utf-8")


def report_markdown(
    *,
    title: str,
    identity: str,
    status: OnboardingStatus,
    passed: bool,
    score: float,
    checks: Sequence[_Check],
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
    next_actions: tuple[str, ...],
) -> str:
    lines = [
        f"# {title}",
        "",
        f"- Identity: `{identity}`",
        f"- Passed: `{passed}`",
        f"- Status: `{status}`",
        f"- Score: `{score}`",
        "",
        "## Checks",
        "",
        "| check | kind | required | status | summary |",
        "|---|---|---|---|---|",
    ]
    for check in checks:
        lines.append(f"| `{check.name}` | `{check.kind}` | `{check.required}` | `{check.status}` | {check.summary} |")
    lines.extend(["", "## Blockers", ""])
    lines.extend(f"- `{item}`" for item in blockers) if blockers else lines.append("- none")
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- `{item}`" for item in warnings) if warnings else lines.append("- none")
    lines.extend(["", "## Runbook", ""])
    lines.extend(f"- {item}" for item in next_actions) if next_actions else lines.append(
        "- Continue with route readiness."
    )
    lines.append("")
    return "\n".join(lines)


def source_discovery_markdown(report: Any) -> str:
    checks = tuple(
        RenderedCheck(
            name=f"table.{table.qualified_name}",
            kind="schema",
            required=True,
            status="ready",
            summary=f"columns={len(table.columns)}, rows={table.row_count}",
        )
        for table in report.tables
    )
    return report_markdown(
        title="Source discovery",
        identity=report.dataset or report.source,
        status=report.status,
        passed=report.passed,
        score=report.score,
        checks=checks,
        blockers=report.blockers,
        warnings=report.warnings,
        next_actions=report.next_actions,
    )


def route_bootstrap_markdown(report: Any) -> str:
    checks = (
        RenderedCheck(
            name="manifest.draft",
            kind="manifest",
            required=True,
            status=report.status,
            summary=report.manifest_path,
        ),
    )
    return report_markdown(
        title="Route bootstrap",
        identity=report.route.case_id,
        status=report.status,
        passed=report.passed,
        score=report.score,
        checks=checks,
        blockers=report.blockers,
        warnings=report.warnings,
        next_actions=(*report.next_actions, *report.next_commands),
    )


def route_doctor_markdown(report: Any) -> str:
    checks = tuple(
        RenderedCheck(
            name=artifact.name,
            kind="artifact",
            required=artifact.required,
            status=artifact.status,
            summary=artifact.summary,
        )
        for artifact in report.artifacts
    )
    return report_markdown(
        title="Route doctor",
        identity=report.route.case_id,
        status=report.status,
        passed=report.passed,
        score=report.score,
        checks=checks,
        blockers=report.blockers,
        warnings=report.warnings,
        next_actions=report.next_actions,
    )
