"""Portable projection of one dbt compile report into review Markdown."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import quote


def render_dbt_ci_report(
    report: Mapping[str, object],
    *,
    airflow_base_url: str | None = None,
) -> str:
    """Render frozen compiler decisions without re-evaluating domain policy."""

    passed = report.get("passed") is True
    lines = [
        "# dpone dbt self-service preview",
        "",
        f"Status: **{'PASS' if passed else 'BLOCKED'}**",
        "",
    ]
    models = report.get("models")
    if isinstance(models, list) and models:
        lines.extend(
            [
                "| Model | Source | Target | Strategy | Support | Certification |",
                "|---|---|---|---|---|---|",
            ]
        )
        for raw_model in models:
            if not isinstance(raw_model, Mapping):
                continue
            intent = _mapping(raw_model.get("intent"))
            target = _mapping(intent.get("target"))
            capability = _mapping(raw_model.get("route_capability"))
            strategy = _mapping(raw_model.get("resolved_strategy"))
            lines.append(
                "| "
                + " | ".join(
                    _cell(value)
                    for value in (
                        raw_model.get("model"),
                        _relation(raw_model.get("source_relation")),
                        _relation(target),
                        strategy.get("mode"),
                        capability.get("support"),
                        capability.get("evidence_status"),
                    )
                )
                + " |"
            )
        lines.append("")
    workflows = report.get("workflows")
    if isinstance(workflows, list) and workflows:
        lines.extend(["## Planned DAGs", ""])
        for raw_workflow in workflows:
            if not isinstance(raw_workflow, Mapping):
                continue
            dag_id = str(raw_workflow.get("dag_id") or "")
            label = f"`{dag_id}`"
            if airflow_base_url and dag_id:
                url = (
                    airflow_base_url.rstrip("/")
                    + "/dags/"
                    + quote(
                        dag_id,
                        safe="",
                    )
                )
                label = f"[`{dag_id}`]({url})"
            lines.append(f"- {label}")
        lines.append("")
    issues = report.get("blockers") if not passed else report.get("warnings")
    if isinstance(issues, list) and issues:
        lines.extend(
            [
                "## Blockers" if not passed else "## Warnings",
                "",
            ]
        )
        for raw_issue in issues:
            issue = _mapping(raw_issue)
            code = _cell(issue.get("code"))
            message = _cell(issue.get("message"))
            lines.append(f"- `{code}`: {message}")
            remediation = issue.get("remediation")
            if remediation:
                lines.append(f"  Next: {_cell(remediation)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _relation(value: object) -> str:
    relation = _mapping(value)
    return ".".join(str(relation[key]) for key in ("database", "schema", "name", "table") if relation.get(key))


def _cell(value: object) -> str:
    return str(value or "-").replace("|", "\\|").replace("\n", " ")


__all__ = ["render_dbt_ci_report"]
