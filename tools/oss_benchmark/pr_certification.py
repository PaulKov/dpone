"""PR summary rendering for executable certification evidence."""

from __future__ import annotations

from typing import Any


def render_executable_certification_pr_section(payload: dict[str, Any]) -> list[str]:
    """Render compact PR notes for executable certification."""

    certification = payload.get("runtime_certification_v2") or {}
    if not certification:
        return []
    gates = certification.get("gates") or {}
    summary = certification.get("summary") or {}
    lines = [
        "### Executable certification",
        "",
        f"- Status: **{gates.get('status', 'n/a')}** "
        f"(passed `{summary.get('passed', 0)}`, failed `{summary.get('failed', 0)}`, "
        f"stale `{summary.get('stale', 0)}`, unavailable `{summary.get('unavailable', 0)}`)",
    ]
    failed = gates.get("failed_checks") or []
    if failed:
        lines.append("- Release blockers:")
        for check in failed[:5]:
            lines.append(
                f"  - {check.get('label', 'scenario')}: `{check.get('actual', 'n/a')}` "
                f"expected `{check.get('expected', 'n/a')}`"
            )
    else:
        lines.append("- All required local executable scenarios are fresh and passing.")
    stale = [scenario for scenario in certification.get("scenarios") or [] if _freshness(scenario) == "stale"]
    if stale:
        lines.append("- Stale scenarios:")
        for scenario in stale[:5]:
            freshness = scenario.get("freshness") or {}
            lines.append(
                f"  - `{scenario.get('scenario_id', 'n/a')}` last updated "
                f"`{freshness.get('last_updated_at', 'n/a')}`, age `{freshness.get('stale_age_days', 'n/a')}` days"
            )
    lines.append("")
    return lines


def _freshness(scenario: dict[str, Any]) -> str:
    return str((scenario.get("freshness") or {}).get("status") or "fresh")
