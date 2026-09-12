"""Deterministic text projections for the PostgreSQL to MSSQL R1 plan."""

from __future__ import annotations


def render_postgres_mssql_correctness_text(decision: dict) -> list[str]:
    """Render the compact terminal projection."""

    if not decision:
        return []
    if decision.get("selected") is not True:
        return ["- postgres_mssql_correctness: compatibility"]
    return [
        (
            "- postgres_mssql_correctness: "
            f"{decision.get('capability_id')} "
            f"certification={decision.get('certification_status')} "
            f"activation={decision.get('activation_status')}"
        ),
        f"- postgres_mssql_correctness_source_mode: {decision.get('source_mode')}",
        f"- postgres_mssql_correctness_decision: {decision.get('decision_sha256')}",
        f"- postgres_mssql_correctness_blockers: {','.join(decision.get('blockers') or [])}",
    ]


def render_postgres_mssql_correctness_md(decision: dict) -> list[str]:
    """Render the Markdown projection from the same JSON decision."""

    if decision.get("selected") is not True:
        return ["- execution_profile: `compatibility`", "- selected: `False`"]
    return [
        f"- profile_id: `{decision.get('profile_id')}`",
        f"- capability_id: `{decision.get('capability_id')}`",
        f"- source_mode: `{decision.get('source_mode')}`",
        f"- target_topology: `{decision.get('target_topology')}`",
        f"- authority_contract: `{decision.get('authority_contract')}`",
        f"- implementation_status: `{decision.get('implementation_status')}`",
        f"- certification_status: `{decision.get('certification_status')}`",
        f"- activation_status: `{decision.get('activation_status')}`",
        f"- decision_sha256: `{decision.get('decision_sha256')}`",
        f"- blockers: `{', '.join(decision.get('blockers') or [])}`",
    ]


__all__ = ["render_postgres_mssql_correctness_md", "render_postgres_mssql_correctness_text"]
