"""Rendering helpers for profile advice CLI output."""

from __future__ import annotations

from typing import Any


def render_profile_text(payload: dict[str, Any]) -> str:
    lines = [
        "dpone profile advise",
        f"- selected_profile: {payload['selected_profile']}",
        f"- route: {payload['selected_route']}",
        f"- execution_mode: {payload['execution_mode']}",
        (f"- expected_rps: {payload['expected_rps_range']['min']}..{payload['expected_rps_range']['max']}"),
        f"- confidence: {payload['confidence']}",
    ]
    lines.extend(f"- warning: {code}" for code in payload.get("warnings", []))
    for item in payload.get("recommendations", []):
        lines.append(f"- recommendation: {item['code']} -> {item['patch_path']}={item['value']}")
    return "\n".join(lines) + "\n"


def render_profile_md(payload: dict[str, Any]) -> str:
    lines = [
        "# dpone profile advise",
        "",
        f"- selected profile: `{payload['selected_profile']}`",
        f"- selected route: `{payload['selected_route']}`",
        f"- execution mode: `{payload['execution_mode']}`",
        (f"- expected RPS: `{payload['expected_rps_range']['min']}..{payload['expected_rps_range']['max']}`"),
        f"- confidence: `{payload['confidence']}`",
        "",
        "## Recommendations",
    ]
    for item in payload.get("recommendations", []):
        lines.append(f"- `{item['code']}` ({item['severity']}): {item['message']}")
    if payload.get("warnings"):
        lines.extend(["", "## Warnings"])
        lines.extend(f"- `{code}`" for code in payload["warnings"])
    return "\n".join(lines) + "\n"


__all__ = ["render_profile_md", "render_profile_text"]
