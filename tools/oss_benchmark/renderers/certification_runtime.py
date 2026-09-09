"""Markdown rendering for executable runtime certification evidence."""

from __future__ import annotations

from typing import Any


def render_executable_certification_section(payload: dict[str, Any]) -> str:
    """Render v4 executable certification sections."""

    certification = payload.get("runtime_certification_v2") or {}
    scenarios = list(certification.get("scenarios") or [])
    if not scenarios:
        return ""
    lines: list[str] = []
    lines.extend(_overview(certification))
    lines.extend(_golden_dataset(certification))
    lines.extend(_run_ledger(certification))
    lines.extend(_gates(certification))
    return "\n".join(lines)


def _overview(certification: dict[str, Any]) -> list[str]:
    summary = certification.get("summary") or {}
    lines = [
        '<a id="executable-certification"></a>',
        "",
        "## Executable Certification",
        "",
        "Executable certification runs deterministic, credential-free dpone release scenarios and publishes the ledger behind every runtime claim.",
        "",
        f"Passed `{summary.get('passed', 0)}`, failed `{summary.get('failed', 0)}`, stale `{summary.get('stale', 0)}`, unavailable `{summary.get('unavailable', 0)}`.",
        "",
        "| Scenario | Category | Runner | Status | Freshness | Duration | Row counts |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for scenario in certification.get("scenarios") or []:
        freshness = scenario.get("freshness") or {}
        lines.append(
            "| "
            f"{scenario.get('scenario_id', 'n/a')} | "
            f"`{scenario.get('category', 'n/a')}` | "
            f"`{scenario.get('runner', 'n/a')}` | "
            f"`{scenario.get('status', 'n/a')}` | "
            f"`{freshness.get('status', 'n/a')}` | "
            f"{scenario.get('duration_ms', 0)} ms | "
            f"{_row_counts(scenario.get('row_counts') or {})} |"
        )
    lines.append("")
    return lines


def _golden_dataset(certification: dict[str, Any]) -> list[str]:
    policy = certification.get("golden_datasets") or {}
    return [
        '<a id="golden-dataset-evidence"></a>',
        "",
        "## Golden Dataset Evidence",
        "",
        str(policy.get("policy") or "Golden dataset policy is unavailable."),
        "",
        f"Hash policy: `{policy.get('hash_policy', 'n/a')}`.",
        "",
    ]


def _run_ledger(certification: dict[str, Any]) -> list[str]:
    lines = [
        '<a id="run-ledger"></a>',
        "",
        "## Run Ledger",
        "",
        "Machine-readable run artifacts: `docs/benchmarks/data/runtime-certification/latest/run-ledger.json` and `docs/benchmarks/data/runtime-certification/latest/contract-checks.json`.",
        "",
        "| Scenario | Status | Input hash | Output hash | Artifacts |",
        "|---|---:|---|---|---|",
    ]
    for record in certification.get("run_ledger") or []:
        artifacts = "<br>".join(f"`{path}`" for path in record.get("artifact_paths") or []) or "n/a"
        lines.append(
            "| "
            f"{record.get('scenario_id', 'n/a')} | "
            f"`{record.get('status', 'n/a')}` | "
            f"`{_short_hash(record.get('input_hash'))}` | "
            f"`{_short_hash(record.get('output_hash'))}` | "
            f"{artifacts} |"
        )
    lines.append("")
    return lines


def _gates(certification: dict[str, Any]) -> list[str]:
    gates = certification.get("gates") or {}
    lines = [
        '<a id="certification-gates"></a>',
        "",
        "## Certification Gates",
        "",
        f"Status: **{gates.get('status', 'n/a')}**.",
        "",
        "| Gate | Status | Actual | Expected |",
        "|---|---:|---:|---:|",
    ]
    for check in gates.get("checks") or []:
        lines.append(
            "| "
            f"{check.get('label', 'n/a')} | "
            f"`{check.get('status', 'n/a')}` | "
            f"`{check.get('actual', 'n/a')}` | "
            f"`{check.get('expected', 'n/a')}` |"
        )
    lines.append("")
    return lines


def _row_counts(row_counts: dict[str, Any]) -> str:
    return "<br>".join(f"`{key}` {value}" for key, value in sorted(row_counts.items())) or "n/a"


def _short_hash(value: Any) -> str:
    text = str(value or "")
    return text[:12] if text else "n/a"
