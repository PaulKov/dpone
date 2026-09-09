"""Renderers for data product assertion artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

ASSERTION_REPORT_SCHEMA = "dpone.data_product_assertion_report.v1"


class DataProductAssertionReportRenderer:
    """Build deterministic JSON/Markdown reports from assertion evaluations."""

    def report(self, *, evaluation: Mapping[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": ASSERTION_REPORT_SCHEMA,
            "status": evaluation.get("status"),
            "product_id": evaluation.get("product_id"),
            "assertion_evaluation_id": evaluation.get("assertion_evaluation_id"),
            "summary": dict(evaluation.get("summary", {})) if isinstance(evaluation.get("summary"), Mapping) else {},
            "blockers": [str(item) for item in evaluation.get("blockers", []) if str(item)],
            "warnings": [str(item) for item in evaluation.get("warnings", []) if str(item)],
        }
        payload["markdown"] = _markdown(payload, evaluation.get("assertions", []))
        payload["assertion_report_id"] = stable_fingerprint(payload)
        return payload


def _markdown(payload: Mapping[str, Any], assertions: object) -> str:
    lines = [
        "# Data Product Assertion Report",
        "",
        f"- product: `{payload.get('product_id')}`",
        f"- status: `{payload.get('status')}`",
        "",
        "## Summary",
    ]
    summary = payload.get("summary", {})
    if isinstance(summary, Mapping):
        for key in ("passed", "failed", "warning", "skipped"):
            lines.append(f"- {key}: `{summary.get(key, 0)}`")
    lines.extend(["", "## Assertions", "", "| Assertion | Status | Severity | Details |", "| --- | --- | --- | --- |"])
    for item in _assertion_items(assertions):
        details = ", ".join(str(detail) for detail in item.get("details", []))
        lines.append(f"| `{item.get('id')}` | `{item.get('status')}` | `{item.get('severity')}` | {details} |")
    if payload.get("blockers"):
        lines.extend(["", "## Blockers", *[f"- `{item}`" for item in payload["blockers"]]])
    if payload.get("warnings"):
        lines.extend(["", "## Warnings", *[f"- `{item}`" for item in payload["warnings"]]])
    return "\n".join(lines) + "\n"


def _assertion_items(raw: object) -> Sequence[Mapping[str, Any]]:
    return tuple(item for item in raw if isinstance(item, Mapping)) if isinstance(raw, list) else ()


__all__ = ["ASSERTION_REPORT_SCHEMA", "DataProductAssertionReportRenderer"]
