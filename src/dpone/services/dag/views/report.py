from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dpone.dag.dag_report import DagReport

from .common import DagViewMeta, build_meta


@dataclass(frozen=True, slots=True)
class DagReportView:
    meta: DagViewMeta
    report: DagReport

    def to_jsonable(self) -> dict[str, Any]:
        report_json = self.report.to_jsonable()
        data = self.meta.to_jsonable()
        data.update(report_json)  # legacy-compatible top-level fields
        data["report"] = report_json
        return data


def build_dag_report_view(
    *,
    dag: Any,
    report: DagReport,
    preset: str,
    max_edges: int,
    md_max_edges: int,
    max_triggers: int,
    include_evidence: bool,
    registry_require_fields: Sequence[str],
    lint_enabled: bool,
    profile_name: str | None,
    fail_on: str,
    fail_severity: str,
) -> DagReportView:
    return DagReportView(
        meta=build_meta(
            "dag.report",
            dag,
            options={
                "preset": preset,
                "max_edges": max_edges,
                "md_max_edges": md_max_edges,
                "max_triggers": max_triggers,
                "include_evidence": bool(include_evidence),
                "registry_require_fields": list(registry_require_fields),
                "lint_enabled": bool(lint_enabled),
                "profile": profile_name,
                "fail_on": fail_on,
                "fail_severity": fail_severity,
            },
        ),
        report=report,
    )
