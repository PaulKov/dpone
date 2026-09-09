"""Compatibility facade for DAG report generation."""

from __future__ import annotations

from dpone.dag.dag_report_impl import (
    _detect_anomalies,
    _find_cycles,
    _from_registry_lint_issue,
    _from_validation_issue,
    _severity_rank,
    build_dag_report,
)
from dpone.dag.dag_report_models import DagReport, ReportAnomaly, ReportEdge, ReportLintIssue

__all__ = [
    "ReportEdge",
    "ReportAnomaly",
    "ReportLintIssue",
    "DagReport",
    "_severity_rank",
    "build_dag_report",
    "_from_validation_issue",
    "_from_registry_lint_issue",
    "_detect_anomalies",
    "_find_cycles",
]
