from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_outcome_gate import GitOpsAirflowOutcomeGateEvaluator, GitOpsAirflowOutcomeGateReport
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowOutcomeGateContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsAirflowOutcomeGateService:
    """Evaluate final Airflow XCom summaries for downstream outcome gates."""

    def __init__(self, *, ctx: GitOpsAirflowOutcomeGateContext) -> None:
        self._ctx = ctx
        self._evaluator = GitOpsAirflowOutcomeGateEvaluator()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        xcom_path, xcom_label, path_issue = _safe_path(getattr(args, "xcom_summary_path", None), source="XCOM_SUMMARY")
        required_status = _text(getattr(args, "required_status", None)) or "passed"
        if path_issue is not None:
            report = _blocked_report(path=xcom_label, required_status=required_status, blockers=(path_issue,))
            return _view(args=args, report=report, path=xcom_label)

        xcom_summary, load_blockers = _load_json(ctx=self._ctx, repo_root=repo_root, path=xcom_path, label=xcom_label)
        if load_blockers:
            report = _blocked_report(path=xcom_label, required_status=required_status, blockers=load_blockers)
            return _view(args=args, report=report, path=xcom_label)

        report = self._evaluator.evaluate(
            xcom_summary_path=xcom_label,
            xcom_summary=_mapping(xcom_summary),
            required_status=required_status,
        )
        return _view(args=args, report=report, path=xcom_label)


def _load_json(
    *,
    ctx: GitOpsAirflowOutcomeGateContext,
    repo_root: Path,
    path: Path,
    label: str,
) -> tuple[object, tuple[GitOpsIssue, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (
            GitOpsIssue(
                code="xcom_summary_missing",
                message="Airflow XCom summary JSON does not exist",
                path=label,
                source="dpone gitops airflow outcome-gate",
            ),
        )
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (
            GitOpsIssue(
                code="xcom_summary_json_invalid",
                message=f"Airflow XCom summary JSON could not be parsed: {exc.msg}",
                path=label,
                source="dpone gitops airflow outcome-gate",
            ),
        )


def _blocked_report(
    *,
    path: str,
    required_status: str,
    blockers: tuple[GitOpsIssue, ...],
) -> GitOpsAirflowOutcomeGateReport:
    return GitOpsAirflowOutcomeGateReport(
        xcom_summary_path=path,
        required_status=required_status,
        status="unknown",
        passed=False,
        blockers=blockers,
    )


def _safe_path(raw_path: object, *, source: str) -> tuple[Path, str, GitOpsIssue | None]:
    try:
        path = safe_relative_path(raw_path, source=source)
    except GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return Path("."), label, GitOpsIssue(code="invalid_path", message=str(exc), path=label, source=source)
    return path, "." if path.as_posix() == "." else path.as_posix(), None


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


def _view(*, args: object, report: GitOpsAirflowOutcomeGateReport, path: str) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_outcome_gate",
            path=path,
            options={
                "format": getattr(args, "format", "json"),
                "required_status": getattr(args, "required_status", "passed"),
            },
        ),
        report=report,
    )


__all__ = ["GitOpsAirflowOutcomeGateContext", "GitOpsAirflowOutcomeGateService"]
