from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_runtime_executor import AirflowCommandRunner, GitOpsAirflowRunSpecExecutor
from dpone.gitops.airflow_runtime_models import GitOpsAirflowRuntimeEvidence
from dpone.gitops.airflow_xcom_outcome import (
    AirflowRunIdentityError,
    GitOpsAirflowXComOutcomeBuilder,
    parse_optional_airflow_run_identity,
)
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowRuntimeExecContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsAirflowRunSpecExecService:
    """Runtime adapter used inside the custom dpone Airflow image."""

    def __init__(
        self,
        *,
        ctx: GitOpsAirflowRuntimeExecContext,
        runner: AirflowCommandRunner | None = None,
        run_identity_json: str | None = None,
    ) -> None:
        self._ctx = ctx
        self._executor = GitOpsAirflowRunSpecExecutor(runner=runner)
        self._xcom_builder = GitOpsAirflowXComOutcomeBuilder()
        self._run_identity_json = run_identity_json

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_paths(args)
        if path_blockers:
            report = _blocked_evidence(paths=paths, blockers=path_blockers)
            return _view(args=args, report=report)

        run_identity, identity_blocker = _parse_run_identity(self._run_identity_json)
        if identity_blocker is not None:
            report = _blocked_evidence(paths=paths, blockers=(identity_blocker,))
            self._write_runtime_artifacts(repo_root=repo_root, paths=paths, report=report, run_identity=None)
            return _view(args=args, report=report)

        run_spec, load_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.run_spec_path,
            label=paths.run_spec_label,
        )
        if load_blockers:
            report = _blocked_evidence(paths=paths, blockers=load_blockers)
            self._write_runtime_artifacts(
                repo_root=repo_root,
                paths=paths,
                report=report,
                run_identity=run_identity,
            )
            return _view(args=args, report=report)

        run_spec_mapping = _mapping(run_spec)
        cwd = repo_root / _worktree_path(run_spec_mapping)
        report = self._executor.execute(
            run_spec_path=paths.run_spec_label,
            run_spec=run_spec_mapping,
            cwd=cwd,
        )
        self._write_runtime_artifacts(
            repo_root=repo_root,
            paths=paths,
            report=report,
            run_identity=run_identity,
        )
        return _view(args=args, report=report)

    def _write_runtime_artifacts(
        self,
        *,
        repo_root: Path,
        paths: _ExecPaths,
        report: GitOpsAirflowRuntimeEvidence,
        run_identity: Mapping[str, Any] | None,
    ) -> None:
        evidence_json = report.to_json()
        evidence_digest = "sha256:" + hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
        self._ctx.fs.write_text(repo_root / paths.evidence_path, evidence_json, encoding="utf-8")
        if not paths.xcom_path_blocked:
            xcom = self._xcom_builder.build(
                evidence=report,
                runtime_evidence_path=paths.evidence_label,
                runtime_evidence_sha256=evidence_digest,
                run_identity=run_identity,
            )
            self._ctx.fs.write_text(repo_root / paths.xcom_path, xcom.to_json(), encoding="utf-8")


class _ExecPaths:
    def __init__(
        self,
        *,
        run_spec_path: Path,
        run_spec_label: str,
        evidence_path: Path,
        evidence_label: str,
        xcom_path: Path,
        xcom_label: str,
        xcom_path_blocked: bool,
    ) -> None:
        self.run_spec_path = run_spec_path
        self.run_spec_label = run_spec_label
        self.evidence_path = evidence_path
        self.evidence_label = evidence_label
        self.xcom_path = xcom_path
        self.xcom_label = xcom_label
        self.xcom_path_blocked = xcom_path_blocked


def _resolve_paths(args: object) -> tuple[_ExecPaths, tuple[GitOpsIssue, ...]]:
    run_spec_path, run_spec_label, run_spec_issue = _safe_path(getattr(args, "run_spec_path", None), source="RUN_SPEC")
    raw_evidence = getattr(args, "evidence_output", None) or ".dpone/gitops/airflow/runtime-evidence.json"
    evidence_path, evidence_label, evidence_issue = _safe_path(raw_evidence, source="--evidence-output")
    raw_xcom = getattr(args, "xcom_output", None) or ".dpone/gitops/airflow/xcom-summary.json"
    xcom_path, xcom_label, xcom_issue = _safe_path(raw_xcom, source="--xcom-output")
    paths = _ExecPaths(
        run_spec_path=run_spec_path,
        run_spec_label=run_spec_label,
        evidence_path=evidence_path,
        evidence_label=evidence_label,
        xcom_path=xcom_path,
        xcom_label=xcom_label,
        xcom_path_blocked=xcom_issue is not None,
    )
    return paths, tuple(issue for issue in (run_spec_issue, evidence_issue, xcom_issue) if issue)


def _safe_path(raw_path: object, *, source: str) -> tuple[Path, str, GitOpsIssue | None]:
    try:
        path = safe_relative_path(raw_path, source=source)
    except GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return Path("."), label, GitOpsIssue(code="invalid_path", message=str(exc), path=label, source=source)
    return path, "." if path.as_posix() == "." else path.as_posix(), None


def _load_json(
    *,
    ctx: GitOpsAirflowRuntimeExecContext,
    repo_root: Path,
    path: Path,
    label: str,
) -> tuple[object, tuple[GitOpsIssue, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (
            GitOpsIssue(
                code="run_spec_missing",
                message="Run-spec JSON does not exist",
                path=label,
                source="dpone gitops airflow run-spec-exec",
            ),
        )
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (
            GitOpsIssue(
                code="run_spec_json_invalid",
                message=f"Run-spec JSON could not be parsed: {exc.msg}",
                path=label,
                source="dpone gitops airflow run-spec-exec",
            ),
        )


def _parse_run_identity(raw: str | None) -> tuple[Mapping[str, Any] | None, GitOpsIssue | None]:
    if raw is None or raw == "":
        return None, None
    try:
        return parse_optional_airflow_run_identity(raw), None
    except AirflowRunIdentityError as exc:
        return None, GitOpsIssue(
            code=exc.code,
            message="Airflow run identity is invalid; workload execution was blocked before any command ran",
            path="DPONE_AIRFLOW_RUN_IDENTITY",
            source="dpone gitops airflow run-spec-exec",
        )


def _blocked_evidence(*, paths: _ExecPaths, blockers: tuple[GitOpsIssue, ...]) -> GitOpsAirflowRuntimeEvidence:
    return GitOpsAirflowRuntimeEvidence(
        run_spec_path=paths.run_spec_label,
        bundle_path="",
        image="",
        image_digest=None,
        status="failed",
        started_at="",
        finished_at="",
        duration_seconds=0.0,
        steps=(),
        blockers=blockers,
    )


def _worktree_path(run_spec: Mapping[str, Any]) -> Path:
    try:
        return safe_relative_path(run_spec.get("worktree") or ".", source="run_spec.worktree")
    except GitOpsPathValidationError:
        return Path(".")


def _view(*, args: object, report: GitOpsAirflowRuntimeEvidence) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_runtime_evidence",
            path=getattr(args, "evidence_output", None),
            options={"format": getattr(args, "format", "json")},
        ),
        report=report,
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "GitOpsAirflowRunSpecExecService",
    "GitOpsAirflowRuntimeExecContext",
]
