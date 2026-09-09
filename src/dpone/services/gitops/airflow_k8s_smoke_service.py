from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_k8s_smoke import (
    GitOpsAirflowK8sSmokePlanner,
    GitOpsAirflowK8sSmokeReport,
)
from dpone.gitops.airflow_k8s_smoke_runner import (
    AirflowK8sSmokeRunner,
    SubprocessAirflowK8sSmokeRunner,
)
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta

_SOURCE = "dpone gitops airflow k8s-smoke"


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowK8sSmokeContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsAirflowK8sSmokeService:
    """Build and optionally execute Airflow Kubernetes smoke contracts."""

    def __init__(self, *, ctx: GitOpsAirflowK8sSmokeContext, runner: AirflowK8sSmokeRunner | None = None) -> None:
        self._ctx = ctx
        self._runner = runner or SubprocessAirflowK8sSmokeRunner()
        self._planner = GitOpsAirflowK8sSmokePlanner()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_paths(args)
        if path_blockers:
            report = _blocked_report(paths=paths, args=args, blockers=path_blockers)
            return _view(args=args, report=report)

        run_spec, run_spec_blockers = _load_json(self._ctx, repo_root, paths.run_spec_path, paths.run_spec_label)
        runtime_profile, profile_blockers = _load_json(
            self._ctx,
            repo_root,
            paths.runtime_profile_path,
            paths.runtime_profile_label,
        )
        pod_contract, pod_blockers = _load_json(self._ctx, repo_root, paths.pod_contract_path, paths.pod_contract_label)
        image_contract, image_blockers = _load_optional_json(
            self._ctx,
            repo_root,
            paths.image_contract_path,
            paths.image_contract_label,
        )
        xcom_summary, xcom_blockers = _load_optional_json(
            self._ctx,
            repo_root,
            paths.xcom_summary_path,
            paths.xcom_summary_label,
        )
        blockers = (*run_spec_blockers, *profile_blockers, *pod_blockers, *image_blockers, *xcom_blockers)
        if blockers:
            report = _blocked_report(paths=paths, args=args, blockers=blockers)
            return _view(args=args, report=report)

        report = self._planner.plan(
            run_spec_path=paths.run_spec_label,
            run_spec=_mapping(run_spec),
            runtime_profile_path=paths.runtime_profile_label,
            runtime_profile=_mapping(runtime_profile),
            pod_contract_path=paths.pod_contract_label,
            pod_contract=_mapping(pod_contract),
            image_contract_path=paths.image_contract_label,
            image_contract=_mapping(image_contract) if image_contract is not None else None,
            xcom_summary_path=paths.xcom_summary_label,
            xcom_summary=_mapping(xcom_summary) if xcom_summary is not None else None,
            mode=str(getattr(args, "mode", "plan")),
            runner_kind=str(getattr(args, "runner_kind", "kubernetes_pod_operator")),
            runner_policy=str(getattr(args, "runner_policy", "advisory")),
            smoke_name=str(getattr(args, "smoke_name", "dpone-smoke")),
            timeout_seconds=int(getattr(args, "timeout_seconds", 300)),
            kubectl=str(getattr(args, "kubectl", "kubectl")),
            airflow_cmd=str(getattr(args, "airflow_cmd", "airflow")),
        )
        if getattr(args, "mode", "plan") == "live" and report.passed:
            results = tuple(self._runner.run(command=command, cwd=repo_root) for command in report.commands)
            report = self._planner.with_results(report, results=results)
        return _view(args=args, report=report)


class _SmokePaths:
    def __init__(
        self,
        *,
        run_spec_path: Path,
        run_spec_label: str,
        runtime_profile_path: Path,
        runtime_profile_label: str,
        pod_contract_path: Path,
        pod_contract_label: str,
        image_contract_path: Path | None,
        image_contract_label: str | None,
        xcom_summary_path: Path | None,
        xcom_summary_label: str | None,
    ) -> None:
        self.run_spec_path = run_spec_path
        self.run_spec_label = run_spec_label
        self.runtime_profile_path = runtime_profile_path
        self.runtime_profile_label = runtime_profile_label
        self.pod_contract_path = pod_contract_path
        self.pod_contract_label = pod_contract_label
        self.image_contract_path = image_contract_path
        self.image_contract_label = image_contract_label
        self.xcom_summary_path = xcom_summary_path
        self.xcom_summary_label = xcom_summary_label


def _resolve_paths(args: object) -> tuple[_SmokePaths, tuple[GitOpsIssue, ...]]:
    run_spec_path, run_spec_label, run_spec_issue = _safe_path(getattr(args, "run_spec_path", None), "RUN_SPEC")
    profile_path, profile_label, profile_issue = _safe_path(
        getattr(args, "runtime_profile_path", None),
        "--runtime-profile-path",
    )
    pod_path, pod_label, pod_issue = _safe_path(getattr(args, "pod_contract_path", None), "--pod-contract-path")
    image_path, image_label, image_issue = _safe_optional_path(
        getattr(args, "image_contract_path", None),
        "--image-contract-path",
    )
    xcom_path, xcom_label, xcom_issue = _safe_optional_path(
        getattr(args, "xcom_summary_path", None),
        "--xcom-summary-path",
    )
    return (
        _SmokePaths(
            run_spec_path=run_spec_path,
            run_spec_label=run_spec_label,
            runtime_profile_path=profile_path,
            runtime_profile_label=profile_label,
            pod_contract_path=pod_path,
            pod_contract_label=pod_label,
            image_contract_path=image_path,
            image_contract_label=image_label,
            xcom_summary_path=xcom_path,
            xcom_summary_label=xcom_label,
        ),
        tuple(issue for issue in (run_spec_issue, profile_issue, pod_issue, image_issue, xcom_issue) if issue),
    )


def _safe_path(raw_path: object, source: str) -> tuple[Path, str, GitOpsIssue | None]:
    try:
        path = safe_relative_path(raw_path, source=source)
    except GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return Path("."), label, GitOpsIssue(code="invalid_path", message=str(exc), path=label, source=source)
    return path, "." if path.as_posix() == "." else path.as_posix(), None


def _safe_optional_path(raw_path: object, source: str) -> tuple[Path | None, str | None, GitOpsIssue | None]:
    if raw_path in (None, ""):
        return None, None, None
    path, label, issue = _safe_path(raw_path, source)
    return path, label, issue


def _load_json(
    ctx: GitOpsAirflowK8sSmokeContext,
    repo_root: Path,
    path: Path,
    label: str,
) -> tuple[object, tuple[GitOpsIssue, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (
            _issue(code="airflow_k8s_artifact_missing", message="Required JSON artifact is missing", path=label),
        )
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (
            _issue(
                code="airflow_k8s_json_invalid", message=f"JSON artifact could not be parsed: {exc.msg}", path=label
            ),
        )


def _load_optional_json(
    ctx: GitOpsAirflowK8sSmokeContext,
    repo_root: Path,
    path: Path | None,
    label: str | None,
) -> tuple[object | None, tuple[GitOpsIssue, ...]]:
    if path is None or label is None:
        return None, ()
    return _load_json(ctx, repo_root, path, label)


def _blocked_report(
    *,
    paths: _SmokePaths,
    args: object,
    blockers: tuple[GitOpsIssue, ...],
) -> GitOpsAirflowK8sSmokeReport:
    return GitOpsAirflowK8sSmokeReport(
        mode=str(getattr(args, "mode", "plan")),
        runner_kind=str(getattr(args, "runner_kind", "kubernetes_pod_operator")),
        runner_policy=str(getattr(args, "runner_policy", "advisory")),
        run_spec_path=paths.run_spec_label,
        runtime_profile_path=paths.runtime_profile_label,
        pod_contract_path=paths.pod_contract_label,
        image_contract_path=paths.image_contract_label,
        xcom_summary_path=paths.xcom_summary_label,
        namespace="",
        service_account="",
        image="",
        image_digest=None,
        image_ref="",
        smoke_name=str(getattr(args, "smoke_name", "dpone-smoke")),
        timeout_seconds=int(getattr(args, "timeout_seconds", 300)),
        checks=(),
        commands=(),
        blockers=blockers,
    )


def _view(*, args: object, report: GitOpsAirflowK8sSmokeReport) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_k8s_smoke",
            path=report.pod_contract_path,
            options={"format": getattr(args, "format", "json"), "mode": report.mode},
        ),
        report=report,
    )


def _issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=_SOURCE)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = ["GitOpsAirflowK8sSmokeContext", "GitOpsAirflowK8sSmokeService"]
