from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta

_SOURCE = "dpone gitops airflow cluster-doctor"


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowClusterDoctorContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsAirflowClusterDoctorService:
    """Build and optionally execute opt-in Airflow cluster readiness checks."""

    def __init__(self, *, ctx: GitOpsAirflowClusterDoctorContext, runner: Any | None = None) -> None:
        self._ctx = ctx
        self._runner = runner or _runner_domain().SubprocessAirflowClusterDoctorRunner()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_paths(args)
        if path_blockers:
            return _view(args=args, report=_blocked_report(paths=paths, args=args, blockers=path_blockers))

        runtime_profile, profile_blockers = _load_json(
            self._ctx,
            repo_root,
            paths.runtime_profile_path,
            paths.runtime_profile_label,
            "airflow_cluster_runtime_profile_missing",
            "airflow_cluster_runtime_profile_json_invalid",
        )
        pod_contract, contract_blockers = _load_json(
            self._ctx,
            repo_root,
            paths.pod_contract_path,
            paths.pod_contract_label,
            "airflow_cluster_pod_contract_missing",
            "airflow_cluster_pod_contract_json_invalid",
        )
        bridge_plan, bridge_blockers = _load_optional_json(
            self._ctx,
            repo_root,
            paths.connection_bridge_plan_path,
            paths.connection_bridge_plan_label,
            explicit=paths.connection_bridge_plan_explicit,
        )
        blockers = (*profile_blockers, *contract_blockers, *bridge_blockers)
        if blockers:
            return _view(args=args, report=_blocked_report(paths=paths, args=args, blockers=blockers))

        report = (
            _planner_domain()
            .GitOpsAirflowClusterDoctorPlanner()
            .plan(
                artifact_dir=paths.artifact_dir_label,
                runtime_profile_path=paths.runtime_profile_label,
                runtime_profile=_mapping(runtime_profile),
                pod_contract_path=paths.pod_contract_label,
                pod_contract=_mapping(pod_contract),
                connection_bridge_plan_path=paths.connection_bridge_plan_label,
                connection_bridge_plan=_mapping(bridge_plan) if bridge_plan is not None else None,
                mode=str(getattr(args, "mode", "plan")),
                runner_policy=str(getattr(args, "runner_policy", "advisory")),
                timeout_seconds=int(getattr(args, "timeout_seconds", 120)),
                kubectl=str(getattr(args, "kubectl", "kubectl")),
                extra_external_secrets=tuple(getattr(args, "external_secret", ()) or ()),
                require_external_secret_ready=bool(getattr(args, "require_external_secret_ready", False)),
                require_external_secret=bool(getattr(args, "require_external_secret", False)),
            )
        )
        if getattr(args, "mode", "plan") == "live" and report.passed:
            results = tuple(self._runner.run(command=command, cwd=repo_root) for command in report.commands)
            report = _planner_domain().GitOpsAirflowClusterDoctorPlanner().with_results(report, results=results)
        return _view(args=args, report=report)


class _ClusterDoctorPaths:
    def __init__(
        self,
        *,
        artifact_dir_path: Path,
        artifact_dir_label: str,
        runtime_profile_path: Path,
        runtime_profile_label: str,
        pod_contract_path: Path,
        pod_contract_label: str,
        connection_bridge_plan_path: Path,
        connection_bridge_plan_label: str,
        connection_bridge_plan_explicit: bool,
    ) -> None:
        self.artifact_dir_path = artifact_dir_path
        self.artifact_dir_label = artifact_dir_label
        self.runtime_profile_path = runtime_profile_path
        self.runtime_profile_label = runtime_profile_label
        self.pod_contract_path = pod_contract_path
        self.pod_contract_label = pod_contract_label
        self.connection_bridge_plan_path = connection_bridge_plan_path
        self.connection_bridge_plan_label = connection_bridge_plan_label
        self.connection_bridge_plan_explicit = connection_bridge_plan_explicit


def _resolve_paths(args: object) -> tuple[_ClusterDoctorPaths, tuple[Any, ...]]:
    artifact_dir_path, artifact_dir_label, artifact_issue = _safe_path(
        getattr(args, "artifact_dir", ".dpone/gitops/airflow") or ".dpone/gitops/airflow",
        source="--artifact-dir",
    )
    profile_path, profile_label, profile_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "runtime_profile_path", None),
        artifact_name="runtime-profile.json",
        source="--runtime-profile-path",
    )
    pod_path, pod_label, pod_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "pod_contract_path", None),
        artifact_name="pod-contract.json",
        source="--pod-contract-path",
    )
    bridge_raw = getattr(args, "connection_bridge_plan_path", None)
    bridge_path, bridge_label, bridge_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=bridge_raw,
        artifact_name="connection-bridge-plan.json",
        source="--connection-bridge-plan-path",
    )
    paths = _ClusterDoctorPaths(
        artifact_dir_path=artifact_dir_path,
        artifact_dir_label=artifact_dir_label,
        runtime_profile_path=profile_path,
        runtime_profile_label=profile_label,
        pod_contract_path=pod_path,
        pod_contract_label=pod_label,
        connection_bridge_plan_path=bridge_path,
        connection_bridge_plan_label=bridge_label,
        connection_bridge_plan_explicit=bool(bridge_raw),
    )
    return paths, tuple(issue for issue in (artifact_issue, profile_issue, pod_issue, bridge_issue) if issue)


def _artifact_or_explicit_path(
    artifact_dir: Path,
    *,
    raw_path: object,
    artifact_name: str,
    source: str,
) -> tuple[Path, str, Any | None]:
    if raw_path:
        return _safe_path(raw_path, source=source)
    path = artifact_dir / artifact_name
    return path, _label(path), None


def _load_json(
    ctx: GitOpsAirflowClusterDoctorContext,
    repo_root: Path,
    path: Path,
    label: str,
    missing_code: str,
    invalid_code: str,
) -> tuple[object, tuple[Any, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (_issue(code=missing_code, message="Required JSON artifact is missing", path=label),)
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (_issue(code=invalid_code, message=f"JSON artifact could not be parsed: {exc.msg}", path=label),)


def _load_optional_json(
    ctx: GitOpsAirflowClusterDoctorContext,
    repo_root: Path,
    path: Path,
    label: str,
    *,
    explicit: bool,
) -> tuple[object | None, tuple[Any, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        if explicit:
            issue = _issue(
                code="airflow_cluster_connection_bridge_plan_missing",
                message="Explicit connection bridge plan JSON artifact is missing",
                path=label,
            )
            return None, (issue,)
        return None, ()
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        issue = _issue(
            code="airflow_cluster_connection_bridge_plan_json_invalid",
            message=f"Connection bridge plan JSON could not be parsed: {exc.msg}",
            path=label,
        )
        return None, (issue,)


def _blocked_report(*, paths: _ClusterDoctorPaths, args: object, blockers: tuple[Any, ...]) -> Any:
    return _models_domain().GitOpsAirflowClusterDoctorReport(
        mode=str(getattr(args, "mode", "plan")),
        runner_policy=str(getattr(args, "runner_policy", "advisory")),
        artifact_dir=paths.artifact_dir_label,
        runtime_profile_path=paths.runtime_profile_label,
        pod_contract_path=paths.pod_contract_label,
        connection_bridge_plan_path=paths.connection_bridge_plan_label,
        namespace="",
        service_account="",
        timeout_seconds=int(getattr(args, "timeout_seconds", 120)),
        secret_refs=(),
        external_secret_refs=(),
        checks=(),
        commands=(),
        blockers=blockers,
    )


def _view(*, args: object, report: Any) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_cluster_doctor",
            path=report.connection_bridge_plan_path or report.pod_contract_path,
            options={"format": getattr(args, "format", "json"), "mode": report.mode},
        ),
        report=report,
    )


def _safe_path(raw_path: object, *, source: str) -> tuple[Path, str, Any | None]:
    try:
        path = _paths_domain().safe_relative_path(raw_path, source=source)
    except _paths_domain().GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return Path("."), label, _issue(code="invalid_path", message=str(exc), path=label)
    return path, _label(path), None


def _label(path: Path) -> str:
    return "." if path.as_posix() == "." else path.as_posix()


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _issue(*, code: str, message: str, path: str) -> Any:
    return _gitops_models().GitOpsIssue(code=code, message=message, path=path, source=_SOURCE)


def _planner_domain() -> Any:
    return import_module("dpone.gitops.airflow_cluster_doctor")


def _runner_domain() -> Any:
    return import_module("dpone.gitops.airflow_cluster_doctor_runner")


def _models_domain() -> Any:
    return import_module("dpone.gitops.airflow_cluster_doctor_models")


def _paths_domain() -> Any:
    return import_module("dpone.gitops.paths")


def _gitops_models() -> Any:
    return import_module("dpone.gitops.models")


__all__ = ["GitOpsAirflowClusterDoctorContext", "GitOpsAirflowClusterDoctorService"]
