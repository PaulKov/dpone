from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_pod_doctor import (
    GitOpsAirflowPodDoctor,
    GitOpsAirflowPodDoctorReport,
    build_pod_doctor_issue,
)
from dpone.gitops.airflow_policy import normalize_airflow_runner_policy
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowPodDoctorContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class GitOpsAirflowPodDoctorService:
    """Validate generated Airflow pod contract artifacts offline."""

    def __init__(self, *, ctx: GitOpsAirflowPodDoctorContext) -> None:
        self._ctx = ctx
        self._doctor = GitOpsAirflowPodDoctor()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_paths(args)
        runner_policy = normalize_airflow_runner_policy(getattr(args, "runner_policy", "advisory"))
        if path_blockers:
            report = _report(paths=paths, runner_policy=runner_policy, checks=(), warnings=(), blockers=path_blockers)
            return _view(args=args, report=report)

        pod_contract, contract_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.pod_contract_path,
            label=paths.pod_contract_label,
            missing_code="pod_contract_missing",
            invalid_code="pod_contract_json_invalid",
        )
        pod_spec, spec_blockers = _load_yaml(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.pod_spec_path,
            label=paths.pod_spec_label,
        )
        kpo_kwargs, kwargs_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.kpo_kwargs_path,
            label=paths.kpo_kwargs_label,
            missing_code="kpo_kwargs_missing",
            invalid_code="kpo_kwargs_json_invalid",
        )
        checks: tuple[Any, ...] = ()
        warnings: tuple[Any, ...] = ()
        blockers: tuple[Any, ...] = (*contract_blockers, *spec_blockers, *kwargs_blockers)
        if not blockers:
            checks, warnings, doctor_blockers = self._doctor.validate(
                pod_contract_path=paths.pod_contract_label,
                pod_contract=pod_contract,
                pod_spec_path=paths.pod_spec_label,
                pod_spec=pod_spec,
                kpo_kwargs_path=paths.kpo_kwargs_label,
                kpo_kwargs=kpo_kwargs,
                runner_policy=runner_policy,
            )
            blockers = (*blockers, *doctor_blockers)
        report = _report(
            paths=paths,
            runner_policy=runner_policy,
            checks=checks,
            warnings=warnings,
            blockers=blockers,
        )
        return _view(args=args, report=report)


class _PodDoctorPaths:
    def __init__(
        self,
        *,
        artifact_dir_path: Path,
        artifact_dir_label: str,
        pod_contract_path: Path,
        pod_contract_label: str,
        pod_spec_path: Path,
        pod_spec_label: str,
        kpo_kwargs_path: Path,
        kpo_kwargs_label: str,
    ) -> None:
        self.artifact_dir_path = artifact_dir_path
        self.artifact_dir_label = artifact_dir_label
        self.pod_contract_path = pod_contract_path
        self.pod_contract_label = pod_contract_label
        self.pod_spec_path = pod_spec_path
        self.pod_spec_label = pod_spec_label
        self.kpo_kwargs_path = kpo_kwargs_path
        self.kpo_kwargs_label = kpo_kwargs_label


def _resolve_paths(args: object) -> tuple[_PodDoctorPaths, tuple[Any, ...]]:
    artifact_dir_path, artifact_dir_label, artifact_issue = _safe_path(
        getattr(args, "artifact_dir", ".dpone/gitops/airflow") or ".dpone/gitops/airflow",
        source="--artifact-dir",
    )
    contract_path, contract_label, contract_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "pod_contract_path", None),
        artifact_name="pod-contract.json",
        source="--pod-contract-path",
    )
    pod_spec_path, pod_spec_label, pod_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "pod_spec_path", None),
        artifact_name="pod-spec.yaml",
        source="--pod-spec-path",
    )
    kpo_kwargs_path, kpo_kwargs_label, kpo_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "kpo_kwargs_path", None),
        artifact_name="kpo-kwargs.json",
        source="--kpo-kwargs-path",
    )
    paths = _PodDoctorPaths(
        artifact_dir_path=artifact_dir_path,
        artifact_dir_label=artifact_dir_label,
        pod_contract_path=contract_path,
        pod_contract_label=contract_label,
        pod_spec_path=pod_spec_path,
        pod_spec_label=pod_spec_label,
        kpo_kwargs_path=kpo_kwargs_path,
        kpo_kwargs_label=kpo_kwargs_label,
    )
    return paths, tuple(issue for issue in (artifact_issue, contract_issue, pod_issue, kpo_issue) if issue)


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
    return path, "." if path.as_posix() == "." else path.as_posix(), None


def _load_json(
    *,
    ctx: GitOpsAirflowPodDoctorContext,
    repo_root: Path,
    path: Path,
    label: str,
    missing_code: str,
    invalid_code: str,
) -> tuple[object, tuple[Any, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (_issue(code=missing_code, message="Required JSON artifact does not exist", path=label),)
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (_issue(code=invalid_code, message=f"JSON artifact could not be parsed: {exc.msg}", path=label),)


def _load_yaml(
    *,
    ctx: GitOpsAirflowPodDoctorContext,
    repo_root: Path,
    path: Path,
    label: str,
) -> tuple[object, tuple[Any, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (_issue(code="pod_spec_missing", message="Pod spec artifact does not exist", path=label),)
    try:
        return ctx.yaml.load(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except Exception as exc:  # pragma: no cover - adapter-specific parser errors
        return None, (
            _issue(code="pod_spec_yaml_invalid", message=f"Pod spec YAML could not be parsed: {exc}", path=label),
        )


def _safe_path(raw_path: object, *, source: str) -> tuple[Path, str, Any | None]:
    try:
        path = safe_relative_path(raw_path, source=source)
    except GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return (
            Path("."),
            label,
            build_pod_doctor_issue(code="invalid_path", message=str(exc), path=label, source=source),
        )
    return path, "." if path.as_posix() == "." else path.as_posix(), None


def _report(
    *,
    paths: _PodDoctorPaths,
    runner_policy: str,
    checks: tuple[Any, ...],
    warnings: tuple[Any, ...],
    blockers: tuple[Any, ...],
) -> GitOpsAirflowPodDoctorReport:
    return GitOpsAirflowPodDoctorReport(
        artifact_dir=paths.artifact_dir_label,
        pod_contract_path=paths.pod_contract_label,
        pod_spec_path=paths.pod_spec_label,
        kpo_kwargs_path=paths.kpo_kwargs_label,
        runner_policy=runner_policy,
        checks=checks,
        warnings=warnings,
        blockers=blockers,
    )


def _issue(*, code: str, message: str, path: str) -> Any:
    return build_pod_doctor_issue(code=code, message=message, path=path)


def _view(*, args: object, report: GitOpsAirflowPodDoctorReport) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_pod_doctor",
            path=report.pod_contract_path,
            options={
                "artifact_dir": report.artifact_dir,
                "format": getattr(args, "format", "json"),
                "runner_policy": report.runner_policy,
            },
        ),
        report=report,
    )


__all__ = ["GitOpsAirflowPodDoctorContext", "GitOpsAirflowPodDoctorService"]
