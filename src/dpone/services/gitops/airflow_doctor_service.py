from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol

from dpone.gitops.airflow_doctor import GitOpsAirflowDoctor
from dpone.gitops.airflow_models import GitOpsAirflowCheck, GitOpsAirflowDoctorReport
from dpone.gitops.airflow_policy import (
    AirflowRunnerPolicyFinding,
    GitOpsAirflowRunnerPolicyEvaluator,
    normalize_airflow_runner_policy,
)
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowDoctorContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class GitOpsAirflowDoctorService:
    """Runs offline readiness checks for Airflow/Kubernetes runner artifacts."""

    def __init__(self, *, ctx: GitOpsAirflowDoctorContext) -> None:
        self._ctx = ctx
        self._doctor = GitOpsAirflowDoctor()
        self._policy = GitOpsAirflowRunnerPolicyEvaluator()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_paths(args)
        if path_blockers:
            report = _report(args=args, paths=paths, checks=(), warnings=(), blockers=path_blockers)
            return _view(args=args, report=report)

        bundle, bundle_load_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.bundle_path,
            label=paths.bundle_label,
            missing_code="bundle_missing",
            invalid_code="bundle_json_invalid",
        )
        pod_template, pod_load_blockers = _load_yaml(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.pod_template_path,
            label=paths.pod_template_label,
        )
        image_contract, image_load_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.image_contract_path,
            label=paths.image_contract_label,
            missing_code="image_contract_missing",
            invalid_code="image_contract_json_invalid",
        )
        checks: list[GitOpsAirflowCheck] = []
        warnings: list[GitOpsIssue] = []
        blockers: list[GitOpsIssue] = [*bundle_load_blockers, *pod_load_blockers, *image_load_blockers]

        if not bundle_load_blockers:
            bundle_checks, bundle_warnings, bundle_blockers = self._doctor.validate_bundle(
                bundle_path=paths.bundle_label,
                bundle=bundle,
                require_attestation=bool(getattr(args, "require_attestation", False)),
            )
            checks.extend(bundle_checks)
            warnings.extend(bundle_warnings)
            blockers.extend(bundle_blockers)
        if not pod_load_blockers:
            pod_checks, pod_blockers = self._doctor.validate_pod_template(
                pod_template_path=paths.pod_template_label,
                pod_template=pod_template,
            )
            checks.extend(pod_checks)
            blockers.extend(pod_blockers)
        if not image_load_blockers:
            image_checks, image_blockers = self._doctor.validate_image_contract(
                image_contract_path=paths.image_contract_label,
                image_contract=image_contract,
                expected_image=_optional_str(getattr(args, "image", None)),
            )
            checks.extend(image_checks)
            blockers.extend(image_blockers)
        if not bundle_load_blockers and not pod_load_blockers and not image_load_blockers:
            policy_findings = self._policy.evaluate(
                profile=normalize_airflow_runner_policy(getattr(args, "runner_policy", "advisory")),
                bundle_path=paths.bundle_label,
                bundle=bundle,
                pod_template_path=paths.pod_template_label,
                pod_template=pod_template,
                image_contract_path=paths.image_contract_label,
                image_contract=image_contract,
            )
            checks.extend(_policy_checks(policy_findings))
            warnings.extend(_policy_issues(policy_findings, severity="warning"))
            blockers.extend(_policy_issues(policy_findings, severity="blocker"))

        report = _report(
            args=args, paths=paths, checks=tuple(checks), warnings=tuple(warnings), blockers=tuple(blockers)
        )
        return _view(args=args, report=report)


class _ResolvedAirflowPaths:
    def __init__(
        self,
        *,
        bundle_path: Path,
        bundle_label: str,
        pod_template_path: Path,
        pod_template_label: str,
        image_contract_path: Path,
        image_contract_label: str,
    ) -> None:
        self.bundle_path = bundle_path
        self.bundle_label = bundle_label
        self.pod_template_path = pod_template_path
        self.pod_template_label = pod_template_label
        self.image_contract_path = image_contract_path
        self.image_contract_label = image_contract_label


def _resolve_paths(args: object) -> tuple[_ResolvedAirflowPaths, tuple[GitOpsIssue, ...]]:
    resolved: dict[str, tuple[Path, str]] = {}
    blockers: list[GitOpsIssue] = []
    for attr, source in (
        ("bundle_path", "BUNDLE"),
        ("pod_template", "--pod-template"),
        ("image_contract", "--image-contract"),
    ):
        raw_path = getattr(args, attr, None)
        try:
            path = safe_relative_path(raw_path, source=source)
            resolved[attr] = (path, path.as_posix())
        except GitOpsPathValidationError as exc:
            blockers.append(GitOpsIssue(code="invalid_path", message=str(exc), path=str(raw_path or ""), source=source))
            resolved[attr] = (Path("."), str(raw_path or ""))
    paths = _ResolvedAirflowPaths(
        bundle_path=resolved["bundle_path"][0],
        bundle_label=resolved["bundle_path"][1],
        pod_template_path=resolved["pod_template"][0],
        pod_template_label=resolved["pod_template"][1],
        image_contract_path=resolved["image_contract"][0],
        image_contract_label=resolved["image_contract"][1],
    )
    return paths, tuple(blockers)


def _load_json(
    *,
    ctx: GitOpsAirflowDoctorContext,
    repo_root: Path,
    path: Path,
    label: str,
    missing_code: str,
    invalid_code: str,
) -> tuple[object, tuple[GitOpsIssue, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (_issue(code=missing_code, message="Required JSON artifact does not exist", path=label),)
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (_issue(code=invalid_code, message=f"JSON artifact could not be parsed: {exc.msg}", path=label),)


def _load_yaml(
    *,
    ctx: GitOpsAirflowDoctorContext,
    repo_root: Path,
    path: Path,
    label: str,
) -> tuple[object, tuple[GitOpsIssue, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (_issue(code="pod_template_missing", message="Pod template artifact does not exist", path=label),)
    try:
        return ctx.yaml.load(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except Exception as exc:  # pragma: no cover - adapter-specific parser errors
        return None, (
            _issue(
                code="pod_template_yaml_invalid", message=f"Pod template YAML could not be parsed: {exc}", path=label
            ),
        )


def _report(
    *,
    args: object,
    paths: _ResolvedAirflowPaths,
    checks: tuple[GitOpsAirflowCheck, ...],
    warnings: tuple[GitOpsIssue, ...],
    blockers: tuple[GitOpsIssue, ...],
) -> GitOpsAirflowDoctorReport:
    return GitOpsAirflowDoctorReport(
        bundle_path=paths.bundle_label,
        pod_template=paths.pod_template_label,
        image_contract=paths.image_contract_label,
        image=_optional_str(getattr(args, "image", None)),
        runner_policy=normalize_airflow_runner_policy(getattr(args, "runner_policy", "advisory")),
        checks=checks,
        warnings=warnings,
        blockers=blockers,
    )


def _optional_str(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source="dpone gitops airflow doctor")


def _policy_checks(findings: tuple[AirflowRunnerPolicyFinding, ...]) -> tuple[GitOpsAirflowCheck, ...]:
    return tuple(
        GitOpsAirflowCheck(
            name=finding.name,
            passed=finding.passed,
            severity=finding.severity,
            message=finding.message,
            path=finding.path,
            source="dpone gitops airflow policy",
        )
        for finding in findings
    )


def _policy_issues(
    findings: tuple[AirflowRunnerPolicyFinding, ...],
    *,
    severity: str,
) -> tuple[GitOpsIssue, ...]:
    return tuple(
        GitOpsIssue(
            code=finding.issue_code,
            message=finding.message,
            path=finding.path,
            source="dpone gitops airflow policy",
        )
        for finding in findings
        if not finding.passed and finding.severity == severity
    )


def _view(*, args: object, report: GitOpsAirflowDoctorReport) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_doctor",
            path=report.bundle_path,
            options={
                "format": getattr(args, "format", "json"),
                "require_attestation": bool(getattr(args, "require_attestation", False)),
                "runner_policy": report.runner_policy,
            },
        ),
        report=report,
    )


__all__ = ["GitOpsAirflowDoctorContext", "GitOpsAirflowDoctorService"]
