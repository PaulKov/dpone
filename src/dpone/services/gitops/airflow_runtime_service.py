from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_doctor import GitOpsAirflowDoctor
from dpone.gitops.airflow_run_spec import GitOpsAirflowRunSpecBuilder
from dpone.gitops.airflow_runtime_evidence import AirflowRuntimeEvidenceFinding, GitOpsAirflowRuntimeEvidenceVerifier
from dpone.gitops.airflow_runtime_models import (
    GitOpsAirflowRunSpec,
    GitOpsAirflowRuntimeEvidence,
    read_airflow_runtime_evidence,
)
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowRuntimeContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsAirflowRunSpecService:
    """Build and write the Airflow runtime contract consumed by the custom dpone image."""

    def __init__(self, *, ctx: GitOpsAirflowRuntimeContext) -> None:
        self._ctx = ctx
        self._doctor = GitOpsAirflowDoctor()
        self._builder = GitOpsAirflowRunSpecBuilder()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_run_spec_paths(args)
        image = _optional_str(getattr(args, "image", None)) or ""
        blockers = [*path_blockers]
        if not image:
            blockers.append(_issue(code="image_required", message="Airflow run-spec requires --image", path="--image"))
        if blockers:
            report = _blocked_run_spec(args=args, paths=paths, image=image, blockers=tuple(blockers))
            return _run_spec_view(args=args, report=report)

        bundle, load_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.bundle_path,
            label=paths.bundle_label,
            missing_code="bundle_missing",
            invalid_code="bundle_json_invalid",
            source="dpone gitops airflow run-spec",
        )
        if load_blockers:
            report = _blocked_run_spec(args=args, paths=paths, image=image, blockers=load_blockers)
            return _run_spec_view(args=args, report=report)

        bundle_mapping = _mapping(bundle)
        _, bundle_warnings, bundle_blockers = self._doctor.validate_bundle(
            bundle_path=paths.bundle_label,
            bundle=bundle_mapping,
            require_attestation=bool(getattr(args, "require_attestation", False)),
        )
        report = self._builder.build(
            bundle_path=paths.bundle_label,
            bundle=bundle_mapping,
            image=image,
            image_digest=_optional_str(getattr(args, "image_digest", None)),
            worktree=paths.worktree_label,
            evidence_output=paths.evidence_label,
            require_attestation=bool(getattr(args, "require_attestation", False)),
        )
        if bundle_warnings or bundle_blockers:
            report = GitOpsAirflowRunSpec(
                bundle_path=report.bundle_path,
                bundle_digest=report.bundle_digest,
                image=report.image,
                image_digest=report.image_digest,
                worktree=report.worktree,
                evidence_output=report.evidence_output,
                entries=report.entries,
                steps=report.steps,
                warnings=(*report.warnings, *bundle_warnings),
                blockers=(*report.blockers, *bundle_blockers),
            )
        if report.passed:
            self._ctx.fs.write_text(repo_root / paths.output_path, report.to_json(), encoding="utf-8")
        return _run_spec_view(args=args, report=report)


class GitOpsAirflowEvidenceVerifyService:
    """Verify runtime evidence emitted by the custom dpone image."""

    def __init__(self, *, ctx: GitOpsAirflowRuntimeContext) -> None:
        self._ctx = ctx
        self._verifier = GitOpsAirflowRuntimeEvidenceVerifier()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_evidence_paths(args)
        if path_blockers:
            report = _blocked_evidence(paths=paths, blockers=path_blockers)
            return _evidence_view(args=args, report=report)

        run_spec, run_spec_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.run_spec_path,
            label=paths.run_spec_label,
            missing_code="run_spec_missing",
            invalid_code="run_spec_json_invalid",
            source="dpone gitops airflow evidence-verify",
        )
        evidence_payload, evidence_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.evidence_path,
            label=paths.evidence_label,
            missing_code="runtime_evidence_missing",
            invalid_code="runtime_evidence_json_invalid",
            source="dpone gitops airflow evidence-verify",
        )
        blockers = (*run_spec_blockers, *evidence_blockers)
        if blockers:
            report = _blocked_evidence(paths=paths, blockers=blockers)
            return _evidence_view(args=args, report=report)

        evidence = read_airflow_runtime_evidence(_mapping(evidence_payload))
        findings = self._verifier.verify(
            run_spec_path=paths.run_spec_label,
            run_spec=_mapping(run_spec),
            evidence=evidence,
            require_all_steps=bool(getattr(args, "require_all_steps", False)),
        )
        finding_blockers = _finding_issues(findings)
        report = evidence.with_issues(
            blockers=(*evidence.blockers, *finding_blockers),
            status="failed" if finding_blockers or evidence.status != "passed" else "passed",
        )
        return _evidence_view(args=args, report=report)


class _RunSpecPaths:
    def __init__(
        self,
        *,
        bundle_path: Path,
        bundle_label: str,
        output_path: Path,
        output_label: str,
        evidence_path: Path,
        evidence_label: str,
        worktree_path: Path,
        worktree_label: str,
    ) -> None:
        self.bundle_path = bundle_path
        self.bundle_label = bundle_label
        self.output_path = output_path
        self.output_label = output_label
        self.evidence_path = evidence_path
        self.evidence_label = evidence_label
        self.worktree_path = worktree_path
        self.worktree_label = worktree_label


class _EvidencePaths:
    def __init__(
        self,
        *,
        run_spec_path: Path,
        run_spec_label: str,
        evidence_path: Path,
        evidence_label: str,
    ) -> None:
        self.run_spec_path = run_spec_path
        self.run_spec_label = run_spec_label
        self.evidence_path = evidence_path
        self.evidence_label = evidence_label


def _resolve_run_spec_paths(args: object) -> tuple[_RunSpecPaths, tuple[GitOpsIssue, ...]]:
    bundle_path, bundle_label, bundle_issue = _safe_path(getattr(args, "bundle_path", None), source="BUNDLE")
    output_path, output_label, output_issue = _safe_path(
        getattr(args, "output_path", ".dpone/gitops/airflow/run-spec.json"),
        source="--output-path",
    )
    evidence_path, evidence_label, evidence_issue = _safe_path(
        getattr(args, "evidence_output", ".dpone/gitops/airflow/runtime-evidence.json"),
        source="--evidence-output",
    )
    worktree_path, worktree_label, worktree_issue = _safe_path(getattr(args, "worktree", "."), source="--worktree")
    paths = _RunSpecPaths(
        bundle_path=bundle_path,
        bundle_label=bundle_label,
        output_path=output_path,
        output_label=output_label,
        evidence_path=evidence_path,
        evidence_label=evidence_label,
        worktree_path=worktree_path,
        worktree_label=worktree_label,
    )
    return paths, tuple(issue for issue in (bundle_issue, output_issue, evidence_issue, worktree_issue) if issue)


def _resolve_evidence_paths(args: object) -> tuple[_EvidencePaths, tuple[GitOpsIssue, ...]]:
    run_spec_path, run_spec_label, run_spec_issue = _safe_path(getattr(args, "run_spec_path", None), source="RUN_SPEC")
    evidence_path, evidence_label, evidence_issue = _safe_path(getattr(args, "evidence_path", None), source="EVIDENCE")
    paths = _EvidencePaths(
        run_spec_path=run_spec_path,
        run_spec_label=run_spec_label,
        evidence_path=evidence_path,
        evidence_label=evidence_label,
    )
    return paths, tuple(issue for issue in (run_spec_issue, evidence_issue) if issue)


def _safe_path(raw_path: object, *, source: str) -> tuple[Path, str, GitOpsIssue | None]:
    try:
        path = safe_relative_path(raw_path, source=source)
    except GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return Path("."), label, GitOpsIssue(code="invalid_path", message=str(exc), path=label, source=source)
    return path, "." if path.as_posix() == "." else path.as_posix(), None


def _load_json(
    *,
    ctx: GitOpsAirflowRuntimeContext,
    repo_root: Path,
    path: Path,
    label: str,
    missing_code: str,
    invalid_code: str,
    source: str,
) -> tuple[object, tuple[GitOpsIssue, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (
            GitOpsIssue(code=missing_code, message="Required JSON artifact does not exist", path=label, source=source),
        )
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (
            GitOpsIssue(
                code=invalid_code,
                message=f"JSON artifact could not be parsed: {exc.msg}",
                path=label,
                source=source,
            ),
        )


def _blocked_run_spec(
    *,
    args: object,
    paths: _RunSpecPaths,
    image: str,
    blockers: tuple[GitOpsIssue, ...],
) -> GitOpsAirflowRunSpec:
    return GitOpsAirflowRunSpec(
        bundle_path=paths.bundle_label,
        bundle_digest=None,
        image=image,
        image_digest=_optional_str(getattr(args, "image_digest", None)),
        worktree=paths.worktree_label,
        evidence_output=paths.evidence_label,
        entries=(),
        steps=(),
        blockers=blockers,
    )


def _blocked_evidence(*, paths: _EvidencePaths, blockers: tuple[GitOpsIssue, ...]) -> GitOpsAirflowRuntimeEvidence:
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


def _finding_issues(findings: tuple[AirflowRuntimeEvidenceFinding, ...]) -> tuple[GitOpsIssue, ...]:
    return tuple(
        GitOpsIssue(
            code=finding.code,
            message=finding.message,
            path=finding.path,
            source="dpone gitops airflow evidence-verify",
        )
        for finding in findings
    )


def _run_spec_view(*, args: object, report: GitOpsAirflowRunSpec) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_run_spec",
            path=getattr(args, "output_path", ".dpone/gitops/airflow/run-spec.json"),
            options={
                "format": getattr(args, "format", "json"),
                "require_attestation": bool(getattr(args, "require_attestation", False)),
            },
        ),
        report=report,
    )


def _evidence_view(*, args: object, report: GitOpsAirflowRuntimeEvidence) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_runtime_evidence",
            path=getattr(args, "evidence_path", None),
            options={
                "format": getattr(args, "format", "json"),
                "require_all_steps": bool(getattr(args, "require_all_steps", False)),
            },
        ),
        report=report,
    )


def _issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source="dpone gitops airflow run-spec")


def _optional_str(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "GitOpsAirflowEvidenceVerifyService",
    "GitOpsAirflowRunSpecService",
    "GitOpsAirflowRuntimeContext",
]
