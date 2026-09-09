from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

from dpone.gitops.bundle_verify import (
    GitOpsBundleAttestationCheck,
    GitOpsBundleSchemaCheck,
    GitOpsBundleVerifier,
    GitOpsBundleVerifyReport,
)
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsBundleVerifyContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsBundleVerifyService:
    """Verifies an emitted GitOps bundle and optional attestation offline."""

    def __init__(self, *, ctx: GitOpsBundleVerifyContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        require_attestation = bool(getattr(args, "require_attestation", False))
        raw_bundle_path = getattr(args, "bundle_path", None)
        if not raw_bundle_path:
            report = _blocked_report(
                bundle_path="",
                require_attestation=require_attestation,
                blocker=GitOpsIssue(
                    code="bundle_path_missing",
                    message="Bundle verify requires a repo-relative bundle JSON path",
                    path="PATH",
                    source="dpone gitops bundle verify",
                ),
            )
            return _verify_view(args=args, report=report)

        try:
            bundle_path = safe_relative_path(raw_bundle_path, source="PATH")
        except GitOpsPathValidationError as exc:
            report = _blocked_report(
                bundle_path=str(raw_bundle_path),
                require_attestation=require_attestation,
                blocker=GitOpsIssue(
                    code="invalid_bundle_path",
                    message=str(exc),
                    path=str(raw_bundle_path),
                    source="dpone gitops bundle verify",
                ),
            )
            return _verify_view(args=args, report=report)

        bundle_label = bundle_path.as_posix()
        full_path = repo_root / bundle_path
        if not self._ctx.fs.exists(full_path):
            report = _blocked_report(
                bundle_path=bundle_label,
                require_attestation=require_attestation,
                blocker=GitOpsIssue(
                    code="bundle_missing",
                    message="Bundle JSON does not exist",
                    path=bundle_label,
                    source="dpone gitops bundle verify",
                ),
            )
            return _verify_view(args=args, report=report)

        try:
            payload = json.loads(self._ctx.fs.read_text(full_path, encoding="utf-8"))
        except json.JSONDecodeError as exc:
            report = _blocked_report(
                bundle_path=bundle_label,
                require_attestation=require_attestation,
                blocker=GitOpsIssue(
                    code="bundle_json_invalid",
                    message=f"Bundle JSON could not be parsed: {exc.msg}",
                    path=bundle_label,
                    source="dpone gitops bundle verify",
                ),
            )
            return _verify_view(args=args, report=report)

        schema_issues = GitOpsSchemaValidator().validate(payload, expected_kind="gitops.bundle")
        schema_check = GitOpsBundleSchemaCheck(
            expected_kind="gitops.bundle",
            actual_kind=_actual_kind(payload),
            passed=not schema_issues,
            issues=schema_issues,
        )
        if not isinstance(payload, Mapping):
            report = GitOpsBundleVerifyReport(
                bundle_path=bundle_label,
                schema_check=schema_check,
                attestation_check=_empty_attestation_check(require_attestation=require_attestation),
                blockers=schema_issues,
            )
            return _verify_view(args=args, report=report)

        result = GitOpsBundleVerifier().verify(
            repo_root=repo_root,
            bundle=payload,
            require_attestation=require_attestation,
        )
        report = GitOpsBundleVerifyReport(
            bundle_path=bundle_label,
            schema_check=schema_check,
            attestation_check=result.attestation_check,
            artifact_checks=result.artifact_checks,
            warnings=result.warnings,
            blockers=(*schema_issues, *result.blockers),
        )
        return _verify_view(args=args, report=report)


def _blocked_report(
    *,
    bundle_path: str,
    require_attestation: bool,
    blocker: GitOpsIssue,
) -> GitOpsBundleVerifyReport:
    return GitOpsBundleVerifyReport(
        bundle_path=bundle_path,
        schema_check=GitOpsBundleSchemaCheck(
            expected_kind="gitops.bundle",
            actual_kind=None,
            passed=False,
            issues=(blocker,),
        ),
        attestation_check=_empty_attestation_check(require_attestation=require_attestation),
        blockers=(blocker,),
    )


def _empty_attestation_check(*, require_attestation: bool) -> GitOpsBundleAttestationCheck:
    return GitOpsBundleAttestationCheck(
        present=False,
        required=require_attestation,
        expected_bundle_digest=None,
        actual_bundle_digest=None,
        passed=not require_attestation,
    )


def _actual_kind(payload: object) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("kind")
    return value if isinstance(value, str) else None


def _verify_view(*, args: object, report: GitOpsBundleVerifyReport) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.bundle_verify",
            path=report.bundle_path or None,
            options={
                "format": getattr(args, "format", "json"),
                "require_attestation": bool(getattr(args, "require_attestation", False)),
            },
        ),
        report=report,
    )


__all__ = ["GitOpsBundleVerifyContext", "GitOpsBundleVerifyService"]
