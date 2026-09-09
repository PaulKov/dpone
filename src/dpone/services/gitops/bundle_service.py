from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import Protocol

from dpone.gitops.bundle_attestation import BundleAttestationPayload, GitOpsBundleAttestationBuilder
from dpone.gitops.bundle_policy import GitOpsBundlePolicyBlocker, GitOpsBundlePolicyEvaluator
from dpone.gitops.bundle_profiles import bundle_policy_profile_names, resolve_bundle_policy_defaults
from dpone.gitops.models import (
    GitOpsAffectedReport,
    GitOpsBundleArtifactDigest,
    GitOpsBundleAttestation,
    GitOpsBundleEntry,
    GitOpsBundlePolicy,
    GitOpsBundleReport,
    GitOpsIssue,
    GitOpsVerifyReport,
)
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.gitops.rendering import render_gitops_bundle_markdown
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.affected_service import GitOpsAffectedService
from dpone.services.gitops.verify_service import GitOpsVerifyService
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsBundleContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class GitOpsBundleService:
    """Builds deterministic GitOps handoff bundles for schedulers and CI gates."""

    def __init__(self, *, ctx: GitOpsBundleContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        output_dir, output_label, output_blocker = _resolve_output_dir(args=args)
        policy = _bundle_policy(args)
        if output_blocker is not None:
            report = _blocked_report(output_label=output_label, policy=policy, blocker=output_blocker)
            return _bundle_view(args=args, report=report)

        affected_report = self._build_affected_report(args=args, output_label=output_label)
        affected_path = output_dir / "affected.json"
        self._ctx.fs.write_text(repo_root / affected_path, affected_report.to_json(), encoding="utf-8")

        entries, verify_warnings, verify_blockers = self._build_entries(
            args=args,
            policy=policy,
            repo_root=repo_root,
            affected_report=affected_report,
        )
        warnings = (*affected_report.warnings, *verify_warnings)
        blockers = (
            *affected_report.blockers,
            *verify_blockers,
            *_policy_profile_blockers(policy),
            *_to_issues(
                GitOpsBundlePolicyEvaluator().evaluate(
                    affected=affected_report,
                    policy=policy,
                    warnings=warnings,
                )
            ),
        )
        report = GitOpsBundleReport(
            output_dir=output_label,
            affected_path=affected_path.as_posix(),
            summary_path=(output_dir / "summary.md").as_posix(),
            entries=entries,
            policy=policy,
            warnings=warnings,
            blockers=blockers,
        )
        if bool(getattr(args, "attest", False)):
            report = self._with_attestation(
                args=args,
                repo_root=repo_root,
                report=report,
                affected_report=affected_report,
            )
        self._write_report_artifacts(repo_root=repo_root, output_dir=output_dir, report=report)
        return _bundle_view(args=args, report=report)

    def _build_affected_report(self, *, args: object, output_label: str) -> GitOpsAffectedReport:
        affected_args = _copy_args(
            args,
            emit_plans=True,
            output_dir=f"{output_label}/manifests",
            output=None,
            format="json",
        )
        view = GitOpsAffectedService(ctx=self._ctx).build_view(affected_args)
        return view.report

    def _build_entries(
        self,
        *,
        args: object,
        policy: GitOpsBundlePolicy,
        repo_root: Path,
        affected_report: GitOpsAffectedReport,
    ) -> tuple[tuple[GitOpsBundleEntry, ...], tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
        entries: list[GitOpsBundleEntry] = []
        warnings: list[GitOpsIssue] = []
        blockers: list[GitOpsIssue] = []
        for impacted in affected_report.impacted_manifests:
            if impacted.emitted_plan is None:
                blockers.append(
                    GitOpsIssue(
                        code="plan_not_emitted",
                        message="Impacted manifest did not have an emitted GitOps plan",
                        path=impacted.manifest,
                        source="dpone gitops bundle",
                    )
                )
                continue
            verify_path = Path(impacted.emitted_plan).parent / "gitops_verify.json"
            verify_report = self._build_verify_report(args=args, policy=policy, plan=impacted.emitted_plan)
            self._ctx.fs.write_text(repo_root / verify_path, verify_report.to_json(), encoding="utf-8")
            entries.append(
                GitOpsBundleEntry(
                    manifest=impacted.manifest,
                    plan_path=impacted.emitted_plan,
                    verify_path=verify_path.as_posix(),
                    passed=verify_report.passed,
                )
            )
            warnings.extend(verify_report.warnings)
            blockers.extend(verify_report.blockers)
        return tuple(entries), tuple(warnings), tuple(blockers)

    def _build_verify_report(self, *, args: object, policy: GitOpsBundlePolicy, plan: str) -> GitOpsVerifyReport:
        verify_args = Namespace(
            plan=plan,
            worktree=getattr(args, "worktree", "."),
            verify_lock=policy.verify_lock,
            output=None,
            format="json",
        )
        report: GitOpsVerifyReport = GitOpsVerifyService(ctx=self._ctx).build_view(verify_args).report
        return report

    def _write_report_artifacts(self, *, repo_root: Path, output_dir: Path, report: GitOpsBundleReport) -> None:
        self._ctx.fs.write_text(repo_root / output_dir / "bundle.json", report.to_json(), encoding="utf-8")
        if report.attestation is not None:
            return
        self._ctx.fs.write_text(
            repo_root / output_dir / "summary.md",
            render_gitops_bundle_markdown(report),
            encoding="utf-8",
        )

    def _with_attestation(
        self,
        *,
        args: object,
        repo_root: Path,
        report: GitOpsBundleReport,
        affected_report: GitOpsAffectedReport,
    ) -> GitOpsBundleReport:
        self._ctx.fs.write_text(
            repo_root / report.summary_path, render_gitops_bundle_markdown(report), encoding="utf-8"
        )
        artifact_paths = (
            report.affected_path,
            *(entry.plan_path for entry in report.entries),
            *(entry.verify_path for entry in report.entries),
            report.summary_path,
        )
        payload = GitOpsBundleAttestationBuilder().build(
            repo_root=repo_root,
            artifact_paths=artifact_paths,
            provenance=_attestation_provenance(args=args, report=report, affected_report=affected_report),
        )
        return GitOpsBundleReport(
            output_dir=report.output_dir,
            affected_path=report.affected_path,
            summary_path=report.summary_path,
            entries=report.entries,
            policy=report.policy,
            warnings=report.warnings,
            blockers=report.blockers,
            attestation=_to_attestation(payload),
        )


def _resolve_output_dir(args: object) -> tuple[Path, str, GitOpsIssue | None]:
    raw_output_dir = getattr(args, "output_dir", ".dpone/gitops/bundle")
    try:
        output_dir = safe_relative_path(raw_output_dir, source="--output-dir")
    except GitOpsPathValidationError as exc:
        return (
            Path("."),
            str(raw_output_dir),
            GitOpsIssue(code="invalid_path", message=str(exc), path=str(raw_output_dir), source="--output-dir"),
        )
    output_label = "." if output_dir.as_posix() == "." else output_dir.as_posix()
    return output_dir, output_label, None


def _bundle_policy(args: object) -> GitOpsBundlePolicy:
    raw_profile = getattr(args, "policy_profile", "custom")
    defaults = resolve_bundle_policy_defaults(raw_profile)
    if defaults is None:
        profile = str(raw_profile or "custom").strip().lower()
        return GitOpsBundlePolicy(profile=profile)
    return GitOpsBundlePolicy(
        profile=defaults.profile,
        verify_lock=defaults.verify_lock or bool(getattr(args, "verify_lock", False)),
        fail_on_empty_impact=defaults.fail_on_empty_impact or bool(getattr(args, "fail_on_empty_impact", False)),
        fail_on_warnings=defaults.fail_on_warnings or bool(getattr(args, "fail_on_warnings", False)),
        require_lock=defaults.require_lock or bool(getattr(args, "require_lock", False)),
    )


def _blocked_report(*, output_label: str, policy: GitOpsBundlePolicy, blocker: GitOpsIssue) -> GitOpsBundleReport:
    return GitOpsBundleReport(
        output_dir=output_label,
        affected_path=f"{output_label}/affected.json",
        summary_path=f"{output_label}/summary.md",
        entries=(),
        policy=policy,
        blockers=(blocker,),
    )


def _bundle_view(*, args: object, report: GitOpsBundleReport) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.bundle",
            path=report.output_dir,
            options={
                "format": getattr(args, "format", "json"),
                "output_dir": report.output_dir,
                "verify_lock": bool(getattr(args, "verify_lock", False)),
                "fail_on_empty_impact": bool(getattr(args, "fail_on_empty_impact", False)),
                "fail_on_warnings": bool(getattr(args, "fail_on_warnings", False)),
                "require_lock": bool(getattr(args, "require_lock", False)),
                "policy_profile": report.policy.profile,
                "attest": bool(getattr(args, "attest", False)),
            },
        ),
        report=report,
    )


def _copy_args(args: object, **overrides: object) -> Namespace:
    data = dict(vars(args))
    data.update(overrides)
    return Namespace(**data)


def _to_issues(blockers: tuple[GitOpsBundlePolicyBlocker, ...]) -> tuple[GitOpsIssue, ...]:
    return tuple(
        GitOpsIssue(code=blocker.code, message=blocker.message, path=blocker.path, source=blocker.source)
        for blocker in blockers
    )


def _policy_profile_blockers(policy: GitOpsBundlePolicy) -> tuple[GitOpsIssue, ...]:
    if policy.profile in bundle_policy_profile_names():
        return ()
    return (
        GitOpsIssue(
            code="invalid_policy_profile",
            message=f"Unknown GitOps bundle policy profile: {policy.profile}",
            path=policy.profile,
            source="--policy-profile",
        ),
    )


def _attestation_provenance(
    *,
    args: object,
    report: GitOpsBundleReport,
    affected_report: GitOpsAffectedReport,
) -> dict[str, object]:
    return {
        "producer": "dpone gitops bundle",
        "schema_version": "1",
        "from_ref": getattr(args, "from_ref", None),
        "to_ref": getattr(args, "to_ref", None),
        "changed_files": list(affected_report.changed_files),
        "changed_files_file": getattr(args, "changed_files_file", None),
        "runner": getattr(args, "runner", "generic"),
        "policy_profile": report.policy.profile,
        "output_dir": report.output_dir,
    }


def _to_attestation(payload: BundleAttestationPayload) -> GitOpsBundleAttestation:
    return GitOpsBundleAttestation(
        bundle_digest=payload.bundle_digest,
        provenance=payload.provenance,
        artifacts=tuple(
            GitOpsBundleArtifactDigest(path=artifact.path, sha256=artifact.sha256, bytes=artifact.bytes)
            for artifact in payload.artifacts
        ),
    )


__all__ = ["GitOpsBundleContext", "GitOpsBundleService"]
