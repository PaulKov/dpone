from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_doctor import GitOpsAirflowDoctor
from dpone.gitops.airflow_models import GitOpsAirflowArtifact, GitOpsAirflowRenderReport
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.airflow_render_catalog import (
    airflow_render_artifact_paths,
    run_spec_exec_command,
)
from dpone.services.gitops.airflow_render_writer import GitOpsAirflowArtifactWriter
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowRenderContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class GitOpsAirflowRenderService:
    """Renders deterministic Airflow/Kubernetes artifacts from a GitOps bundle."""

    def __init__(self, *, ctx: GitOpsAirflowRenderContext) -> None:
        self._ctx = ctx
        self._doctor = GitOpsAirflowDoctor()
        self._writer = GitOpsAirflowArtifactWriter(ctx=ctx)

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        bundle_path, bundle_label, bundle_blocker = _resolve_path(args, attr="bundle_path", source="BUNDLE")
        output_dir, output_label, output_blocker = _resolve_output_dir(args)
        image_contract_path, image_contract_label, image_contract_blocker = _resolve_image_contract(args, output_dir)
        image = str(getattr(args, "image", "") or "").strip()
        blockers = tuple(
            issue
            for issue in (
                bundle_blocker,
                output_blocker,
                image_contract_blocker,
                _image_blocker(image),
            )
            if issue is not None
        )
        if blockers:
            report = GitOpsAirflowRenderReport(
                bundle_path=bundle_label,
                output_dir=output_label,
                image=image,
                artifacts=(),
                commands=(),
                blockers=blockers,
            )
            return _view(args=args, report=report)

        bundle, bundle_warnings, bundle_blockers = _load_bundle(
            ctx=self._ctx,
            repo_root=repo_root,
            bundle_path=bundle_path,
            bundle_label=bundle_label,
        )
        bundle_checks, doctor_warnings, doctor_blockers = self._doctor.validate_bundle(
            bundle_path=bundle_label,
            bundle=bundle,
            require_attestation=bool(getattr(args, "require_attestation", False)),
        )
        _ = bundle_checks
        warnings = (*bundle_warnings, *doctor_warnings)
        blockers = (*bundle_blockers, *doctor_blockers)
        if blockers:
            report = GitOpsAirflowRenderReport(
                bundle_path=bundle_label,
                output_dir=output_label,
                image=image,
                artifacts=(),
                commands=(),
                warnings=warnings,
                blockers=blockers,
            )
            return _view(args=args, report=report)

        bundle_mapping = _as_mapping(bundle)
        run_spec_path = output_dir / "run-spec.json"
        evidence_path = output_dir / "runtime-evidence.json"
        runtime_profile_path = output_dir / "runtime-profile.json"
        xcom_summary_path = output_dir / "xcom-summary.json"
        dag_factory_path = output_dir / "airflow_dag_factory.py"
        outcome_gate_path = output_dir / "outcome_gate.py"
        artifact_paths = airflow_render_artifact_paths(
            output_dir=output_dir,
            image_contract_path=image_contract_path,
            run_spec_path=run_spec_path,
            evidence_path=evidence_path,
            runtime_profile_path=runtime_profile_path,
            xcom_summary_path=xcom_summary_path,
            dag_factory_path=dag_factory_path,
            outcome_gate_path=outcome_gate_path,
        )
        commands = (
            run_spec_exec_command(
                run_spec_path=run_spec_path,
                evidence_path=evidence_path,
                xcom_path=xcom_summary_path,
            ),
        )
        self._writer.write(
            args=args,
            repo_root=repo_root,
            bundle_path=bundle_label,
            bundle=bundle_mapping,
            output_dir=output_dir,
            image_contract_path=image_contract_path,
            run_spec_path=run_spec_path,
            evidence_path=evidence_path,
            runtime_profile_path=runtime_profile_path,
            xcom_summary_path=xcom_summary_path,
            dag_factory_path=dag_factory_path,
            outcome_gate_path=outcome_gate_path,
            image=image,
            commands=commands,
        )
        artifacts = _artifacts(ctx=self._ctx, repo_root=repo_root, paths=artifact_paths)
        report = GitOpsAirflowRenderReport(
            bundle_path=bundle_label,
            output_dir=output_label,
            image=image,
            artifacts=artifacts,
            commands=commands,
            warnings=warnings,
        )
        return _view(args=args, report=report)


def _resolve_path(args: object, *, attr: str, source: str) -> tuple[Path, str, GitOpsIssue | None]:
    raw_path = getattr(args, attr, None)
    try:
        path = safe_relative_path(raw_path, source=source)
    except GitOpsPathValidationError as exc:
        return (
            Path("."),
            str(raw_path or ""),
            GitOpsIssue(
                code="invalid_path",
                message=str(exc),
                path=str(raw_path or ""),
                source=source,
            ),
        )
    return path, path.as_posix(), None


def _resolve_output_dir(args: object) -> tuple[Path, str, GitOpsIssue | None]:
    raw_output_dir = getattr(args, "output_dir", ".dpone/gitops/airflow")
    try:
        output_dir = safe_relative_path(raw_output_dir, source="--output-dir")
    except GitOpsPathValidationError as exc:
        return (
            Path("."),
            str(raw_output_dir),
            GitOpsIssue(
                code="invalid_path",
                message=str(exc),
                path=str(raw_output_dir),
                source="--output-dir",
            ),
        )
    return output_dir, "." if output_dir.as_posix() == "." else output_dir.as_posix(), None


def _resolve_image_contract(args: object, output_dir: Path) -> tuple[Path, str, GitOpsIssue | None]:
    raw_path = getattr(args, "image_contract", None) or (output_dir / "image-contract.json").as_posix()
    try:
        path = safe_relative_path(raw_path, source="--image-contract")
    except GitOpsPathValidationError as exc:
        return (
            Path("."),
            str(raw_path),
            GitOpsIssue(
                code="invalid_path",
                message=str(exc),
                path=str(raw_path),
                source="--image-contract",
            ),
        )
    return path, path.as_posix(), None


def _load_bundle(
    *,
    ctx: GitOpsAirflowRenderContext,
    repo_root: Path,
    bundle_path: Path,
    bundle_label: str,
) -> tuple[object, tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
    full_path = repo_root / bundle_path
    if not ctx.fs.exists(full_path):
        return (
            None,
            (),
            (
                GitOpsIssue(
                    code="bundle_missing",
                    message="GitOps bundle JSON does not exist",
                    path=bundle_label,
                    source="dpone gitops airflow render",
                ),
            ),
        )
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), (), ()
    except json.JSONDecodeError as exc:
        return (
            None,
            (),
            (
                GitOpsIssue(
                    code="bundle_json_invalid",
                    message=f"GitOps bundle JSON could not be parsed: {exc.msg}",
                    path=bundle_label,
                    source="dpone gitops airflow render",
                ),
            ),
        )


def _image_blocker(image: str) -> GitOpsIssue | None:
    if image:
        return None
    return GitOpsIssue(
        code="image_required",
        message="Airflow runner rendering requires --image",
        path="--image",
        source="dpone gitops airflow render",
    )


def _artifacts(
    *,
    ctx: GitOpsAirflowRenderContext,
    repo_root: Path,
    paths: tuple[tuple[Path, str, str, bool], ...],
) -> tuple[GitOpsAirflowArtifact, ...]:
    return tuple(
        GitOpsAirflowArtifact(
            path=path.as_posix(),
            kind=kind,
            required=required,
            exists=ctx.fs.exists(repo_root / path),
            reason=reason,
        )
        for path, kind, reason, required in paths
    )


def _as_mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _view(*, args: object, report: GitOpsAirflowRenderReport) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_render",
            path=report.output_dir,
            options={
                "format": getattr(args, "format", "json"),
                "require_attestation": bool(getattr(args, "require_attestation", False)),
            },
        ),
        report=report,
    )


__all__ = ["GitOpsAirflowRenderContext", "GitOpsAirflowRenderService"]
