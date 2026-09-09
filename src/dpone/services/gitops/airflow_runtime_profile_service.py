from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_artifacts import GitOpsAirflowArtifactRenderer
from dpone.gitops.airflow_git_sync_artifacts import GitOpsAirflowGitSyncArtifactCollector
from dpone.gitops.airflow_json_artifacts import load_gitops_json_artifact
from dpone.gitops.airflow_runtime_profile import (
    GitOpsAirflowConnectionBridgeBuilder,
    GitOpsAirflowRuntimeProfileBuilder,
    GitOpsAirflowRuntimeProfileInput,
)
from dpone.gitops.airflow_runtime_profile_git_sync import GitOpsAirflowRuntimeProfileGitSyncBuilder
from dpone.gitops.airflow_runtime_profile_options import parse_runtime_profile_options
from dpone.gitops.airflow_runtime_profile_paths import (
    GitOpsAirflowRuntimeProfilePaths,
    resolve_runtime_profile_paths,
)
from dpone.gitops.airflow_runtime_profile_policy import GitOpsAirflowRuntimeProfilePolicy
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowRuntimeProfileContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class GitOpsAirflowRuntimeProfileService:
    """Build the Airflow runtime placement profile and XCom handoff artifacts."""

    def __init__(self, *, ctx: GitOpsAirflowRuntimeProfileContext) -> None:
        self._ctx = ctx
        self._builder = GitOpsAirflowRuntimeProfileBuilder()
        self._policy = GitOpsAirflowRuntimeProfilePolicy()
        self._renderer = GitOpsAirflowArtifactRenderer()
        self._git_sync = GitOpsAirflowRuntimeProfileGitSyncBuilder(
            collector=GitOpsAirflowGitSyncArtifactCollector(fs=ctx.fs)
        )
        self._connection_bridge = GitOpsAirflowConnectionBridgeBuilder(fs=ctx.fs, yaml=ctx.yaml)

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = resolve_runtime_profile_paths(args)
        parsed, parse_blockers = parse_runtime_profile_options(args)
        bundle, bundle_blockers, run_spec, run_spec_blockers = _load_inputs(
            ctx=self._ctx,
            repo_root=repo_root,
            paths=paths,
            enabled=not path_blockers,
        )
        git_sync, git_sync_blockers = self._git_sync.build(
            args=args,
            repo_root=repo_root,
            bundle=_mapping(bundle),
            control_paths=(paths.bundle_label, paths.run_spec_label),
            enabled=not path_blockers and not bundle_blockers,
        )
        connection_bridge, bridge_warnings, bridge_blockers = self._connection_bridge.build(
            args=args,
            repo_root=repo_root,
            bundle=_mapping(bundle),
            enabled=not path_blockers and not bundle_blockers,
        )
        profile = self._builder.build(
            GitOpsAirflowRuntimeProfileInput(
                bundle_path=paths.bundle_label,
                run_spec_path=paths.run_spec_label,
                runtime_evidence_path=paths.runtime_evidence_label,
                xcom_summary_path=paths.xcom_summary_label,
                dag_factory_path=paths.dag_factory_label,
                outcome_gate_path=paths.outcome_gate_label,
                image=parsed.image,
                image_digest=parsed.image_digest,
                namespace=parsed.namespace,
                service_account=parsed.service_account,
                resource_requests=parsed.resource_requests,
                resource_limits=parsed.resource_limits,
                artifact_sink_kind=parsed.artifact_sink_kind,
                artifact_sink_path=parsed.artifact_sink_path,
                runner_policy=parsed.runner_policy,
                env=parsed.env,
                labels=parsed.labels,
                annotations=parsed.annotations,
                git_sync=git_sync,
                connection_bridge=connection_bridge,
                outcome_mode=parsed.outcome_mode,
                bundle=_mapping(bundle),
                run_spec=_mapping(run_spec),
                blockers=(
                    *path_blockers,
                    *parse_blockers,
                    *bundle_blockers,
                    *run_spec_blockers,
                    *git_sync_blockers,
                    *bridge_blockers,
                ),
                warnings=bridge_warnings,
            )
        )
        policy_warnings, policy_blockers = self._policy.evaluate(profile)
        profile = profile.with_issues(
            warnings=(*profile.warnings, *policy_warnings),
            blockers=(*profile.blockers, *policy_blockers),
        )
        if profile.passed:
            self._write_artifacts(repo_root=repo_root, paths=paths, profile=profile)
        return _view(args=args, report=profile, path=paths.output_label)

    def _write_artifacts(self, *, repo_root: Path, paths: GitOpsAirflowRuntimeProfilePaths, profile: Any) -> None:
        xcom_summary = self._builder.xcom_summary(profile, runtime_profile_path=paths.output_label)
        self._ctx.fs.write_text(repo_root / paths.output_path, profile.to_json(), encoding="utf-8")
        self._ctx.fs.write_text(repo_root / paths.xcom_summary_path, xcom_summary.to_json(), encoding="utf-8")
        self._ctx.fs.write_text(
            repo_root / paths.dag_factory_path,
            self._renderer.dag_factory(
                task_id="dpone_gitops_runtime",
                image=profile.image,
                namespace=profile.namespace,
                service_account=profile.service_account,
                run_spec_path=profile.run_spec_path,
                runtime_profile_path=paths.output_label,
                runtime_evidence_path=profile.runtime_evidence_path,
                xcom_summary_path=profile.xcom_summary_path,
                outcome_mode=profile.outcome_mode,
                env=profile.env,
                labels=profile.labels or {},
                annotations=profile.annotations or {},
            ),
            encoding="utf-8",
        )
        self._ctx.fs.write_text(
            repo_root / paths.outcome_gate_path,
            self._renderer.outcome_gate_module(
                upstream_task_id="dpone_gitops_runtime",
                required_status="passed",
            ),
            encoding="utf-8",
        )


def _load_inputs(
    *,
    ctx: GitOpsAirflowRuntimeProfileContext,
    repo_root: Path,
    paths: GitOpsAirflowRuntimeProfilePaths,
    enabled: bool,
) -> tuple[object, tuple[Any, ...], object, tuple[Any, ...]]:
    if not enabled:
        return None, (), None, ()
    bundle, bundle_blockers = load_gitops_json_artifact(
        fs=ctx.fs,
        repo_root=repo_root,
        path=paths.bundle_path,
        label=paths.bundle_label,
        missing_code="bundle_missing",
        invalid_code="bundle_json_invalid",
        source="dpone gitops airflow runtime-profile",
    )
    run_spec, run_spec_blockers = load_gitops_json_artifact(
        fs=ctx.fs,
        repo_root=repo_root,
        path=paths.run_spec_path,
        label=paths.run_spec_label,
        missing_code="run_spec_missing",
        invalid_code="run_spec_json_invalid",
        source="dpone gitops airflow runtime-profile",
    )
    return bundle, bundle_blockers, run_spec, run_spec_blockers


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


def _optional_text(value: object) -> str | None:
    text = _text(value)
    return text or None


def _view(*, args: object, report: Any, path: str) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_runtime_profile",
            path=path,
            options={
                "format": getattr(args, "format", "json"),
                "runner_policy": getattr(args, "runner_policy", "advisory"),
            },
        ),
        report=report,
    )


__all__ = [
    "GitOpsAirflowRuntimeProfileContext",
    "GitOpsAirflowRuntimeProfileService",
]
