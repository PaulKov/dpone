from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowConnectionBridgePlanContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsAirflowConnectionBridgePlanService:
    """Build Airflow connection bridge deploy-time skeleton artifacts."""

    def __init__(self, *, ctx: GitOpsAirflowConnectionBridgePlanContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_paths(args)
        runtime_profile, profile_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.runtime_profile_path,
            label=paths.runtime_profile_label,
            invalid_code="airflow_runtime_profile_json_invalid",
        )
        pod_contract, contract_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.pod_contract_path,
            label=paths.pod_contract_label,
            invalid_code="airflow_pod_contract_json_invalid",
        )
        builder = _plan_domain().GitOpsAirflowConnectionBridgePlanBuilder()
        report = builder.build(
            _plan_domain().GitOpsAirflowConnectionBridgePlanInput(
                artifact_dir=paths.artifact_dir_label,
                output_path=paths.output_label,
                runtime_profile_path=paths.runtime_profile_label,
                pod_contract_path=paths.pod_contract_label,
                secret_manifest_path=paths.secret_manifest_label,
                external_secret_path=paths.external_secret_label,
                env_example_path=paths.env_example_label,
                runtime_profile=_mapping(runtime_profile),
                pod_contract=_mapping(pod_contract),
                external_secret_store=_text(getattr(args, "external_secret_store", None)) or "airflow-connections",
                external_secret_store_kind=_text(getattr(args, "external_secret_store_kind", None)) or "SecretStore",
                external_secret_remote_prefix=_text(getattr(args, "external_secret_remote_prefix", None))
                or "airflow/connections",
                blockers=(*path_blockers, *profile_blockers, *contract_blockers),
            )
        )
        if report.passed:
            _write_report_and_artifacts(ctx=self._ctx, repo_root=repo_root, report=report)
        return GitOpsView(
            meta=build_gitops_meta(
                "gitops.airflow_connection_bridge_plan",
                path=report.output_path,
                options={
                    "artifact_dir": report.artifact_dir,
                    "format": getattr(args, "format", "json"),
                },
            ),
            report=report,
        )


class _BridgePlanPaths:
    def __init__(
        self,
        *,
        artifact_dir_path: Path,
        artifact_dir_label: str,
        output_path: Path,
        output_label: str,
        runtime_profile_path: Path,
        runtime_profile_label: str,
        pod_contract_path: Path,
        pod_contract_label: str,
        secret_manifest_path: Path,
        secret_manifest_label: str,
        external_secret_path: Path,
        external_secret_label: str,
        env_example_path: Path,
        env_example_label: str,
    ) -> None:
        self.artifact_dir_path = artifact_dir_path
        self.artifact_dir_label = artifact_dir_label
        self.output_path = output_path
        self.output_label = output_label
        self.runtime_profile_path = runtime_profile_path
        self.runtime_profile_label = runtime_profile_label
        self.pod_contract_path = pod_contract_path
        self.pod_contract_label = pod_contract_label
        self.secret_manifest_path = secret_manifest_path
        self.secret_manifest_label = secret_manifest_label
        self.external_secret_path = external_secret_path
        self.external_secret_label = external_secret_label
        self.env_example_path = env_example_path
        self.env_example_label = env_example_label


def _resolve_paths(args: object) -> tuple[_BridgePlanPaths, tuple[Any, ...]]:
    artifact_dir_path, artifact_dir_label, artifact_issue = _safe_path(
        getattr(args, "artifact_dir", ".dpone/gitops/airflow") or ".dpone/gitops/airflow",
        source="--artifact-dir",
    )
    output_path, output_label, output_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "output_path", None),
        artifact_name="connection-bridge-plan.json",
        source="--output-path",
    )
    runtime_profile_path, runtime_profile_label, profile_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "runtime_profile_path", None),
        artifact_name="runtime-profile.json",
        source="--runtime-profile-path",
    )
    pod_contract_path, pod_contract_label, contract_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "pod_contract_path", None),
        artifact_name="pod-contract.json",
        source="--pod-contract-path",
    )
    secret_path, secret_label, secret_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "secret_manifest_path", None),
        artifact_name="airflow-connections-secret.yaml",
        source="--secret-manifest-path",
    )
    external_path, external_label, external_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "external_secret_path", None),
        artifact_name="airflow-connections-externalsecret.yaml",
        source="--external-secret-path",
    )
    env_path, env_label, env_issue = _artifact_or_explicit_path(
        artifact_dir_path,
        raw_path=getattr(args, "env_example_path", None),
        artifact_name="airflow-connections.env.example",
        source="--env-example-path",
    )
    paths = _BridgePlanPaths(
        artifact_dir_path=artifact_dir_path,
        artifact_dir_label=artifact_dir_label,
        output_path=output_path,
        output_label=output_label,
        runtime_profile_path=runtime_profile_path,
        runtime_profile_label=runtime_profile_label,
        pod_contract_path=pod_contract_path,
        pod_contract_label=pod_contract_label,
        secret_manifest_path=secret_path,
        secret_manifest_label=secret_label,
        external_secret_path=external_path,
        external_secret_label=external_label,
        env_example_path=env_path,
        env_example_label=env_label,
    )
    issues = (
        artifact_issue,
        output_issue,
        profile_issue,
        contract_issue,
        secret_issue,
        external_issue,
        env_issue,
    )
    return paths, tuple(issue for issue in issues if issue is not None)


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
    *,
    ctx: GitOpsAirflowConnectionBridgePlanContext,
    repo_root: Path,
    path: Path,
    label: str,
    invalid_code: str,
) -> tuple[object, tuple[Any, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, ()
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (_issue(code=invalid_code, message=f"JSON artifact could not be parsed: {exc.msg}", path=label),)


def _write_report_and_artifacts(
    *,
    ctx: GitOpsAirflowConnectionBridgePlanContext,
    repo_root: Path,
    report: Any,
) -> None:
    ctx.fs.write_text(repo_root / _safe_relative_path(report.output_path, source="--output-path"), report.to_json())
    for artifact in report.artifacts:
        if artifact.content is not None:
            ctx.fs.write_text(
                repo_root / _safe_relative_path(artifact.path, source="connection_bridge_plan.artifacts[].path"),
                artifact.content,
                encoding="utf-8",
            )


def _safe_path(raw_path: object, *, source: str) -> tuple[Path, str, Any | None]:
    try:
        path = _safe_relative_path(raw_path, source=source)
    except _path_validation_error() as exc:
        label = str(raw_path or "")
        return Path("."), label, _issue(code="invalid_path", message=str(exc), path=label)
    return path, _label(path), None


def _safe_relative_path(raw_path: object, *, source: str) -> Path:
    return _paths_domain().safe_relative_path(raw_path, source=source)


def _path_validation_error() -> type[Exception]:
    return _paths_domain().GitOpsPathValidationError


def _label(path: Path) -> str:
    return "." if path.as_posix() == "." else path.as_posix()


def _mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _text(value: object) -> str:
    return str(value or "").strip()


def _issue(*, code: str, message: str, path: str) -> Any:
    source = _plan_domain().AIRFLOW_CONNECTION_BRIDGE_PLAN_SOURCE
    return _models_domain().GitOpsIssue(code=code, message=message, path=path, source=source)


def _plan_domain() -> Any:
    return import_module("dpone.gitops.airflow_connection_bridge_plan")


def _paths_domain() -> Any:
    return import_module("dpone.gitops.paths")


def _models_domain() -> Any:
    return import_module("dpone.gitops.models")


__all__ = ["GitOpsAirflowConnectionBridgePlanContext", "GitOpsAirflowConnectionBridgePlanService"]
