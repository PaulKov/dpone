from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.views import GitOpsView, build_gitops_meta

_SOURCE = "dpone gitops airflow k8s-manifests"


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowK8sManifestsContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class GitOpsAirflowK8sManifestsService:
    """Render Kubernetes manifests from generated Airflow runtime artifacts."""

    def __init__(self, *, ctx: GitOpsAirflowK8sManifestsContext) -> None:
        self._ctx = ctx
        self._builder = _builder_domain().GitOpsAirflowK8sManifestBuilder()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_paths(args)
        if path_blockers:
            return _view(_blocked_report(paths=paths, blockers=path_blockers))

        runtime_profile, profile_blockers = _load_json(
            self._ctx,
            repo_root,
            paths.runtime_profile_path,
            paths.runtime_profile_label,
            "airflow_k8s_manifests_runtime_profile_missing",
            "airflow_k8s_manifests_runtime_profile_json_invalid",
        )
        pod_contract, pod_blockers = _load_json(
            self._ctx,
            repo_root,
            paths.pod_contract_path,
            paths.pod_contract_label,
            "airflow_k8s_manifests_pod_contract_missing",
            "airflow_k8s_manifests_pod_contract_json_invalid",
        )
        bridge_plan, bridge_blockers = _load_optional_json(
            self._ctx,
            repo_root,
            paths.connection_bridge_plan_path,
            paths.connection_bridge_plan_label,
            explicit=paths.connection_bridge_plan_explicit,
        )
        blockers = (*profile_blockers, *pod_blockers, *bridge_blockers)
        if blockers:
            return _view(_blocked_report(paths=paths, blockers=blockers))

        report = self._builder.build(
            artifact_dir=paths.artifact_dir_label,
            manifest_path=paths.manifest_output_label,
            runtime_profile_path=paths.runtime_profile_label,
            runtime_profile=_mapping(runtime_profile),
            pod_contract_path=paths.pod_contract_label,
            pod_contract=_mapping(pod_contract),
            connection_bridge_plan_path=paths.connection_bridge_plan_label if bridge_plan is not None else None,
            connection_bridge_plan=_mapping(bridge_plan) if bridge_plan is not None else None,
            include_network_policy=bool(getattr(args, "include_network_policy", False)),
            gitops_controller=str(getattr(args, "gitops_controller", "plain")),
        )
        if report.passed:
            self._ctx.fs.write_text(
                repo_root / paths.manifest_output_path,
                _render_yaml_documents(report.documents, self._ctx.yaml),
                encoding="utf-8",
            )
        return _view(report)


class _K8sManifestPaths:
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
        manifest_output_path: Path,
        manifest_output_label: str,
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
        self.manifest_output_path = manifest_output_path
        self.manifest_output_label = manifest_output_label


def _resolve_paths(args: object) -> tuple[_K8sManifestPaths, tuple[Any, ...]]:
    artifact_dir, artifact_label, artifact_issue = _safe_path(
        getattr(args, "artifact_dir", ".dpone/gitops/airflow") or ".dpone/gitops/airflow",
        "--artifact-dir",
    )
    profile_path, profile_label, profile_issue = _artifact_or_explicit(
        artifact_dir,
        raw_path=getattr(args, "runtime_profile_path", None),
        artifact_name="runtime-profile.json",
        source="--runtime-profile-path",
    )
    pod_path, pod_label, pod_issue = _artifact_or_explicit(
        artifact_dir,
        raw_path=getattr(args, "pod_contract_path", None),
        artifact_name="pod-contract.json",
        source="--pod-contract-path",
    )
    bridge_raw = getattr(args, "connection_bridge_plan_path", None)
    bridge_path, bridge_label, bridge_issue = _artifact_or_explicit(
        artifact_dir,
        raw_path=bridge_raw,
        artifact_name="connection-bridge-plan.json",
        source="--connection-bridge-plan-path",
    )
    output_path, output_label, output_issue = _safe_path(
        getattr(args, "manifest_output", ".dpone/gitops/airflow/airflow-k8s-manifests.yaml"),
        "--manifest-output",
    )
    paths = _K8sManifestPaths(
        artifact_dir_path=artifact_dir,
        artifact_dir_label=artifact_label,
        runtime_profile_path=profile_path,
        runtime_profile_label=profile_label,
        pod_contract_path=pod_path,
        pod_contract_label=pod_label,
        connection_bridge_plan_path=bridge_path,
        connection_bridge_plan_label=bridge_label,
        connection_bridge_plan_explicit=bool(bridge_raw),
        manifest_output_path=output_path,
        manifest_output_label=output_label,
    )
    issues = (artifact_issue, profile_issue, pod_issue, bridge_issue, output_issue)
    return paths, tuple(issue for issue in issues if issue)


def _artifact_or_explicit(
    artifact_dir: Path,
    *,
    raw_path: object,
    artifact_name: str,
    source: str,
) -> tuple[Path, str, Any | None]:
    if raw_path:
        return _safe_path(raw_path, source)
    path = artifact_dir / artifact_name
    return path, _label(path), None


def _load_json(
    ctx: GitOpsAirflowK8sManifestsContext,
    repo_root: Path,
    path: Path,
    label: str,
    missing_code: str,
    invalid_code: str,
) -> tuple[object, tuple[Any, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None, (_issue(missing_code, "Required JSON artifact is missing", label),)
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (_issue(invalid_code, f"JSON artifact could not be parsed: {exc.msg}", label),)


def _load_optional_json(
    ctx: GitOpsAirflowK8sManifestsContext,
    repo_root: Path,
    path: Path,
    label: str,
    *,
    explicit: bool,
) -> tuple[object | None, tuple[Any, ...]]:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        if explicit:
            return None, (
                _issue(
                    "airflow_k8s_manifests_connection_bridge_plan_missing",
                    "Explicit connection bridge plan is missing",
                    label,
                ),
            )
        return None, ()
    try:
        return json.loads(ctx.fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (
            _issue(
                "airflow_k8s_manifests_connection_bridge_plan_json_invalid",
                f"JSON artifact could not be parsed: {exc.msg}",
                label,
            ),
        )


def _blocked_report(*, paths: _K8sManifestPaths, blockers: tuple[Any, ...]) -> Any:
    return _models_domain().GitOpsAirflowK8sManifestsReport(
        artifact_dir=paths.artifact_dir_label,
        manifest_path=paths.manifest_output_label,
        runtime_profile_path=paths.runtime_profile_label,
        pod_contract_path=paths.pod_contract_label,
        connection_bridge_plan_path=paths.connection_bridge_plan_label,
        namespace="",
        service_account="",
        objects=(),
        blockers=blockers,
    )


def _view(report: Any) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta("gitops.airflow_k8s_manifests", path=report.manifest_path),
        report=report,
    )


def _safe_path(raw_path: object, source: str) -> tuple[Path, str, Any | None]:
    paths = _paths_domain()
    try:
        path = paths.safe_relative_path(raw_path, source=source)
    except paths.GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return Path("."), label, _issue("invalid_path", str(exc), label)
    return path, _label(path), None


def _label(path: Path) -> str:
    return "." if path.as_posix() == "." else path.as_posix()


def _render_yaml_documents(documents: tuple[Mapping[str, Any], ...], yaml_codec: YamlCodec) -> str:
    return "\n---\n".join(yaml_codec.dump(dict(document)).strip() for document in documents) + "\n"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _issue(code: str, message: str, path: str) -> Any:
    return _gitops_models().GitOpsIssue(code=code, message=message, path=path, source=_SOURCE)


def _builder_domain() -> Any:
    return import_module("dpone.gitops.airflow_k8s_manifests")


def _models_domain() -> Any:
    return import_module("dpone.gitops.airflow_k8s_manifests_models")


def _paths_domain() -> Any:
    return import_module("dpone.gitops.paths")


def _gitops_models() -> Any:
    return import_module("dpone.gitops.models")


__all__ = ["GitOpsAirflowK8sManifestsContext", "GitOpsAirflowK8sManifestsService"]
