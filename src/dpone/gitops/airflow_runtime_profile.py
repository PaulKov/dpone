from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.airflow_connection_names import airflow_conn_env_name
from dpone.gitops.airflow_connection_bridge_models import (
    GitOpsAirflowConnectionBridge,
    GitOpsAirflowConnectionEnvRef,
)
from dpone.gitops.airflow_git_sync_models import GitOpsAirflowGitSyncContract
from dpone.gitops.airflow_outcome_gate import normalize_airflow_outcome_mode
from dpone.gitops.airflow_runtime_profile_models import (
    GitOpsAirflowArtifactSink,
    GitOpsAirflowRuntimeProfile,
    GitOpsAirflowRuntimeResources,
    GitOpsAirflowXComSummary,
)
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec

_BRIDGE_MODES = {"k8s_secret", "env", "disabled"}
_BRIDGE_RUNTIME_MODES = {"runtime_only", "airflow_image"}


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRuntimeProfileInput:
    bundle_path: str
    run_spec_path: str
    runtime_evidence_path: str
    xcom_summary_path: str
    dag_factory_path: str
    outcome_gate_path: str
    image: str
    image_digest: str | None
    namespace: str
    service_account: str
    resource_requests: Mapping[str, str]
    resource_limits: Mapping[str, str]
    artifact_sink_kind: str
    artifact_sink_path: str | None
    runner_policy: str
    env: tuple[dict[str, str], ...] = ()
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None
    git_sync: GitOpsAirflowGitSyncContract | None = None
    connection_bridge: GitOpsAirflowConnectionBridge | None = None
    outcome_mode: str = "strict_fail"
    bundle: Mapping[str, Any] | None = None
    run_spec: Mapping[str, Any] | None = None
    warnings: tuple[Any, ...] = ()
    blockers: tuple[Any, ...] = ()


class GitOpsAirflowRuntimeProfileBuilder:
    """Builds the stable runtime-placement contract for Airflow Kubernetes runners."""

    def build(self, data: GitOpsAirflowRuntimeProfileInput) -> GitOpsAirflowRuntimeProfile:
        profile = GitOpsAirflowRuntimeProfile(
            bundle_path=data.bundle_path,
            bundle_digest=_bundle_digest(data.bundle),
            run_spec_path=data.run_spec_path,
            runtime_evidence_path=data.runtime_evidence_path,
            xcom_summary_path=data.xcom_summary_path,
            dag_factory_path=data.dag_factory_path,
            outcome_gate_path=data.outcome_gate_path,
            image=data.image,
            image_digest=data.image_digest,
            namespace=data.namespace,
            service_account=data.service_account,
            resources=GitOpsAirflowRuntimeResources(
                requests=dict(data.resource_requests),
                limits=dict(data.resource_limits),
            ),
            artifact_sink=GitOpsAirflowArtifactSink(kind=data.artifact_sink_kind, path=data.artifact_sink_path),
            env=data.env,
            labels=data.labels,
            annotations=data.annotations,
            git_sync=data.git_sync,
            connection_bridge=data.connection_bridge,
            outcome_mode=normalize_airflow_outcome_mode(data.outcome_mode),
            runner_policy=data.runner_policy,
            warnings=data.warnings,
            blockers=data.blockers,
        )
        return profile

    def xcom_summary(
        self,
        profile: GitOpsAirflowRuntimeProfile,
        *,
        runtime_profile_path: str,
    ) -> GitOpsAirflowXComSummary:
        return GitOpsAirflowXComSummary(
            runtime_profile_path=runtime_profile_path,
            run_spec_path=profile.run_spec_path,
            runtime_evidence_path=profile.runtime_evidence_path,
            blockers=profile.blockers,
        )


class GitOpsAirflowConnectionBridgeBuilder:
    """Discover Airflow connection ids and build a secrets-safe runtime bridge."""

    def __init__(self, *, fs: FileSystem, yaml: YamlCodec) -> None:
        self._fs = fs
        self._yaml = yaml

    def build(
        self,
        *,
        args: object,
        repo_root: Path,
        bundle: Mapping[str, Any],
        enabled: bool,
    ) -> tuple[GitOpsAirflowConnectionBridge, tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
        mode = _text(getattr(args, "airflow_connection_bridge", None)) or "k8s_secret"
        runtime_mode = _text(getattr(args, "airflow_runtime_mode", None)) or "runtime_only"
        secret_name = _text(getattr(args, "airflow_connection_secret", None)) or "dpone-airflow-connections"
        warnings: list[GitOpsIssue] = []
        blockers: list[GitOpsIssue] = []
        if mode not in _BRIDGE_MODES:
            blockers.append(_bridge_issue("airflow_connection_bridge_invalid", "Unknown Airflow bridge mode", mode))
        if runtime_mode not in _BRIDGE_RUNTIME_MODES:
            blockers.append(_bridge_issue("airflow_runtime_mode_invalid", "Unknown Airflow runtime mode", runtime_mode))
        connection_ids: tuple[str, ...] = ()
        if enabled:
            connection_ids, discover_warnings, discover_blockers = self._discover(repo_root=repo_root, bundle=bundle)
            warnings.extend(discover_warnings)
            blockers.extend(discover_blockers)
        bridge = GitOpsAirflowConnectionBridge(
            mode=mode,
            runtime_mode=runtime_mode,
            secret_name=secret_name if mode == "k8s_secret" else None,
            required_connection_ids=connection_ids,
            env=_env_refs(connection_ids, mode=mode, secret_name=secret_name),
            warnings=tuple(item.to_jsonable() for item in warnings),
            blockers=tuple(item.to_jsonable() for item in blockers),
            enabled=True,
        )
        return bridge, tuple(warnings), tuple(blockers)

    def _discover(
        self,
        *,
        repo_root: Path,
        bundle: Mapping[str, Any],
    ) -> tuple[tuple[str, ...], tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
        ids: list[str] = []
        warnings: list[GitOpsIssue] = []
        blockers: list[GitOpsIssue] = []
        for raw_path in _candidate_manifest_paths(repo_root=repo_root, bundle=bundle, fs=self._fs):
            try:
                path = safe_relative_path(raw_path, source="airflow.connection_bridge.manifest")
            except GitOpsPathValidationError as exc:
                blockers.append(_bridge_issue("airflow_connection_manifest_path_invalid", str(exc), str(raw_path)))
                continue
            full_path = repo_root / path
            if not self._fs.exists(full_path):
                warnings.append(
                    _bridge_issue("airflow_connection_manifest_missing", "Manifest path is missing", raw_path)
                )
                continue
            try:
                data = self._yaml.load(self._fs.read_text(full_path, encoding="utf-8"))
            except Exception as exc:
                blockers.append(_bridge_issue("airflow_connection_manifest_yaml_invalid", str(exc), raw_path))
                continue
            ids.extend(_connection_ids(data))
        return tuple(dict.fromkeys(ids)), tuple(warnings), tuple(blockers)


def _bundle_digest(bundle: Mapping[str, Any] | None) -> str | None:
    attestation = _mapping(bundle.get("attestation") if bundle else None)
    digest = str(attestation.get("bundle_digest") or "").strip()
    if not digest:
        return None
    return digest if digest.startswith("sha256:") else f"sha256:{digest}"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _candidate_manifest_paths(*, repo_root: Path, bundle: Mapping[str, Any], fs: FileSystem) -> tuple[str, ...]:
    paths: list[str] = []
    for entry in _bundle_entries(bundle):
        manifest = _text(entry.get("manifest"))
        if manifest:
            paths.append(manifest)
        paths.extend(_plan_manifest_paths(repo_root=repo_root, raw_path=entry.get("plan_path"), fs=fs))
    return tuple(dict.fromkeys(paths))


def _plan_manifest_paths(*, repo_root: Path, raw_path: object, fs: FileSystem) -> tuple[str, ...]:
    path = _text(raw_path)
    if not path:
        return ()
    try:
        rel_path = safe_relative_path(path, source="bundle.entries.plan_path")
    except GitOpsPathValidationError:
        return ()
    try:
        payload = json.loads(fs.read_text(repo_root / rel_path, encoding="utf-8"))
    except Exception:
        return ()
    raw_entries = payload.get("sparse_paths") if isinstance(payload, Mapping) else None
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, str | bytes):
        return ()
    return tuple(
        _text(item.get("path"))
        for item in raw_entries
        if isinstance(item, Mapping) and _text(item.get("path")).endswith((".yaml", ".yml"))
    )


def _connection_ids(value: object) -> tuple[str, ...]:
    found: list[str] = []
    if isinstance(value, Mapping):
        if str(value.get("connection_type") or "").strip().lower() == "airflow":
            connection_id = _text(value.get("connection_id"))
            if connection_id:
                found.append(connection_id)
        for nested in value.values():
            found.extend(_connection_ids(nested))
    elif isinstance(value, list):
        for item in value:
            found.extend(_connection_ids(item))
    return tuple(found)


def _env_refs(
    connection_ids: Sequence[str],
    *,
    mode: str,
    secret_name: str,
) -> tuple[GitOpsAirflowConnectionEnvRef, ...]:
    refs: list[GitOpsAirflowConnectionEnvRef] = []
    for connection_id in connection_ids:
        env_name = airflow_conn_env_name(connection_id)
        if mode == "k8s_secret":
            refs.append(
                GitOpsAirflowConnectionEnvRef(
                    connection_id=connection_id,
                    env_name=env_name,
                    secret_name=secret_name,
                    secret_key=env_name,
                )
            )
        elif mode == "env":
            refs.append(GitOpsAirflowConnectionEnvRef(connection_id=connection_id, env_name=env_name))
    return tuple(refs)


def _bundle_entries(bundle: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_entries = bundle.get("entries")
    if not isinstance(raw_entries, list):
        return ()
    return tuple(entry for entry in raw_entries if isinstance(entry, Mapping))


def _text(value: object) -> str:
    return str(value or "").strip()


def _bridge_issue(code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source="dpone gitops airflow connection-bridge")


__all__ = [
    "GitOpsAirflowConnectionBridgeBuilder",
    "GitOpsAirflowRuntimeProfileBuilder",
    "GitOpsAirflowRuntimeProfileInput",
]
