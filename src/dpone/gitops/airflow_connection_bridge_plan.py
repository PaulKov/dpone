from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.airflow_connection_names import is_valid_airflow_conn_env_name, is_valid_airflow_connection_id
from dpone.gitops.airflow_connection_bridge_models import (
    GitOpsAirflowConnectionBridge,
    connection_bridge_from_mapping,
)
from dpone.gitops.models import GitOpsIssue
from dpone.kubernetes_names import is_valid_kubernetes_dns_label

AIRFLOW_CONNECTION_BRIDGE_PLAN_SOURCE = "dpone gitops airflow connection-bridge-plan"
AIRFLOW_CONNECTION_BRIDGE_PLAN_KIND = "gitops.airflow_connection_bridge_plan"
_PLACEHOLDER = "REPLACE_WITH_AIRFLOW_CONNECTION_URI"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowConnectionBridgePlanArtifact:
    path: str
    kind: str
    required: bool
    exists: bool
    reason: str
    content: str | None = None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "required": self.required,
            "exists": self.exists,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowConnectionBridgePlanReport:
    artifact_dir: str
    output_path: str
    runtime_profile_path: str
    pod_contract_path: str
    mode: str
    runtime_mode: str
    secret_name: str | None
    required_connection_ids: tuple[str, ...]
    env: tuple[dict[str, Any], ...]
    artifacts: tuple[GitOpsAirflowConnectionBridgePlanArtifact, ...]
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = AIRFLOW_CONNECTION_BRIDGE_PLAN_KIND
    schema_version: str = "1"
    producer: str = AIRFLOW_CONNECTION_BRIDGE_PLAN_SOURCE

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "artifact_dir": self.artifact_dir,
            "output_path": self.output_path,
            "runtime_profile_path": self.runtime_profile_path,
            "pod_contract_path": self.pod_contract_path,
            "mode": self.mode,
            "runtime_mode": self.runtime_mode,
            "secret_name": self.secret_name,
            "required_connection_ids": list(self.required_connection_ids),
            "env": [dict(item) for item in self.env],
            "artifacts": [artifact.to_jsonable() for artifact in self.artifacts],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowConnectionBridgePlanInput:
    artifact_dir: str
    output_path: str
    runtime_profile_path: str
    pod_contract_path: str
    secret_manifest_path: str
    external_secret_path: str
    env_example_path: str
    runtime_profile: Mapping[str, Any] | None
    pod_contract: Mapping[str, Any] | None
    external_secret_store: str
    external_secret_store_kind: str
    external_secret_remote_prefix: str
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()


class GitOpsAirflowConnectionBridgePlanBuilder:
    """Build deployable, secrets-safe Airflow connection bridge skeletons."""

    def build(self, data: GitOpsAirflowConnectionBridgePlanInput) -> GitOpsAirflowConnectionBridgePlanReport:
        bridge, bridge_warnings, bridge_blockers = _select_bridge(data.runtime_profile, data.pod_contract)
        warnings = (*data.warnings, *bridge_warnings)
        blockers = (*data.blockers, *bridge_blockers, *_bridge_policy_blockers(bridge, data.output_path))
        artifacts = _artifacts(
            bridge=bridge,
            secret_manifest_path=data.secret_manifest_path,
            external_secret_path=data.external_secret_path,
            env_example_path=data.env_example_path,
            external_secret_store=data.external_secret_store,
            external_secret_store_kind=data.external_secret_store_kind,
            external_secret_remote_prefix=data.external_secret_remote_prefix,
            enabled=not blockers,
        )
        return GitOpsAirflowConnectionBridgePlanReport(
            artifact_dir=data.artifact_dir,
            output_path=data.output_path,
            runtime_profile_path=data.runtime_profile_path,
            pod_contract_path=data.pod_contract_path,
            mode=bridge.mode if bridge is not None else "disabled",
            runtime_mode=bridge.runtime_mode if bridge is not None else "runtime_only",
            secret_name=_valid_bridge_secret_name(bridge) if bridge is not None else None,
            required_connection_ids=_valid_required_connection_ids(bridge) if bridge is not None else (),
            env=_valid_env_refs(bridge) if bridge is not None else (),
            artifacts=artifacts,
            warnings=warnings,
            blockers=blockers,
        )


def _select_bridge(
    runtime_profile: Mapping[str, Any] | None,
    pod_contract: Mapping[str, Any] | None,
) -> tuple[GitOpsAirflowConnectionBridge | None, tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
    profile_bridge = connection_bridge_from_mapping(_mapping(runtime_profile).get("connection_bridge"))
    contract_bridge = connection_bridge_from_mapping(_mapping(pod_contract).get("connection_bridge"))
    warnings: list[GitOpsIssue] = []
    blockers: list[GitOpsIssue] = []
    if profile_bridge is None and contract_bridge is None:
        blockers.append(
            _issue(
                "airflow_connection_bridge_plan_source_missing",
                "runtime-profile.json or pod-contract.json must contain connection_bridge",
                "",
            )
        )
        return None, tuple(warnings), tuple(blockers)
    if profile_bridge is None:
        warnings.append(
            _issue("airflow_connection_bridge_plan_profile_missing", "runtime-profile bridge is missing", "")
        )
    if contract_bridge is None:
        warnings.append(_issue("airflow_connection_bridge_plan_contract_missing", "pod-contract bridge is missing", ""))
    if (
        profile_bridge is not None
        and contract_bridge is not None
        and _signature(profile_bridge) != _signature(contract_bridge)
    ):
        warnings.append(
            _issue(
                "airflow_connection_bridge_plan_source_mismatch",
                "runtime-profile and pod-contract connection_bridge sections differ; pod-contract wins",
                "",
            )
        )
    return contract_bridge or profile_bridge, tuple(warnings), tuple(blockers)


def _bridge_policy_blockers(
    bridge: GitOpsAirflowConnectionBridge | None,
    output_path: str,
) -> tuple[GitOpsIssue, ...]:
    if bridge is None or not bridge.required_connection_ids:
        return ()
    blockers: list[GitOpsIssue] = []
    blockers.extend(_bridge_connection_id_blockers(bridge, output_path))
    blockers.extend(_bridge_secret_name_blockers(bridge, output_path))
    blockers.extend(_bridge_secret_key_blockers(bridge, output_path))
    if bridge.mode == "disabled" and bridge.runtime_mode != "airflow_image":
        blockers.append(
            _issue(
                "airflow_connection_bridge_plan_disabled",
                "Required Airflow connections need k8s_secret/env bridge or airflow_image runtime mode",
                output_path,
            )
        )
    if bridge.mode == "k8s_secret":
        missing = [ref.env_name for ref in bridge.env if not ref.secret_name]
        if missing:
            blockers.append(
                _issue(
                    "airflow_connection_bridge_plan_secret_ref_missing",
                    "k8s_secret bridge env refs must include secret_ref values: " + ", ".join(missing),
                    output_path,
                )
            )
    return tuple(blockers)


def _valid_required_connection_ids(bridge: GitOpsAirflowConnectionBridge) -> tuple[str, ...]:
    return tuple(item for item in bridge.required_connection_ids if is_valid_airflow_connection_id(item))


def _valid_env_refs(bridge: GitOpsAirflowConnectionBridge) -> tuple[dict[str, Any], ...]:
    return tuple(
        ref.to_jsonable()
        for ref in bridge.env
        if is_valid_airflow_connection_id(ref.connection_id)
        and (not ref.secret_name or is_valid_kubernetes_dns_label(ref.secret_name))
        and is_valid_airflow_conn_env_name(ref.secret_key or ref.env_name)
    )


def _bridge_connection_id_blockers(
    bridge: GitOpsAirflowConnectionBridge,
    output_path: str,
) -> tuple[GitOpsIssue, ...]:
    blockers: list[GitOpsIssue] = []
    for _ in (item for item in bridge.required_connection_ids if not is_valid_airflow_connection_id(item)):
        blockers.append(
            _issue(
                "airflow_connection_bridge_plan_invalid_connection_id",
                "Airflow connection bridge ids must be logical Airflow Connection ids, not URIs or credentials.",
                output_path,
            )
        )
    for ref in (item for item in bridge.env if not is_valid_airflow_connection_id(item.connection_id)):
        blockers.append(
            _issue(
                "airflow_connection_bridge_plan_invalid_connection_id",
                "Airflow connection bridge env refs must use logical Airflow Connection ids, not URIs or credentials.",
                output_path,
            )
        )
    return tuple(blockers)


def _valid_bridge_secret_name(bridge: GitOpsAirflowConnectionBridge) -> str | None:
    return bridge.secret_name if is_valid_kubernetes_dns_label(bridge.secret_name) else None


def _bridge_secret_name_blockers(
    bridge: GitOpsAirflowConnectionBridge,
    output_path: str,
) -> tuple[GitOpsIssue, ...]:
    blockers: list[GitOpsIssue] = []
    if bridge.secret_name is not None and not is_valid_kubernetes_dns_label(bridge.secret_name):
        blockers.append(
            _issue(
                "airflow_connection_bridge_plan_invalid_secret_name",
                "Airflow connection bridge Secret names must be safe Kubernetes DNS labels.",
                output_path,
            )
        )
    for _ in (item for item in bridge.env if item.secret_name and not is_valid_kubernetes_dns_label(item.secret_name)):
        blockers.append(
            _issue(
                "airflow_connection_bridge_plan_invalid_secret_name",
                "Airflow connection bridge env refs must use safe Kubernetes Secret names.",
                output_path,
            )
        )
    return tuple(blockers)


def _bridge_secret_key_blockers(
    bridge: GitOpsAirflowConnectionBridge,
    output_path: str,
) -> tuple[GitOpsIssue, ...]:
    blockers: list[GitOpsIssue] = []
    for _ in (item for item in bridge.env if not is_valid_airflow_conn_env_name(item.secret_key or item.env_name)):
        blockers.append(
            _issue(
                "airflow_connection_bridge_plan_invalid_secret_key",
                "Airflow connection bridge Secret keys must be safe AIRFLOW_CONN_* keys.",
                output_path,
            )
        )
    return tuple(blockers)


def _artifacts(
    *,
    bridge: GitOpsAirflowConnectionBridge | None,
    secret_manifest_path: str,
    external_secret_path: str,
    env_example_path: str,
    external_secret_store: str,
    external_secret_store_kind: str,
    external_secret_remote_prefix: str,
    enabled: bool,
) -> tuple[GitOpsAirflowConnectionBridgePlanArtifact, ...]:
    if bridge is None or not bridge.required_connection_ids or not enabled or bridge.runtime_mode == "airflow_image":
        return ()
    env = tuple(ref.to_jsonable() for ref in bridge.env)
    artifacts = [
        GitOpsAirflowConnectionBridgePlanArtifact(
            path=env_example_path,
            kind="airflow.ConnectionEnvExample",
            required=bridge.mode == "env",
            exists=True,
            reason="written",
            content=_env_example(env),
        )
    ]
    if bridge.mode == "k8s_secret":
        secret_name = bridge.secret_name or "dpone-airflow-connections"
        artifacts.extend(
            [
                GitOpsAirflowConnectionBridgePlanArtifact(
                    path=secret_manifest_path,
                    kind="kubernetes.Secret",
                    required=True,
                    exists=True,
                    reason="written",
                    content=_secret_manifest(secret_name=secret_name, env=env),
                ),
                GitOpsAirflowConnectionBridgePlanArtifact(
                    path=external_secret_path,
                    kind="kubernetes.ExternalSecret",
                    required=False,
                    exists=True,
                    reason="written",
                    content=_external_secret_manifest(
                        secret_name=secret_name,
                        env=env,
                        store=external_secret_store,
                        store_kind=external_secret_store_kind,
                        remote_prefix=external_secret_remote_prefix,
                    ),
                ),
            ]
        )
    return tuple(artifacts)


def _secret_manifest(*, secret_name: str, env: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "apiVersion: v1",
        "kind: Secret",
        "metadata:",
        f"  name: {_yaml_string(secret_name)}",
        "type: Opaque",
        "stringData:",
    ]
    lines.extend(f"  {ref['env_name']}: {_yaml_string(_PLACEHOLDER)}" for ref in env)
    return "\n".join(lines) + "\n"


def _external_secret_manifest(
    *,
    secret_name: str,
    env: Sequence[Mapping[str, Any]],
    store: str,
    store_kind: str,
    remote_prefix: str,
) -> str:
    prefix = remote_prefix.strip().strip("/")
    lines = [
        "apiVersion: external-secrets.io/v1beta1",
        "kind: ExternalSecret",
        "metadata:",
        f"  name: {_yaml_string(secret_name)}",
        "spec:",
        "  refreshInterval: 1h",
        "  secretStoreRef:",
        f"    name: {_yaml_string(store)}",
        f"    kind: {_yaml_string(store_kind)}",
        "  target:",
        f"    name: {_yaml_string(secret_name)}",
        "    creationPolicy: Owner",
        "  data:",
    ]
    for ref in env:
        env_name = str(ref["env_name"])
        remote_key = f"{prefix}/{env_name}" if prefix else env_name
        lines.extend(
            [
                f"    - secretKey: {_yaml_string(env_name)}",
                "      remoteRef:",
                f"        key: {_yaml_string(remote_key)}",
            ]
        )
    return "\n".join(lines) + "\n"


def _env_example(env: Sequence[Mapping[str, Any]]) -> str:
    return "".join(f"{ref['env_name']}={_PLACEHOLDER}\n" for ref in env)


def _signature(bridge: GitOpsAirflowConnectionBridge) -> dict[str, Any]:
    payload = bridge.to_jsonable()
    return {
        "mode": payload.get("mode"),
        "runtime_mode": payload.get("runtime_mode"),
        "required_connection_ids": payload.get("required_connection_ids"),
        "env": payload.get("env"),
        "secret_name": payload.get("secret_name"),
    }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _yaml_string(value: object) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def _issue(code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=AIRFLOW_CONNECTION_BRIDGE_PLAN_SOURCE)


__all__ = [
    "AIRFLOW_CONNECTION_BRIDGE_PLAN_KIND",
    "AIRFLOW_CONNECTION_BRIDGE_PLAN_SOURCE",
    "GitOpsAirflowConnectionBridgePlanArtifact",
    "GitOpsAirflowConnectionBridgePlanBuilder",
    "GitOpsAirflowConnectionBridgePlanInput",
    "GitOpsAirflowConnectionBridgePlanReport",
]
