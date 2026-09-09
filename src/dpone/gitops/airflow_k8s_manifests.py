from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_k8s_controller_metadata import (
    SUPPORTED_AIRFLOW_K8S_GITOPS_CONTROLLERS,
    airflow_k8s_gitops_controller_hints,
    apply_airflow_k8s_gitops_metadata,
    normalize_airflow_k8s_gitops_controller,
)
from dpone.gitops.airflow_k8s_manifests_models import (
    GitOpsAirflowK8sManifestObject,
    GitOpsAirflowK8sManifestsReport,
    airflow_k8s_manifests_issue,
)


@dataclass(frozen=True, slots=True)
class _SecretRef:
    name: str
    kind: str
    source: str
    keys: tuple[str, ...] = ()


class GitOpsAirflowK8sManifestBuilder:
    """Build deployable Kubernetes objects from generated Airflow runtime contracts."""

    def build(
        self,
        *,
        artifact_dir: str,
        manifest_path: str,
        runtime_profile_path: str,
        runtime_profile: Mapping[str, Any],
        pod_contract_path: str,
        pod_contract: Mapping[str, Any],
        connection_bridge_plan_path: str | None,
        connection_bridge_plan: Mapping[str, Any] | None,
        include_network_policy: bool = False,
        gitops_controller: str = "plain",
    ) -> GitOpsAirflowK8sManifestsReport:
        namespace = _text(runtime_profile.get("namespace")) or _text(pod_contract.get("namespace")) or "default"
        service_account = _text(runtime_profile.get("service_account")) or _text(pod_contract.get("service_account"))
        controller = normalize_airflow_k8s_gitops_controller(gitops_controller)
        blockers = _blockers(
            runtime_profile_path=runtime_profile_path,
            runtime_profile=runtime_profile,
            pod_contract_path=pod_contract_path,
            pod_contract=pod_contract,
            connection_bridge_plan_path=connection_bridge_plan_path,
            connection_bridge_plan=connection_bridge_plan,
            namespace=namespace,
            service_account=service_account,
            gitops_controller=controller,
        )
        objects = (
            ()
            if blockers
            else _objects(
                namespace=namespace,
                service_account=service_account,
                runtime_profile=runtime_profile,
                pod_contract=pod_contract,
                connection_bridge_plan=connection_bridge_plan,
                include_network_policy=include_network_policy,
                gitops_controller=controller,
            )
        )
        return GitOpsAirflowK8sManifestsReport(
            artifact_dir=artifact_dir,
            manifest_path=manifest_path,
            runtime_profile_path=runtime_profile_path,
            pod_contract_path=pod_contract_path,
            connection_bridge_plan_path=connection_bridge_plan_path,
            gitops_controller=controller,
            namespace=namespace,
            service_account=service_account,
            objects=objects,
            controller_hints=airflow_k8s_gitops_controller_hints(controller),
            blockers=blockers,
        )


def _blockers(
    *,
    runtime_profile_path: str,
    runtime_profile: Mapping[str, Any],
    pod_contract_path: str,
    pod_contract: Mapping[str, Any],
    connection_bridge_plan_path: str | None,
    connection_bridge_plan: Mapping[str, Any] | None,
    namespace: str,
    service_account: str,
    gitops_controller: str,
) -> tuple[Any, ...]:
    issues: list[Any] = []
    if runtime_profile.get("kind") != "gitops.airflow_runtime_profile":
        issues.append(
            _issue(
                "airflow_k8s_manifests_runtime_profile_kind", "Runtime profile kind is invalid", runtime_profile_path
            )
        )
    if pod_contract.get("kind") != "gitops.airflow_pod_contract":
        issues.append(
            _issue("airflow_k8s_manifests_pod_contract_kind", "Pod contract kind is invalid", pod_contract_path)
        )
    if (
        connection_bridge_plan is not None
        and connection_bridge_plan.get("kind") != "gitops.airflow_connection_bridge_plan"
    ):
        issues.append(
            _issue(
                "airflow_k8s_manifests_connection_bridge_plan_kind",
                "Connection bridge plan kind is invalid",
                connection_bridge_plan_path or "connection-bridge-plan.json",
            )
        )
    if not namespace or namespace != _text(pod_contract.get("namespace")):
        issues.append(
            _issue("airflow_k8s_manifests_namespace", "Namespace must match pod contract", runtime_profile_path)
        )
    if not service_account or service_account != _text(pod_contract.get("service_account")):
        issues.append(
            _issue(
                "airflow_k8s_manifests_service_account",
                "Service account must match pod contract",
                runtime_profile_path,
            )
        )
    if gitops_controller not in SUPPORTED_AIRFLOW_K8S_GITOPS_CONTROLLERS:
        issues.append(
            _issue(
                "airflow_k8s_manifests_gitops_controller",
                "GitOps controller must be one of: plain, argocd, flux",
                "--gitops-controller",
            )
        )
    return tuple(issues)


def _objects(
    *,
    namespace: str,
    service_account: str,
    runtime_profile: Mapping[str, Any],
    pod_contract: Mapping[str, Any],
    connection_bridge_plan: Mapping[str, Any] | None,
    include_network_policy: bool,
    gitops_controller: str,
) -> tuple[GitOpsAirflowK8sManifestObject, ...]:
    image_pull_secrets = _image_pull_secret_names(pod_contract)
    objects = [
        _object(
            _service_account(namespace, service_account, image_pull_secrets),
            source="runtime_profile",
            gitops_controller=gitops_controller,
        ),
        _object(_role(namespace, service_account), source="runtime_profile", gitops_controller=gitops_controller),
        _object(
            _role_binding(namespace, service_account),
            source="runtime_profile",
            gitops_controller=gitops_controller,
        ),
    ]
    objects.extend(
        _object(_secret(namespace, ref), source=ref.source, gitops_controller=gitops_controller)
        for ref in _secret_refs(runtime_profile, pod_contract, connection_bridge_plan)
    )
    external_secret = _external_secret(namespace, connection_bridge_plan)
    if external_secret is not None:
        objects.append(_object(external_secret, source="connection_bridge_plan", gitops_controller=gitops_controller))
    if include_network_policy:
        objects.append(
            _object(
                _network_policy(namespace, service_account),
                source="--include-network-policy",
                gitops_controller=gitops_controller,
            )
        )
    return tuple(objects)


def _service_account(namespace: str, name: str, image_pull_secrets: Sequence[str]) -> dict[str, Any]:
    body: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": name, "namespace": namespace},
    }
    if image_pull_secrets:
        body["imagePullSecrets"] = [{"name": secret} for secret in image_pull_secrets]
    return body


def _role(namespace: str, name: str) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": name, "namespace": namespace},
        "rules": [
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["create", "delete", "get", "list", "watch"]},
            {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]},
            {"apiGroups": [""], "resources": ["events"], "verbs": ["get", "list", "watch"]},
        ],
    }


def _role_binding(namespace: str, name: str) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": name, "namespace": namespace},
        "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": namespace}],
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
    }


def _secret(namespace: str, ref: _SecretRef) -> dict[str, Any]:
    annotations = {"dpone.io/source": ref.source}
    if ref.keys:
        annotations["dpone.io/required-keys"] = ",".join(ref.keys)
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": ref.name, "namespace": namespace, "annotations": annotations},
        "type": "Opaque",
        "stringData": {},
    }


def _external_secret(namespace: str, plan: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not plan or not _has_external_secret_artifact(plan):
        return None
    name = _text(plan.get("secret_name"))
    if not name:
        return None
    keys = tuple(_connection_secret_keys(plan))
    return {
        "apiVersion": "external-secrets.io/v1beta1",
        "kind": "ExternalSecret",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {"name": "dpone-secret-store", "kind": "ClusterSecretStore"},
            "target": {"name": name, "creationPolicy": "Owner"},
            "data": [{"secretKey": key, "remoteRef": {"key": name, "property": key}} for key in keys],
        },
    }


def _network_policy(namespace: str, service_account: str) -> dict[str, Any]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": f"{service_account}-allow-all-egress", "namespace": namespace},
        "spec": {
            "podSelector": {},
            "policyTypes": ["Egress"],
            "egress": [{}],
        },
    }


def _secret_refs(
    runtime_profile: Mapping[str, Any],
    pod_contract: Mapping[str, Any],
    connection_bridge_plan: Mapping[str, Any] | None,
) -> tuple[_SecretRef, ...]:
    refs: dict[str, _SecretRef] = {}
    for ref in (
        *_git_sync_secret_refs(runtime_profile),
        *_connection_secret_refs(connection_bridge_plan or pod_contract.get("connection_bridge")),
        *_image_pull_secret_refs(pod_contract),
    ):
        current = refs.get(ref.name)
        keys = tuple(sorted({*(current.keys if current else ()), *ref.keys}))
        refs[ref.name] = _SecretRef(name=ref.name, kind=ref.kind, source=ref.source, keys=keys)
    return tuple(refs.values())


def _git_sync_secret_refs(runtime_profile: Mapping[str, Any]) -> tuple[_SecretRef, ...]:
    auth = _mapping(_mapping(runtime_profile.get("git_sync")).get("auth"))
    mode = _text(auth.get("mode"))
    if mode == "ssh_secret":
        secret = _mapping(auth.get("ssh_secret"))
        name = _text(secret.get("name"))
        if not name:
            return ()
        return (
            _SecretRef(
                name,
                "gitSyncSshSecret",
                "git_sync.auth",
                (_text(secret.get("known_hosts_key")) or "known_hosts", _text(secret.get("ssh_key")) or "ssh"),
            ),
        )
    if mode == "https_secret":
        secret = _mapping(auth.get("https_secret"))
        name = _text(secret.get("name"))
        if not name:
            return ()
        return (
            _SecretRef(
                name,
                "gitSyncHttpsSecret",
                "git_sync.auth",
                (_text(secret.get("username_key")) or "username", _text(secret.get("password_key")) or "password"),
            ),
        )
    return ()


def _connection_secret_refs(value: object) -> tuple[_SecretRef, ...]:
    keys_by_name: dict[str, set[str]] = {}
    for item in _list(_mapping(value).get("env")):
        secret = _mapping(_mapping(item).get("secret_ref"))
        name = _text(secret.get("name"))
        key = _text(secret.get("key"))
        if name and key:
            keys_by_name.setdefault(name, set()).add(key)
    return tuple(
        _SecretRef(name, "airflowConnectionSecret", "connection_bridge", tuple(sorted(keys)))
        for name, keys in sorted(keys_by_name.items())
    )


def _image_pull_secret_refs(pod_contract: Mapping[str, Any]) -> tuple[_SecretRef, ...]:
    return tuple(
        _SecretRef(name, "imagePullSecret", "pod_spec", (".dockerconfigjson",))
        for name in _image_pull_secret_names(pod_contract)
    )


def _image_pull_secret_names(pod_contract: Mapping[str, Any]) -> tuple[str, ...]:
    raw = _mapping(_mapping(pod_contract.get("pod_spec")).get("spec")).get("imagePullSecrets")
    if not isinstance(raw, list):
        return ()
    return tuple(_text(item.get("name")) for item in raw if isinstance(item, Mapping) and _text(item.get("name")))


def _connection_secret_keys(plan: Mapping[str, Any]) -> tuple[str, ...]:
    refs = _connection_secret_refs(plan)
    return tuple(key for ref in refs for key in ref.keys)


def _has_external_secret_artifact(plan: Mapping[str, Any]) -> bool:
    return any(_mapping(item).get("kind") == "kubernetes.ExternalSecret" for item in _list(plan.get("artifacts")))


def _object(body: Mapping[str, Any], *, source: str, gitops_controller: str) -> GitOpsAirflowK8sManifestObject:
    rendered = apply_airflow_k8s_gitops_metadata(body, controller=gitops_controller)
    metadata = _mapping(rendered.get("metadata"))
    return GitOpsAirflowK8sManifestObject(
        api_version=_text(rendered.get("apiVersion")),
        kind=_text(rendered.get("kind")),
        name=_text(metadata.get("name")),
        namespace=_text(metadata.get("namespace")),
        source=source,
        required=True,
        body=rendered,
    )


def _issue(code: str, message: str, path: str) -> Any:
    return airflow_k8s_manifests_issue(code, message, path)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: object) -> tuple[object, ...]:
    return tuple(value) if isinstance(value, list) else ()


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = ["GitOpsAirflowK8sManifestBuilder"]
