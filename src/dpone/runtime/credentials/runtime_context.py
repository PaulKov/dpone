"""Load the exact runtime connection authority published by strict init-fetch."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.runtime_init_fetch_plan import RuntimeArtifactDescriptor, RuntimeInitFetchPlan


import hashlib
import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dpone.contracts.runtime_connection import (
    RuntimeConnectionAuthorityError,
    runtime_connection_authority_subject,
)
from dpone.runtime.init_fetch_contract import cache_relative_path
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan
from dpone.runtime.runtime_init_fetch_storage import read_verified_file

from .binding_resolver import BindingCredentialResolver, KubernetesSecretReader, VaultKvClient
from .workload_scope import WorkloadScopedCredentialResolver

RUNTIME_CONNECTION_CONTEXT_ENV = "DPONE_RUNTIME_CONNECTION_CONTEXT"
RUNTIME_INIT_FETCH_PLAN_B64_ENV = "DPONE_INIT_FETCH_PLAN_B64"
RUNTIME_INIT_FETCH_PLAN_SHA256_ENV = "DPONE_INIT_FETCH_PLAN_SHA256"

_SCHEMAS = {
    "binding_set": "dpone.binding-set.v1",
    "connection_registry": "dpone.connection-registry.v1",
    "credential_runtime": "dpone.credential-runtime.v1",
}
_FILENAMES = {
    "binding_set": "binding-set.json",
    "connection_registry": "connection-registry.json",
    "credential_runtime": "credential-runtime.json",
}


class RuntimeVaultReaderFactory(Protocol):
    """Build one lazy Vault reader from non-secret runtime configuration."""

    def __call__(self, credential_runtime: Mapping[str, Any]) -> VaultKvClient | None: ...


@dataclass(frozen=True, slots=True)
class RuntimeConnectionContext:
    """Verified environment inputs and one workload-scoped resolver."""

    environment: str
    binding_set: Mapping[str, Any]
    connection_registry: Mapping[str, Any]
    credential_runtime: Mapping[str, Any]
    resolver: WorkloadScopedCredentialResolver
    release_id: str | None = None
    deployment_id: str | None = None
    init_fetch_plan_sha256: str | None = None
    authority_subject_sha256: str | None = None


class RuntimeConnectionContextLoader:
    """Read a strict-v2 context without following paths or trusting mutable files."""

    def __init__(
        self,
        *,
        vault_reader_factory: RuntimeVaultReaderFactory | None = None,
        kubernetes_secret_reader: KubernetesSecretReader | None = None,
    ) -> None:
        self._vault_reader_factory = vault_reader_factory or build_vault_kv_reader
        self._kubernetes_secret_reader = kubernetes_secret_reader

    def load(
        self,
        environ: Mapping[str, str] | None = None,
    ) -> RuntimeConnectionContext | None:
        environment = environ if environ is not None else os.environ
        raw_context = str(environment.get(RUNTIME_CONNECTION_CONTEXT_ENV) or "")
        if not raw_context:
            return None
        plan_b64 = str(environment.get(RUNTIME_INIT_FETCH_PLAN_B64_ENV) or "")
        plan_sha256 = str(environment.get(RUNTIME_INIT_FETCH_PLAN_SHA256_ENV) or "")
        if not plan_b64 or not plan_sha256:
            raise _authority_error(
                "DPONE_RUNTIME_CONNECTION_CONTEXT_UNVERIFIED",
                "Strict runtime connection context requires its pinned init-fetch plan.",
            )
        try:
            plan, _ = decode_runtime_init_fetch_plan(plan_b64, plan_sha256)
        except Exception as exc:
            raise _authority_error(
                "DPONE_RUNTIME_CONNECTION_CONTEXT_UNVERIFIED",
                "Strict runtime connection context does not match a valid pinned init-fetch plan.",
            ) from exc

        context_root = Path(raw_context)
        _require_context_root(context_root, plan.binding_set)
        payloads = {
            name: _read_payload(
                context_root / filename,
                root=context_root,
                descriptor=getattr(plan, name),
                label=name.replace("_", "-"),
            )
            for name, filename in _FILENAMES.items()
        }
        _validate_context_payloads(payloads, environment=plan.environment)
        credential_runtime = payloads["credential_runtime"]
        vault_reader = build_required_vault_kv_reader(
            credential_runtime=credential_runtime,
            connection_registry=payloads["connection_registry"],
            factory=self._vault_reader_factory,
        )
        resolver = BindingCredentialResolver(
            binding_set=payloads["binding_set"],
            connection_registry=payloads["connection_registry"],
            vault_kv_reader=vault_reader,
            kubernetes_secret_reader=self._kubernetes_secret_reader,
            evidence_context={
                "release_id": plan.release_id,
                "deployment_id": plan.deployment_id,
                "credential_runtime_environment": plan.environment,
            },
        )
        return RuntimeConnectionContext(
            environment=plan.environment,
            binding_set=payloads["binding_set"],
            connection_registry=payloads["connection_registry"],
            credential_runtime=credential_runtime,
            resolver=WorkloadScopedCredentialResolver(resolver),
            release_id=plan.release_id,
            deployment_id=plan.deployment_id,
            init_fetch_plan_sha256=plan_sha256,
            authority_subject_sha256=_runtime_authority_subject(plan),
        )


def _runtime_authority_subject(plan: RuntimeInitFetchPlan) -> str:
    """Bind admission inputs to the exact loader-verified runtime authority."""

    return runtime_connection_authority_subject(
        environment=plan.environment,
        release_id=plan.release_id,
        deployment_id=plan.deployment_id,
        release_sha256=plan.release.sha256,
        deployment_sha256=plan.deployment.sha256,
        binding_set_sha256=plan.binding_set.sha256,
        connection_registry_sha256=plan.connection_registry.sha256,
        credential_runtime_sha256=plan.credential_runtime.sha256,
    )


def build_vault_kv_reader(credential_runtime: Mapping[str, Any]) -> VaultKvClient | None:
    """Construct the primary production Vault client without reading a secret."""

    vault = _mapping(credential_runtime.get("vault"))
    if not vault:
        return None
    auth = _mapping(vault.get("auth"))
    method = str(auth.get("method") or "")
    if method != "kubernetes":
        raise _authority_error(
            "DPONE_RUNTIME_CREDENTIAL_AUTH_UNSUPPORTED",
            "Strict runtime currently requires Vault Kubernetes authentication.",
        )
    address = str(vault.get("address") or "")
    role = str(auth.get("role") or "")
    if not address or not role:
        raise _authority_error(
            "DPONE_RUNTIME_CREDENTIAL_AUTH_INVALID",
            "Vault runtime address and Kubernetes role are required.",
        )
    try:
        from vault_kv_client import (
            VaultAuth,
            VaultKubernetesAuth,
            VaultManager,
            VaultSettings,
        )
    except ImportError as exc:  # pragma: no cover - optional runtime dependency.
        raise _authority_error(
            "DPONE_RUNTIME_CREDENTIAL_BACKEND_UNAVAILABLE",
            "Vault runtime support is not installed.",
        ) from exc
    kubernetes_auth = VaultKubernetesAuth(
        role=role,
        mount_point=str(auth.get("mount_point") or "k8s"),
        jwt_file=str(auth.get("jwt_file") or "/var/run/secrets/kubernetes.io/serviceaccount/token"),
    )
    return VaultManager(
        VaultSettings(
            addr=address,
            verify=vault.get("verify", True),
            namespace=str(vault.get("namespace") or "") or None,
        ),
        VaultAuth(kubernetes=kubernetes_auth),
    )


def build_required_vault_kv_reader(
    *,
    credential_runtime: Mapping[str, Any],
    connection_registry: Mapping[str, Any],
    factory: RuntimeVaultReaderFactory | None = None,
) -> VaultKvClient | None:
    """Build Vault only when an exact registry entry requires ``vault_kv``."""

    if "vault_kv" not in required_credential_resolvers(connection_registry):
        return None
    try:
        reader = (factory or build_vault_kv_reader)(credential_runtime)
    except RuntimeConnectionAuthorityError:
        raise
    except Exception as exc:
        raise _authority_error(
            "DPONE_RUNTIME_CREDENTIAL_BACKEND_UNAVAILABLE",
            "Pinned runtime credential backend could not be initialized.",
        ) from exc
    if reader is None:
        raise _authority_error(
            "DPONE_RUNTIME_CREDENTIAL_BACKEND_UNAVAILABLE",
            "Pinned runtime credential backend is not configured.",
        )
    return reader


def _require_context_root(
    context_root: Path,
    descriptor: RuntimeArtifactDescriptor,
) -> None:
    if not context_root.is_absolute():
        raise _authority_error(
            "DPONE_RUNTIME_CONNECTION_CONTEXT_UNSAFE",
            "Runtime connection context path must be absolute.",
        )
    expected_parent = cache_relative_path(descriptor.artifact_ref).parent
    expected_tail = ("payload", *expected_parent.parts)
    if context_root.parts[-len(expected_tail) :] != expected_tail:
        raise _authority_error(
            "DPONE_RUNTIME_CONNECTION_CONTEXT_UNVERIFIED",
            "Runtime connection context path does not match the pinned plan.",
        )


def _read_payload(
    path: Path,
    *,
    root: Path,
    descriptor: RuntimeArtifactDescriptor,
    label: str,
) -> dict[str, Any]:
    try:
        payload = read_verified_file(
            path,
            expected_sha256=descriptor.sha256,
            expected_bytes=descriptor.bytes,
            root=root,
        )
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
            parse_float=_finite_float,
        )
    except RuntimeConnectionAuthorityError:
        raise
    except Exception as exc:
        raise _authority_error(
            "DPONE_RUNTIME_CONNECTION_CONTEXT_INVALID",
            f"Verified {label} runtime artifact is invalid.",
        ) from exc
    if not isinstance(value, dict):
        raise _authority_error(
            "DPONE_RUNTIME_CONNECTION_CONTEXT_INVALID",
            f"Verified {label} runtime artifact must be an object.",
        )
    if "sha256:" + hashlib.sha256(payload).hexdigest() != descriptor.sha256:
        raise _authority_error(
            "DPONE_RUNTIME_CONNECTION_CONTEXT_INVALID",
            f"Verified {label} runtime artifact checksum changed.",
        )
    return value


def _validate_context_payloads(
    payloads: Mapping[str, Mapping[str, Any]],
    *,
    environment: str,
) -> None:
    from dpone.gitops.schema_validation import GitOpsSchemaValidator

    validator = GitOpsSchemaValidator()
    for name, schema in _SCHEMAS.items():
        payload = payloads[name]
        issues = validator.validate(payload, expected_kind=schema)
        if issues:
            raise _authority_error(
                "DPONE_RUNTIME_CONNECTION_CONTEXT_INVALID",
                f"Verified {name.replace('_', '-')} does not satisfy its public schema.",
            )
        if payload.get("environment") != environment:
            raise _authority_error(
                "DPONE_RUNTIME_CONNECTION_CONTEXT_INVALID",
                f"Verified {name.replace('_', '-')} environment does not match the pinned deployment.",
            )


def required_credential_resolvers(connection_registry: Mapping[str, Any]) -> frozenset[str]:
    connections = _mapping(connection_registry.get("connections"))
    return frozenset(
        str(credentials.get("resolver") or "")
        for entry in connections.values()
        if isinstance(entry, Mapping)
        for credentials in (_mapping(entry.get("credentials")),)
        if credentials.get("resolver")
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in items:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value}")


def _finite_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("non-finite JSON number")
    return result


def _authority_error(code: str, message: str) -> RuntimeConnectionAuthorityError:
    return RuntimeConnectionAuthorityError(code, message)


__all__ = [
    "RUNTIME_CONNECTION_CONTEXT_ENV",
    "RUNTIME_INIT_FETCH_PLAN_B64_ENV",
    "RUNTIME_INIT_FETCH_PLAN_SHA256_ENV",
    "RuntimeConnectionContext",
    "RuntimeConnectionContextLoader",
    "build_required_vault_kv_reader",
    "build_vault_kv_reader",
    "required_credential_resolvers",
]
