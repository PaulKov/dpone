"""Strict, secret-free runtime plan consumed by indexed Airflow KPOs."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.runtime_artifact_delivery import (
    is_safe_artifact_registry_logical_ref,
    normalize_config_map_ref,
    validate_runtime_image_reference,
)
from dpone.kubernetes_names import is_valid_kubernetes_dns_label
from dpone.runtime.init_fetch_contract import cache_relative_path
from dpone.runtime.runtime_init_fetch_execution import (
    RuntimeExecutionSelection,
    require_execution_token,
)

RUNTIME_INIT_FETCH_PLAN_SCHEMA = "dpone.airflow-runtime-init-fetch-plan.v1"
RUNTIME_INIT_FETCH_PLAN_SCHEMA_V2 = "dpone.airflow-runtime-init-fetch-plan.v2"
RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3 = "dpone.airflow-runtime-init-fetch-plan.v3"
MAX_RUNTIME_INIT_FETCH_PLAN_BYTES = 16 * 1024
MAX_SELECTED_RUNTIME_PAYLOADS = 16

_ENVIRONMENT_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")
_CONTEXT_DIGEST_DIR_RE = re.compile(r"^sha256-[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class RuntimeArtifactDescriptor:
    """Exact content descriptor without an execution-level logical id."""

    artifact_ref: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        cache_relative_path(self.artifact_ref)
        _require_digest("artifact.sha256", self.sha256)
        _positive_integer(self.bytes, "artifact.bytes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "bytes": self.bytes,
        }


@dataclass(frozen=True, slots=True)
class RuntimeWorkloadPackRef:
    """Exact workload pack plus its canonical semantic fingerprint."""

    id: str
    artifact_ref: str
    sha256: str
    bytes: int
    pack_fingerprint: str

    def __post_init__(self) -> None:
        require_execution_token("workload_pack.id", self.id)
        cache_relative_path(self.artifact_ref)
        _require_digest("workload_pack.sha256", self.sha256)
        _positive_integer(self.bytes, "workload_pack.bytes")
        _require_digest("workload_pack.pack_fingerprint", self.pack_fingerprint)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "pack_fingerprint": self.pack_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class RuntimePayloadDescriptor:
    """Exact external runtime payload fetched from the pinned release."""

    id: str
    kind: str
    artifact_ref: str
    sha256: str
    bytes: int
    media_type: str

    def __post_init__(self) -> None:
        require_execution_token("runtime_payload.id", self.id)
        if self.kind not in {
            "dbt_project_bundle",
            "dbt_manifest",
            "dbt_selection_lock",
        }:
            raise ValueError("runtime_payload.kind is unsupported")
        cache_relative_path(self.artifact_ref)
        _require_digest("runtime_payload.sha256", self.sha256)
        _positive_integer(self.bytes, "runtime_payload.bytes")
        if not isinstance(self.media_type, str) or not self.media_type or len(self.media_type) > 200:
            raise ValueError("runtime_payload.media_type must be bounded text")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "media_type": self.media_type,
        }


@dataclass(frozen=True, slots=True)
class RuntimeInitFetchPlan:
    """Complete immutable plan shared by the init and base containers."""

    environment: str
    trust_tier: str
    release_id: str
    deployment_id: str
    runtime_image_ref: str
    runtime_image_digest: str
    artifact_registry_ref: str
    registry_config_ref: Mapping[str, str]
    trust_policy_ref: Mapping[str, str] | None
    identity: Mapping[str, str]
    release: RuntimeArtifactDescriptor
    deployment: RuntimeArtifactDescriptor
    binding_set: RuntimeArtifactDescriptor
    connection_registry: RuntimeArtifactDescriptor
    credential_runtime: RuntimeArtifactDescriptor
    workload_pack: RuntimeWorkloadPackRef
    execution: RuntimeExecutionSelection
    verify: Mapping[str, str]
    runtime_payloads: tuple[RuntimePayloadDescriptor, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "registry_config_ref",
            MappingProxyType(
                normalize_config_map_ref(
                    self.registry_config_ref,
                    field="registry.configuration",
                )
            ),
        )
        object.__setattr__(
            self,
            "trust_policy_ref",
            (
                MappingProxyType(
                    normalize_config_map_ref(
                        self.trust_policy_ref,
                        field="trust_policy",
                    )
                )
                if self.trust_policy_ref is not None
                else None
            ),
        )
        object.__setattr__(self, "identity", MappingProxyType(_identity(self.identity)))
        object.__setattr__(self, "verify", MappingProxyType(_verify(self.verify)))
        _validate_plan(self)

    @property
    def artifacts(
        self,
    ) -> tuple[
        RuntimeArtifactDescriptor | RuntimeWorkloadPackRef | RuntimePayloadDescriptor,
        ...,
    ]:
        return (
            self.deployment,
            self.release,
            self.workload_pack,
            self.binding_set,
            self.connection_registry,
            self.credential_runtime,
            *self.runtime_payloads,
        )

    def to_dict(self) -> dict[str, Any]:
        is_v3 = self.execution.hook_execution is not None
        payload: dict[str, Any] = {
            "schema": (
                RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3
                if is_v3
                else (RUNTIME_INIT_FETCH_PLAN_SCHEMA_V2 if self.runtime_payloads else RUNTIME_INIT_FETCH_PLAN_SCHEMA)
            ),
            "environment": self.environment,
            "trust_tier": self.trust_tier,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "runtime_image": {
                "ref": self.runtime_image_ref,
                "digest": self.runtime_image_digest,
            },
            "registry": {
                "logical_ref": self.artifact_registry_ref,
                "configuration": dict(self.registry_config_ref),
            },
            "trust_policy": (dict(self.trust_policy_ref) if self.trust_policy_ref is not None else None),
            "identity": dict(self.identity),
            "release": self.release.to_dict(),
            "deployment": self.deployment.to_dict(),
            "binding_set": self.binding_set.to_dict(),
            "connection_registry": self.connection_registry.to_dict(),
            "credential_runtime": self.credential_runtime.to_dict(),
            "workload_pack": self.workload_pack.to_dict(),
            "execution": self.execution.to_dict(),
            "verify": dict(self.verify),
        }
        if is_v3 or self.runtime_payloads:
            payload["runtime_payloads"] = [item.to_dict() for item in self.runtime_payloads]
        return payload


def plan_matches_declared_runtime_image(
    plan: RuntimeInitFetchPlan,
    deployment: Mapping[str, Any],
) -> bool:
    """Match the plan image against one deployment-set flavor, as a pair.

    Dual-digest deployments declare the canonical runtime image plus an
    optional ``runtime_image_dbt_*`` flavor selected for ``dbt__*`` workloads.
    The plan pins exactly one flavor; ref and digest must match the SAME
    declared pair (fail-closed against mixed or half-declared ref/digest
    combinations).
    """

    declared = [
        (deployment.get("runtime_image_ref"), deployment.get("runtime_image_digest")),
    ]
    dbt_ref = deployment.get("runtime_image_dbt_ref")
    dbt_digest = deployment.get("runtime_image_dbt_digest")
    if dbt_ref is not None or dbt_digest is not None:
        declared.append((dbt_ref, dbt_digest))
    return (plan.runtime_image_ref, plan.runtime_image_digest) in declared


def canonical_runtime_init_fetch_plan_bytes(plan: RuntimeInitFetchPlan) -> bytes:
    """Return the one canonical compact JSON representation."""

    return json.dumps(
        plan.to_dict(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def runtime_init_fetch_plan_sha256(plan: RuntimeInitFetchPlan) -> str:
    return "sha256:" + hashlib.sha256(canonical_runtime_init_fetch_plan_bytes(plan)).hexdigest()


def _validate_plan(plan: RuntimeInitFetchPlan) -> None:
    if not _ENVIRONMENT_RE.fullmatch(plan.environment):
        raise ValueError("environment must be a bounded lowercase logical name")
    if plan.trust_tier not in {"production", "non_production"}:
        raise ValueError("trust_tier must be production or non_production")
    if plan.trust_tier == "production" and plan.trust_policy_ref is None:
        raise ValueError("production trust_tier requires trust_policy")
    _require_digest("release_id", plan.release_id)
    _require_digest("deployment_id", plan.deployment_id)
    validate_runtime_image_reference(plan.runtime_image_ref, plan.runtime_image_digest)
    if not is_safe_artifact_registry_logical_ref(plan.artifact_registry_ref):
        raise ValueError("registry.logical_ref must be a bounded logical name")
    normalize_config_map_ref(plan.registry_config_ref, field="registry.configuration")
    if plan.trust_policy_ref is not None:
        normalize_config_map_ref(plan.trust_policy_ref, field="trust_policy")
    _identity(plan.identity)
    _verify(plan.verify)
    if plan.trust_tier == "production" and plan.verify["attestations"] != "required_for_prod":
        raise ValueError("production trust_tier requires attestations=required_for_prod")
    release_dir = plan.release_id.replace(":", "-")
    deployment_dir = plan.deployment_id.replace(":", "-")
    _require_artifact_prefix(plan.release, ("releases", release_dir))
    _require_artifact_prefix(plan.workload_pack, ("releases", release_dir))
    if len(plan.runtime_payloads) > MAX_SELECTED_RUNTIME_PAYLOADS:
        raise ValueError("runtime init-fetch plan selects too many runtime payloads")
    for payload in plan.runtime_payloads:
        _require_artifact_prefix(payload, ("releases", release_dir))
    payload_ids = tuple(payload.id for payload in plan.runtime_payloads)
    if len(set(payload_ids)) != len(payload_ids):
        raise ValueError("runtime init-fetch payload ids must be unique")
    _require_artifact_prefix(plan.deployment, ("deployments", plan.environment, deployment_dir))
    _require_runtime_connection_context(plan)
    artifact_refs = tuple(artifact.artifact_ref for artifact in plan.artifacts)
    if len(set(artifact_refs)) != len(artifact_refs):
        raise ValueError("runtime init-fetch artifact_ref values must be unique")
    if plan.workload_pack.id != plan.execution.selector:
        raise ValueError("execution.selector must match workload_pack.id")


def _identity(value: object) -> dict[str, str]:
    item = _mapping(value, "identity", {"method", "service_account", "namespace"})
    method = _text(item["method"], "identity.method")
    service_account = _text(item["service_account"], "identity.service_account")
    namespace = _text(item["namespace"], "identity.namespace")
    if method != "kubernetes_workload_identity":
        raise ValueError("identity.method must be kubernetes_workload_identity")
    if not is_valid_kubernetes_dns_label(service_account):
        raise ValueError("identity.service_account must be a Kubernetes DNS label")
    if not is_valid_kubernetes_dns_label(namespace):
        raise ValueError("identity.namespace must be a Kubernetes DNS label")
    return {"method": method, "service_account": service_account, "namespace": namespace}


def _verify(value: object) -> dict[str, str]:
    item = _mapping(value, "verify", {"checksums", "attestations"})
    checksums = _text(item["checksums"], "verify.checksums")
    attestations = _text(item["attestations"], "verify.attestations")
    if checksums != "required":
        raise ValueError("verify.checksums must be required")
    if attestations not in {"optional", "required_for_prod"}:
        raise ValueError("verify.attestations must be optional or required_for_prod")
    return {"checksums": checksums, "attestations": attestations}


def _mapping(value: object, field: str, keys: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    actual = frozenset(str(key) for key in value)
    if actual != frozenset(keys):
        raise ValueError(f"{field} must contain exactly {sorted(keys)}")
    return value


def _require_artifact_prefix(
    artifact: RuntimeArtifactDescriptor | RuntimeWorkloadPackRef | RuntimePayloadDescriptor,
    prefix: tuple[str, ...],
) -> None:
    relative = cache_relative_path(artifact.artifact_ref)
    if len(relative.parts) <= len(prefix) or relative.parts[: len(prefix)] != prefix:
        raise ValueError("artifact_ref is outside its pinned identity")


def _require_runtime_connection_context(plan: RuntimeInitFetchPlan) -> None:
    artifacts = (
        (plan.binding_set, "binding-set.json"),
        (plan.connection_registry, "connection-registry.json"),
        (plan.credential_runtime, "credential-runtime.json"),
    )
    parents: set[tuple[str, ...]] = set()
    for artifact, filename in artifacts:
        relative = cache_relative_path(artifact.artifact_ref)
        if (
            len(relative.parts) != 3
            or relative.parts[0] != "runtime-connection-contexts"
            or not _CONTEXT_DIGEST_DIR_RE.fullmatch(relative.parts[1])
            or relative.parts[2] != filename
        ):
            raise ValueError("runtime connection artifact_ref is outside its immutable context")
        parents.add(relative.parts[:-1])
    if len(parents) != 1:
        raise ValueError("runtime connection artifacts must share one immutable context")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _require_digest(field: str, value: object) -> None:
    if not is_canonical_sha256_digest(value):
        raise ValueError(f"{field} must be a canonical sha256 digest")


__all__ = [
    "canonical_runtime_init_fetch_plan_bytes",
    "plan_matches_declared_runtime_image",
    "MAX_RUNTIME_INIT_FETCH_PLAN_BYTES",
    "MAX_SELECTED_RUNTIME_PAYLOADS",
    "RUNTIME_INIT_FETCH_PLAN_SCHEMA",
    "RUNTIME_INIT_FETCH_PLAN_SCHEMA_V2",
    "RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3",
    "RuntimeArtifactDescriptor",
    "RuntimeExecutionSelection",
    "RuntimeInitFetchPlan",
    "RuntimePayloadDescriptor",
    "RuntimeWorkloadPackRef",
    "runtime_init_fetch_plan_sha256",
]
