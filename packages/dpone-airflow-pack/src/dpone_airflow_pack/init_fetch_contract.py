"""Provider-only DTOs for strict indexed ``init_fetch`` execution.

The module intentionally depends only on the lightweight Airflow provider.  It
validates the v2 wire projection and creates the complete, immutable runtime
plan before any Airflow DAG is installed.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError

AIRFLOW_INDEX_SCHEMA_V2 = "dpone.airflow-deployment-index.v2"
AIRFLOW_INDEX_SCHEMA_V3 = "dpone.airflow-deployment-index.v3"
RUNTIME_INIT_FETCH_PLAN_SCHEMA = "dpone.airflow-runtime-init-fetch-plan.v1"
RUNTIME_INIT_FETCH_PLAN_SCHEMA_V2 = "dpone.airflow-runtime-init-fetch-plan.v2"
RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3 = "dpone.airflow-runtime-init-fetch-plan.v3"
MAX_RUNTIME_INIT_FETCH_PLAN_BYTES = 16 * 1024
MAX_SELECTED_RUNTIME_PAYLOADS = 16


class InitFetchProviderError(AirflowDeploymentIndexError):
    """Stable parse-time failure for the provider-owned strict lane."""


@dataclass(frozen=True, slots=True)
class ConfigMapReference:
    """Digest-pinned, non-secret reference to one ConfigMap file."""

    kind: str
    name: str
    key: str
    sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "name": self.name,
            "key": self.key,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class ExactArtifact:
    """Exact immutable artifact descriptor mirrored into the runtime plan."""

    artifact_ref: str
    sha256: str
    bytes: int

    def to_dict(self) -> dict[str, str | int]:
        return {
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "bytes": self.bytes,
        }


@dataclass(frozen=True, slots=True)
class ExactWorkloadPack:
    """Exact workload pack and its canonical semantic fingerprint."""

    id: str
    artifact_ref: str
    sha256: str
    bytes: int
    pack_fingerprint: str
    runtime_payload_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, str | int]:
        return {
            "id": self.id,
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "pack_fingerprint": self.pack_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class ExactRuntimePayload:
    """External runtime payload selected by logical id from one release."""

    id: str
    kind: str
    artifact_ref: str
    sha256: str
    bytes: int
    media_type: str

    def to_dict(self) -> dict[str, str | int]:
        return {
            "id": self.id,
            "kind": self.kind,
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "media_type": self.media_type,
        }


@dataclass(frozen=True, slots=True)
class WorkloadIdentity:
    method: str
    service_account: str
    namespace: str

    def to_dict(self) -> dict[str, str]:
        return {
            "method": self.method,
            "service_account": self.service_account,
            "namespace": self.namespace,
        }


@dataclass(frozen=True, slots=True)
class VerificationPolicy:
    checksums: str
    attestations: str

    def to_dict(self) -> dict[str, str]:
        return {
            "checksums": self.checksums,
            "attestations": self.attestations,
        }


@dataclass(frozen=True, slots=True)
class DevEvidenceDelivery:
    """Deployment-owned shared volume and dedicated terminal worker lane."""

    mode: str
    claim_name: str
    mount_path: str
    worker_queue: str

    def to_dict(self) -> dict[str, str]:
        return {
            "mode": self.mode,
            "claim_name": self.claim_name,
            "mount_path": self.mount_path,
            "worker_queue": self.worker_queue,
        }


@dataclass(frozen=True, slots=True)
class EncodedInitFetchPlan:
    """Canonical plan bytes and their immutable transport projections."""

    payload: bytes
    base64: str
    sha256: str


@dataclass(frozen=True, slots=True)
class InitFetchDeliveryContext:
    """One immutable v2 delivery context shared by every generated operator."""

    environment: str
    trust_tier: str
    release_id: str
    deployment_id: str
    runtime_image_ref: str
    runtime_image_digest: str
    artifact_registry_ref: str
    registry_configuration: ConfigMapReference
    trust_policy: ConfigMapReference | None
    identity: WorkloadIdentity
    release: ExactArtifact
    deployment: ExactArtifact
    binding_set: ExactArtifact
    connection_registry: ExactArtifact
    credential_runtime: ExactArtifact
    workload_packs: tuple[ExactWorkloadPack, ...]
    runtime_payloads: tuple[ExactRuntimePayload, ...]
    verify: VerificationPolicy
    dev_evidence_delivery: DevEvidenceDelivery | None
    runtime_image_dbt_ref: str | None = None
    runtime_image_dbt_digest: str | None = None
    mssql_asset_uri_by_ref: Mapping[str, str] | None = None

    def workload_pack(self, workload_id: str) -> ExactWorkloadPack:
        for workload in self.workload_packs:
            if workload.id == workload_id:
                return workload
        raise _contract_error(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            "selected workload is absent from the strict deployment index",
            path=workload_id,
        )

    def runtime_image_for_workload(self, workload_id: str) -> tuple[str, str]:
        """Select full or dbt flavor image for one workload id."""

        if (
            workload_id.startswith("dbt__")
            and self.runtime_image_dbt_ref is not None
            and self.runtime_image_dbt_digest is not None
        ):
            return self.runtime_image_dbt_ref, self.runtime_image_dbt_digest
        return self.runtime_image_ref, self.runtime_image_digest

    def selected_runtime_payloads(
        self,
        workload: ExactWorkloadPack,
    ) -> tuple[ExactRuntimePayload, ...]:
        selected = []
        by_id = {item.id: item for item in self.runtime_payloads}
        for payload_id in workload.runtime_payload_ids:
            payload = by_id.get(payload_id)
            if payload is None:
                raise _contract_error(
                    "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                    "workload references an absent runtime payload",
                    path=payload_id,
                )
            selected.append(payload)
        if len(selected) > MAX_SELECTED_RUNTIME_PAYLOADS:
            raise _contract_error(
                "DPONE_INIT_FETCH_PLAN_TOO_LARGE",
                "workload selects too many external runtime payloads",
            )
        return tuple(selected)

    def encode_plan(
        self,
        *,
        workload_id: str,
        execution_kind: str,
        execution_scope: str,
        hook_execution: str,
        process_selector: str | None = None,
        hook_name: str | None = None,
    ) -> EncodedInitFetchPlan:
        workload = self.workload_pack(workload_id)
        runtime_payloads = self.selected_runtime_payloads(workload)
        if execution_kind not in {"runtime", "pre_hook"}:
            raise _contract_error(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "execution kind must be runtime or pre_hook",
            )
        if execution_scope not in {"workload", "process"}:
            raise _contract_error(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "execution scope must be workload or process",
            )
        if execution_kind == "pre_hook":
            _require_execution_token(hook_name, "execution.hook_name")
        elif hook_name is not None:
            raise _contract_error(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "execution.hook_name is only valid for pre_hook",
            )
        if process_selector is not None:
            _require_execution_token(process_selector, "execution.process_selector")
        if execution_scope == "workload" and process_selector is not None:
            raise _contract_error(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "workload execution cannot select one process",
            )
        if execution_scope == "workload" and hook_execution != "externalized":
            raise _contract_error(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "workload execution must externalize hooks",
            )
        if hook_execution not in {"inline", "externalized"}:
            raise _contract_error(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "execution.hook_execution must be inline or externalized",
            )
        if execution_kind == "pre_hook" and hook_execution != "externalized":
            raise _contract_error(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "pre_hook execution must be externalized",
            )
        image_ref, image_digest = self.runtime_image_for_workload(workload_id)
        plan: dict[str, Any] = {
            "schema": RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3,
            "environment": self.environment,
            "trust_tier": self.trust_tier,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "runtime_image": {
                "ref": image_ref,
                "digest": image_digest,
            },
            "registry": {
                "logical_ref": self.artifact_registry_ref,
                "configuration": self.registry_configuration.to_dict(),
            },
            "trust_policy": self.trust_policy.to_dict() if self.trust_policy is not None else None,
            "identity": self.identity.to_dict(),
            "release": self.release.to_dict(),
            "deployment": self.deployment.to_dict(),
            "binding_set": self.binding_set.to_dict(),
            "connection_registry": self.connection_registry.to_dict(),
            "credential_runtime": self.credential_runtime.to_dict(),
            "workload_pack": workload.to_dict(),
            "execution": {
                "kind": execution_kind,
                "selector": workload.id,
                "scope": execution_scope,
                "process_selector": process_selector,
                "hook_name": hook_name,
                "hook_execution": hook_execution,
            },
            "verify": self.verify.to_dict(),
            "runtime_payloads": [payload.to_dict() for payload in runtime_payloads],
        }
        payload = json.dumps(
            plan,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(payload) > MAX_RUNTIME_INIT_FETCH_PLAN_BYTES:
            raise _contract_error(
                "DPONE_INIT_FETCH_PLAN_TOO_LARGE",
                "runtime init-fetch plan exceeds the 16KiB provider limit",
            )
        return EncodedInitFetchPlan(
            payload=payload,
            base64=base64.b64encode(payload).decode("ascii"),
            sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
        )


def init_fetch_context_from_payload(
    payload: Mapping[str, Any],
    *,
    path: Path | None = None,
) -> InitFetchDeliveryContext:
    """Validate the strict v2 wire projection without importing core dpone."""

    from dpone_airflow_pack.init_fetch_wire import (
        init_fetch_context_from_payload as parse,
    )

    return parse(payload, path=path)


def _contract_error(
    code: str,
    message: str,
    *,
    path: str | None = None,
) -> InitFetchProviderError:
    return InitFetchProviderError(code, message, path=path)


def _require_execution_token(value: object, field: str) -> None:
    from dpone_airflow_pack.init_fetch_validation import execution_token

    execution_token(value, field, None)


__all__ = [
    "AIRFLOW_INDEX_SCHEMA_V2",
    "AIRFLOW_INDEX_SCHEMA_V3",
    "ConfigMapReference",
    "DevEvidenceDelivery",
    "EncodedInitFetchPlan",
    "ExactArtifact",
    "ExactRuntimePayload",
    "ExactWorkloadPack",
    "InitFetchDeliveryContext",
    "InitFetchProviderError",
    "MAX_RUNTIME_INIT_FETCH_PLAN_BYTES",
    "MAX_SELECTED_RUNTIME_PAYLOADS",
    "RUNTIME_INIT_FETCH_PLAN_SCHEMA",
    "RUNTIME_INIT_FETCH_PLAN_SCHEMA_V2",
    "RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3",
    "VerificationPolicy",
    "WorkloadIdentity",
    "init_fetch_context_from_payload",
]
