"""Deployment attestation subject must accept the dual-digest dbt flavor.

Regression for the dual-digest cutover: ``dbt__*`` plans pin the
``runtime_image_dbt_*`` flavor while the deployment-set keeps the canonical
runtime image in ``runtime_image_ref``/``runtime_image_digest``. The
attestation subject previously compared the canonical deployment digest
strictly against ``plan.runtime_image_digest``, so every dbt workload failed
in any environment with deployment attestation enabled (the production trust
tier requires it).

The observed subject must bind the canonical digest read from the verified
deployment bytes because that is the value the signed attestation statement
commits to (see ``AirflowArtifactAttestationBuilder``); the plan image is
validated as a (ref, digest) pair against exactly one declared flavor.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)

from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
    deployment_id,
    release_id,
)
from dpone.runtime.airflow_deployment_attestation_subject import (
    subject_from_runtime_artifacts,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_attestation import StagedRuntimeArtifact
from dpone.runtime.runtime_init_fetch_plan import (
    RuntimeArtifactDescriptor,
    RuntimeExecutionSelection,
    RuntimeInitFetchPlan,
    RuntimeWorkloadPackRef,
)
from dpone.runtime.runtime_init_fetch_receipts import validate_runtime_receipts

CANONICAL_DIGEST = "sha256:" + "1" * 64
CANONICAL_REF = "registry.example/dpone/runtime@" + CANONICAL_DIGEST
DBT_DIGEST = "sha256:" + "2" * 64
DBT_REF = "registry.example/dpone/runtime-dbt@" + DBT_DIGEST
CONTEXT_DIR = "sha256-" + "3" * 64
REGISTRY_SCOPE_ID = "sha256:" + "4" * 64
ENVIRONMENT = "prod"
WORKLOAD_ID = "dbt__daily_marts"
REGISTRY_REF = "dpone-artifacts"

REGISTRY_CONFIG_REF = {
    "kind": "kubernetes_config_map",
    "name": "dpone-artifact-registry",
    "key": "registry.json",
    "sha256": "sha256:" + "5" * 64,
}
TRUST_POLICY_REF = {
    "kind": "kubernetes_config_map",
    "name": "dpone-artifact-trust",
    "key": "trust-policy.json",
    "sha256": "sha256:" + "6" * 64,
}
IDENTITY = {
    "method": "kubernetes_workload_identity",
    "service_account": "dpone-runtime",
    "namespace": "data-platform",
}
VERIFY = {"checksums": "required", "attestations": "required_for_prod"}


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


@dataclass(frozen=True)
class _Receipts:
    """One consistent production deployment declaring both image flavors."""

    plan_dbt: RuntimeInitFetchPlan
    plan_canonical: RuntimeInitFetchPlan
    release: dict[str, Any]
    deployment: dict[str, Any]
    pack: dict[str, Any]
    connections: dict[str, dict[str, Any]]

    def payloads(self, plan: RuntimeInitFetchPlan) -> dict[str, bytes]:
        return {
            plan.release.artifact_ref: _json_bytes(self.release),
            plan.deployment.artifact_ref: _json_bytes(self.deployment),
            plan.workload_pack.artifact_ref: _json_bytes(self.pack),
            plan.binding_set.artifact_ref: _json_bytes(self.connections["binding_set"]),
            plan.connection_registry.artifact_ref: _json_bytes(self.connections["connection_registry"]),
            plan.credential_runtime.artifact_ref: _json_bytes(self.connections["credential_runtime"]),
        }


def _build_receipts() -> _Receipts:
    connections = {
        "binding_set": {"schema": "dpone.binding-set.v1", "environment": ENVIRONMENT},
        "connection_registry": {
            "schema": "dpone.connection-registry.v1",
            "environment": ENVIRONMENT,
        },
        "credential_runtime": {
            "schema": "dpone.credential-runtime.v1",
            "environment": ENVIRONMENT,
        },
    }
    context_root = f"cache://runtime-connection-contexts/{CONTEXT_DIR}"
    connection_files = {
        "binding_set": "binding-set.json",
        "connection_registry": "connection-registry.json",
        "credential_runtime": "credential-runtime.json",
    }
    connection_descriptors = {
        name: {
            "artifact_ref": f"{context_root}/{connection_files[name]}",
            "sha256": _sha256(_json_bytes(payload)),
            "bytes": len(_json_bytes(payload)),
        }
        for name, payload in connections.items()
    }

    pack: dict[str, Any] = {
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "workload": {"workload_id": WORKLOAD_ID},
    }
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    pack_bytes = _json_bytes(pack)

    release: dict[str, Any] = {
        "schema": "dpone.release-set.v2",
        "artifacts": {
            "workload_packs": [
                {
                    "id": WORKLOAD_ID,
                    "sha256": _sha256(pack_bytes),
                    "path": f"packs/{WORKLOAD_ID}.json",
                }
            ],
        },
    }
    release["release_id"] = release_id(release)
    release_dir = release["release_id"].replace(":", "-")

    deployment: dict[str, Any] = {
        "schema": "dpone.deployment-set.v2",
        "environment": ENVIRONMENT,
        "release_ref": release["release_id"],
        "runtime_image_ref": CANONICAL_REF,
        "runtime_image_digest": CANONICAL_DIGEST,
        "runtime_image_dbt_ref": DBT_REF,
        "runtime_image_dbt_digest": DBT_DIGEST,
        "runtime_artifact_delivery": {
            "mode": "init_fetch",
            "artifact_registry_ref": REGISTRY_REF,
            "trust_tier": "production",
            "registry_config_ref": dict(REGISTRY_CONFIG_REF),
            "trust_policy_ref": dict(TRUST_POLICY_REF),
            "identity": dict(IDENTITY),
            "verify": dict(VERIFY),
        },
        "binding_set": connection_descriptors["binding_set"],
        "connection_registry": connection_descriptors["connection_registry"],
        "credential_runtime": connection_descriptors["credential_runtime"],
        "binding_set_ref": canonical_fingerprint(connections["binding_set"]),
        "connection_registry_ref": canonical_fingerprint(connections["connection_registry"]),
        "credential_runtime_ref": canonical_fingerprint(connections["credential_runtime"]),
        "workloads": [
            {
                "id": WORKLOAD_ID,
                "sha256": _sha256(pack_bytes),
                "pack_fingerprint": pack["pack_fingerprint"],
            }
        ],
    }
    deployment["deployment_id"] = deployment_id(deployment)
    deployment_dir = deployment["deployment_id"].replace(":", "-")
    deployment_bytes = _json_bytes(deployment)
    release_bytes = _json_bytes(release)

    def _plan(image_ref: str, image_digest: str) -> RuntimeInitFetchPlan:
        return RuntimeInitFetchPlan(
            environment=ENVIRONMENT,
            trust_tier="production",
            release_id=release["release_id"],
            deployment_id=deployment["deployment_id"],
            runtime_image_ref=image_ref,
            runtime_image_digest=image_digest,
            artifact_registry_ref=REGISTRY_REF,
            registry_config_ref=dict(REGISTRY_CONFIG_REF),
            trust_policy_ref=dict(TRUST_POLICY_REF),
            identity=dict(IDENTITY),
            release=RuntimeArtifactDescriptor(
                artifact_ref=f"cache://releases/{release_dir}/release-set.json",
                sha256=_sha256(release_bytes),
                bytes=len(release_bytes),
            ),
            deployment=RuntimeArtifactDescriptor(
                artifact_ref=f"cache://deployments/{ENVIRONMENT}/{deployment_dir}/deployment.json",
                sha256=_sha256(deployment_bytes),
                bytes=len(deployment_bytes),
            ),
            binding_set=RuntimeArtifactDescriptor(**connection_descriptors["binding_set"]),
            connection_registry=RuntimeArtifactDescriptor(**connection_descriptors["connection_registry"]),
            credential_runtime=RuntimeArtifactDescriptor(**connection_descriptors["credential_runtime"]),
            workload_pack=RuntimeWorkloadPackRef(
                id=WORKLOAD_ID,
                artifact_ref=f"cache://releases/{release_dir}/packs/{WORKLOAD_ID}.json",
                sha256=_sha256(pack_bytes),
                bytes=len(pack_bytes),
                pack_fingerprint=pack["pack_fingerprint"],
            ),
            execution=RuntimeExecutionSelection(kind="runtime", selector=WORKLOAD_ID),
            verify=dict(VERIFY),
        )

    return _Receipts(
        plan_dbt=_plan(DBT_REF, DBT_DIGEST),
        plan_canonical=_plan(CANONICAL_REF, CANONICAL_DIGEST),
        release=release,
        deployment=deployment,
        pack=pack,
        connections=connections,
    )


@dataclass(frozen=True)
class _PlanStub:
    """Plan double for image pairs the real plan contract cannot represent.

    ``RuntimeInitFetchPlan`` itself rejects mixed ref/digest pairs, so the
    subject-level fail-closed behavior can only be exercised with a stub.
    """

    runtime_image_ref: str
    runtime_image_digest: str
    release_id: str
    deployment_id: str
    environment: str
    artifact_registry_ref: str
    release: RuntimeArtifactDescriptor
    deployment: RuntimeArtifactDescriptor


def _stub(plan: RuntimeInitFetchPlan, image_ref: str, image_digest: str) -> _PlanStub:
    return _PlanStub(
        runtime_image_ref=image_ref,
        runtime_image_digest=image_digest,
        release_id=plan.release_id,
        deployment_id=plan.deployment_id,
        environment=plan.environment,
        artifact_registry_ref=plan.artifact_registry_ref,
        release=plan.release,
        deployment=plan.deployment,
    )


def _staged(
    tmp_path: Path,
    plan: Any,
    *,
    release: dict[str, Any],
    deployment: dict[str, Any],
) -> tuple[StagedRuntimeArtifact, ...]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    release_path = tmp_path / "release-set.json"
    deployment_path = tmp_path / "deployment.json"
    release_path.write_bytes(_json_bytes(release))
    deployment_path.write_bytes(_json_bytes(deployment))
    return (
        StagedRuntimeArtifact(descriptor=plan.release, path=release_path),
        StagedRuntimeArtifact(descriptor=plan.deployment, path=deployment_path),
    )


def test_dbt_flavor_plan_builds_subject_bound_to_canonical_digest(tmp_path: Path) -> None:
    receipts = _build_receipts()
    staged = _staged(tmp_path, receipts.plan_dbt, release=receipts.release, deployment=receipts.deployment)

    subject = subject_from_runtime_artifacts(
        plan=receipts.plan_dbt,
        staged_artifacts=staged,
        registry_scope_id=REGISTRY_SCOPE_ID,
    )

    assert subject.runtime_image_digest == CANONICAL_DIGEST
    assert subject.deployment_sha256 == _sha256(_json_bytes(receipts.deployment))
    assert subject.release_set_sha256 == _sha256(_json_bytes(receipts.release))


def test_canonical_plan_builds_subject_with_and_without_dbt_flavor(tmp_path: Path) -> None:
    receipts = _build_receipts()
    without_dbt = dict(receipts.deployment)
    del without_dbt["runtime_image_dbt_ref"]
    del without_dbt["runtime_image_dbt_digest"]

    for index, deployment in enumerate((receipts.deployment, without_dbt)):
        staged = _staged(
            tmp_path / str(index),
            receipts.plan_canonical,
            release=receipts.release,
            deployment=deployment,
        )
        subject = subject_from_runtime_artifacts(
            plan=receipts.plan_canonical,
            staged_artifacts=staged,
            registry_scope_id=REGISTRY_SCOPE_ID,
        )
        assert subject.runtime_image_digest == CANONICAL_DIGEST


@pytest.mark.parametrize(
    ("image_ref", "image_digest"),
    [
        (DBT_REF, CANONICAL_DIGEST),
        (CANONICAL_REF, DBT_DIGEST),
    ],
)
def test_mixed_flavor_pairs_fail_closed_both_directions(
    tmp_path: Path,
    image_ref: str,
    image_digest: str,
) -> None:
    receipts = _build_receipts()
    plan = _stub(receipts.plan_dbt, image_ref, image_digest)
    staged = _staged(tmp_path, plan, release=receipts.release, deployment=receipts.deployment)

    with pytest.raises(InitFetchError) as exc:
        subject_from_runtime_artifacts(
            plan=plan,
            staged_artifacts=staged,
            registry_scope_id=REGISTRY_SCOPE_ID,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
    assert image_digest in str(exc.value)


def test_plan_contract_itself_rejects_mixed_image_pairs() -> None:
    receipts = _build_receipts()

    with pytest.raises(ValueError, match="runtime image digest"):
        replace(
            receipts.plan_dbt,
            runtime_image_ref=DBT_REF,
            runtime_image_digest=CANONICAL_DIGEST,
        )


@pytest.mark.parametrize("half_declared_field", ["runtime_image_dbt_ref", "runtime_image_dbt_digest"])
def test_half_declared_dbt_pair_fails_closed(tmp_path: Path, half_declared_field: str) -> None:
    receipts = _build_receipts()
    deployment = dict(receipts.deployment)
    removed = "runtime_image_dbt_digest" if half_declared_field == "runtime_image_dbt_ref" else "runtime_image_dbt_ref"
    del deployment[removed]
    staged = _staged(tmp_path, receipts.plan_dbt, release=receipts.release, deployment=deployment)

    with pytest.raises(InitFetchError) as exc:
        subject_from_runtime_artifacts(
            plan=receipts.plan_dbt,
            staged_artifacts=staged,
            registry_scope_id=REGISTRY_SCOPE_ID,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_plan_identity_mismatch_raises_coded_error(tmp_path: Path) -> None:
    receipts = _build_receipts()
    deployment = dict(receipts.deployment)
    deployment["environment"] = "dev"
    staged = _staged(tmp_path, receipts.plan_dbt, release=receipts.release, deployment=deployment)

    with pytest.raises(InitFetchError) as exc:
        subject_from_runtime_artifacts(
            plan=receipts.plan_dbt,
            staged_artifacts=staged,
            registry_scope_id=REGISTRY_SCOPE_ID,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_missing_staged_roots_raise_coded_error() -> None:
    receipts = _build_receipts()

    with pytest.raises(InitFetchError) as exc:
        subject_from_runtime_artifacts(
            plan=receipts.plan_dbt,
            staged_artifacts=(),
            registry_scope_id=REGISTRY_SCOPE_ID,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_validate_runtime_receipts_accepts_dbt_flavor_payload() -> None:
    """Full entry-point regression for the path that failed in production."""

    receipts = _build_receipts()

    pack, fingerprint = validate_runtime_receipts(
        receipts.plan_dbt,
        receipts.payloads(receipts.plan_dbt),
    )

    assert pack["workload"]["workload_id"] == WORKLOAD_ID
    assert fingerprint == receipts.pack["pack_fingerprint"]


def test_validate_runtime_receipts_still_accepts_canonical_plan() -> None:
    receipts = _build_receipts()

    pack, fingerprint = validate_runtime_receipts(
        receipts.plan_canonical,
        receipts.payloads(receipts.plan_canonical),
    )

    assert pack["workload"]["workload_id"] == WORKLOAD_ID
    assert fingerprint == receipts.pack["pack_fingerprint"]
