"""Deployment-mirror validation must accept the dual-digest dbt flavor.

Regression for the dual-digest cutover: ``dbt__*`` plans pin the
``runtime_image_dbt_*`` flavor while the deployment-set keeps the canonical
runtime image in ``runtime_image_ref``/``runtime_image_digest`` and declares
the dbt flavor separately. The mirror check previously compared the plan
against the canonical pair only, failing every dbt workload with
``DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED: deployment-set runtime projection
does not match the pinned plan``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import plan_matches_declared_runtime_image
from dpone.runtime.runtime_init_fetch_receipts import _validate_deployment_mirror

FULL_REF = "registry.example/dpone@sha256:" + "a" * 64
FULL_DIGEST = "sha256:" + "a" * 64
DBT_REF = "registry.example/dpone-dbt@sha256:" + "b" * 64
DBT_DIGEST = "sha256:" + "b" * 64


@dataclass(frozen=True)
class _Artifact:
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)


@dataclass(frozen=True)
class _WorkloadPack:
    id: str = "dbt__example"
    sha256: str = "sha256:" + "c" * 64
    pack_fingerprint: str = "sha256:" + "d" * 64


@dataclass(frozen=True)
class _PlanStub:
    runtime_image_ref: str
    runtime_image_digest: str
    release_id: str = "sha256:" + "e" * 64
    artifact_registry_ref: str = "dpone-dev-artifacts"
    trust_tier: str = "production"
    registry_config_ref: dict[str, Any] = None  # type: ignore[assignment]
    trust_policy_ref: dict[str, Any] | None = None
    identity: dict[str, Any] = None  # type: ignore[assignment]
    verify: dict[str, Any] = None  # type: ignore[assignment]
    binding_set: _Artifact = None  # type: ignore[assignment]
    connection_registry: _Artifact = None  # type: ignore[assignment]
    credential_runtime: _Artifact = None  # type: ignore[assignment]
    workload_pack: _WorkloadPack = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "registry_config_ref", self.registry_config_ref or {"name": "registry"})
        object.__setattr__(self, "identity", self.identity or {"method": "kubernetes_workload_identity"})
        object.__setattr__(self, "verify", self.verify or {"checksums": "required"})
        object.__setattr__(self, "binding_set", self.binding_set or _Artifact({"artifact_ref": "cache://b"}))
        object.__setattr__(
            self, "connection_registry", self.connection_registry or _Artifact({"artifact_ref": "cache://c"})
        )
        object.__setattr__(
            self, "credential_runtime", self.credential_runtime or _Artifact({"artifact_ref": "cache://d"})
        )
        object.__setattr__(self, "workload_pack", self.workload_pack or _WorkloadPack())


def _deployment(plan: _PlanStub, *, dbt_flavor: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "release_ref": plan.release_id,
        "runtime_image_ref": FULL_REF,
        "runtime_image_digest": FULL_DIGEST,
        "runtime_artifact_delivery": {
            "mode": "init_fetch",
            "artifact_registry_ref": plan.artifact_registry_ref,
            "trust_tier": plan.trust_tier,
            "registry_config_ref": dict(plan.registry_config_ref),
            "trust_policy_ref": None,
            "identity": dict(plan.identity),
            "verify": dict(plan.verify),
        },
        "binding_set": plan.binding_set.to_dict(),
        "connection_registry": plan.connection_registry.to_dict(),
        "credential_runtime": plan.credential_runtime.to_dict(),
        "workloads": [
            {
                "id": plan.workload_pack.id,
                "sha256": plan.workload_pack.sha256,
                "pack_fingerprint": plan.workload_pack.pack_fingerprint,
            }
        ],
    }
    if dbt_flavor:
        payload["runtime_image_dbt_ref"] = DBT_REF
        payload["runtime_image_dbt_digest"] = DBT_DIGEST
    return payload


def test_dbt_plan_passes_when_deployment_declares_dbt_flavor() -> None:
    plan = _PlanStub(runtime_image_ref=DBT_REF, runtime_image_digest=DBT_DIGEST)
    _validate_deployment_mirror(plan, _deployment(plan, dbt_flavor=True))


def test_canonical_plan_still_passes_with_and_without_dbt_flavor() -> None:
    plan = _PlanStub(runtime_image_ref=FULL_REF, runtime_image_digest=FULL_DIGEST)
    _validate_deployment_mirror(plan, _deployment(plan, dbt_flavor=True))
    _validate_deployment_mirror(plan, _deployment(plan, dbt_flavor=False))


def test_dbt_plan_fails_closed_when_deployment_lacks_dbt_flavor() -> None:
    plan = _PlanStub(runtime_image_ref=DBT_REF, runtime_image_digest=DBT_DIGEST)
    with pytest.raises(InitFetchError) as exc:
        _validate_deployment_mirror(plan, _deployment(plan, dbt_flavor=False))
    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_mixed_flavor_pair_is_rejected() -> None:
    plan = _PlanStub(runtime_image_ref=DBT_REF, runtime_image_digest=FULL_DIGEST)
    deployment = _deployment(plan, dbt_flavor=True)
    assert not plan_matches_declared_runtime_image(plan, deployment)
    with pytest.raises(InitFetchError) as exc:
        _validate_deployment_mirror(plan, deployment)
    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_unknown_image_pair_is_rejected() -> None:
    plan = _PlanStub(
        runtime_image_ref="registry.example/other@sha256:" + "f" * 64,
        runtime_image_digest="sha256:" + "f" * 64,
    )
    with pytest.raises(InitFetchError) as exc:
        _validate_deployment_mirror(plan, _deployment(plan, dbt_flavor=True))
    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
