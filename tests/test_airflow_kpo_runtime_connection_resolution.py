from __future__ import annotations

import base64

import pytest
from dpone_airflow_pack.init_fetch_contract import (
    InitFetchProviderError,
    init_fetch_context_from_payload,
)

from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan

_RELEASE_ID = "sha256:" + "a" * 64
_DEPLOYMENT_ID = "sha256:" + "b" * 64
_IMAGE_DIGEST = "sha256:" + "c" * 64
_CONTEXT_DIR = "sha256-" + "d" * 64


def test_provider_handoff_contains_descriptors_without_runtime_connection_bodies() -> None:
    payload = _index()

    context = init_fetch_context_from_payload(payload)
    encoded = context.encode_plan(
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    plan, _ = decode_runtime_init_fetch_plan(encoded.base64, encoded.sha256)

    assert plan.binding_set.to_dict() == payload["binding_set"]
    assert plan.connection_registry.to_dict() == payload["connection_registry"]
    assert plan.credential_runtime.to_dict() == payload["credential_runtime"]
    serialized = base64.b64decode(encoded.base64, validate=True)
    assert b"teams/example/source-main" not in serialized
    assert b"https://vault.internal" not in serialized
    assert b'"connections"' not in serialized


def test_provider_rejects_legacy_ref_as_strict_runtime_connection_authority() -> None:
    payload = _index()
    connection_registry = payload["connection_registry"]
    assert isinstance(connection_registry, dict)
    connection_registry["artifact_ref"] = (
        f"cache://deployments/prod/{_DEPLOYMENT_ID.replace(':', '-')}/connection-registry.ref"
    )

    with pytest.raises(InitFetchProviderError) as exc:
        init_fetch_context_from_payload(payload)

    assert exc.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"


def _index() -> dict[str, object]:
    release_dir = _RELEASE_ID.replace(":", "-")
    deployment_dir = _DEPLOYMENT_ID.replace(":", "-")
    context_root = f"cache://runtime-connection-contexts/{_CONTEXT_DIR}"
    return {
        "schema": "dpone.airflow-deployment-index.v2",
        "release_id": _RELEASE_ID,
        "deployment_id": _DEPLOYMENT_ID,
        "trust_tier": "production",
        "dag_specs": [],
        "workload_packs": [
            {
                "id": "orders",
                "artifact_ref": f"cache://releases/{release_dir}/packs/orders.json",
                "sha256": "sha256:" + "1" * 64,
                "bytes": 128,
                "pack_fingerprint": "sha256:" + "2" * 64,
            }
        ],
        "binding_set_ref": "sha256:" + "3" * 64,
        "connection_registry_ref": "sha256:" + "4" * 64,
        "credential_runtime_ref": "sha256:" + "5" * 64,
        "binding_set": _artifact(f"{context_root}/binding-set.json", "6"),
        "connection_registry": _artifact(f"{context_root}/connection-registry.json", "7"),
        "credential_runtime": _artifact(f"{context_root}/credential-runtime.json", "8"),
        "runtime_image_ref": f"registry.example/dpone/runtime@{_IMAGE_DIGEST}",
        "runtime_image_digest": _IMAGE_DIGEST,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": {
            "mode": "init_fetch",
            "trust_tier": "production",
            "artifact_registry_ref": "dpone-artifacts",
            "identity": {
                "method": "kubernetes_workload_identity",
                "service_account": "dpone-runtime",
                "namespace": "data-platform",
            },
            "registry_config_ref": {
                "kind": "kubernetes_config_map",
                "name": "dpone-artifact-registry",
                "key": "registry.json",
                "sha256": "sha256:" + "9" * 64,
            },
            "trust_policy_ref": {
                "kind": "kubernetes_config_map",
                "name": "dpone-artifact-trust",
                "key": "policy.json",
                "sha256": "sha256:" + "a" * 64,
            },
            "source": {"artifact_registry_ref": "dpone-artifacts"},
            "verify": {
                "checksums": "required",
                "attestations": "required_for_prod",
            },
        },
        "release": _artifact(
            f"cache://releases/{release_dir}/release-set.json",
            "b",
        ),
        "deployment": _artifact(
            f"cache://deployments/prod/{deployment_dir}/deployment.json",
            "c",
        ),
    }


def _artifact(artifact_ref: str, digest_character: str) -> dict[str, object]:
    return {
        "artifact_ref": artifact_ref,
        "sha256": "sha256:" + digest_character * 64,
        "bytes": 128,
    }
