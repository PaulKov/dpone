from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from dpone_airflow_pack.deployment_index import (
    AirflowDeploymentIndexError,
    load_airflow_deployment_index,
)
from dpone_airflow_pack.run_identity import (
    build_dag_run_identity_context,
    build_task_group_run_identity_context,
    build_workload_run_identity,
    serialize_run_identity,
)

from dpone.contracts.airflow_run_identity import (
    AirflowRunIdentity,
    AirflowRunIdentityError,
    parse_airflow_run_identity_json,
)
from dpone.gitops.schema_contracts import get_gitops_schema_contract

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
DAG_SPEC_SHA256 = "sha256:" + "c" * 64
PACK_SHA256 = "sha256:" + "d" * 64
IMAGE_DIGEST = "sha256:" + "e" * 64
BINDING_REF = "sha256:" + "1" * 64
REGISTRY_REF = "sha256:" + "2" * 64
CREDENTIAL_RUNTIME_REF = "sha256:" + "3" * 64
PACK_FINGERPRINT = "sha256:" + "4" * 64
RELEASE_DIR = RELEASE_ID.replace(":", "-")
DEPLOYMENT_DIR = DEPLOYMENT_ID.replace(":", "-")
IMAGE_REF = f"registry.example/dpone/runtime@{IMAGE_DIGEST}"


def _identity_payload() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "dag_spec": {"id": "orders_daily", "sha256": DAG_SPEC_SHA256},
        "workload_pack": {"id": "load_orders", "sha256": PACK_SHA256},
        "runtime_image_digest": IMAGE_DIGEST,
        "binding_set_ref": BINDING_REF,
        "connection_registry_ref": REGISTRY_REF,
        "credential_runtime_ref": CREDENTIAL_RUNTIME_REF,
        "airflow_bundle": {
            "backend": "git",
            "ref": "git:7ac31f2",
            "versioned": True,
            "version": "7ac31f2",
            "snapshot_ref": None,
        },
    }


def test_run_identity_round_trips_provider_canonical_json() -> None:
    parsed = parse_airflow_run_identity_json(json.dumps(_identity_payload()))

    assert parsed == AirflowRunIdentity.from_mapping(_identity_payload())
    assert json.loads(parsed.to_json()) == _identity_payload()
    assert parsed.semantic_fingerprint.startswith("sha256:")
    assert parsed.semantic_fingerprint == AirflowRunIdentity.from_mapping(parsed.to_dict()).semantic_fingerprint


def test_run_identity_v1_rejects_activation_occurrence_as_separate_contract() -> None:
    with pytest.raises(AirflowRunIdentityError, match="unsupported fields"):
        AirflowRunIdentity.from_mapping(
            {**_identity_payload(), "activation_id": "3f60628e-ef48-48b0-84c3-a9e27a82a7f2"}
        )


def test_run_identity_matches_public_json_schema() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    contract = get_gitops_schema_contract("dpone.airflow-run-identity.v1")

    assert contract is not None
    jsonschema.validate(_identity_payload(), contract.schema)
    invalid = {**_identity_payload(), "password": "must-not-be-carried"}
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid, contract.schema)


@pytest.mark.parametrize(
    "field",
    ["password", "vault_path", "signed_url", "connection_uri", "secret_token"],
)
def test_run_identity_rejects_secret_like_fields(field: str) -> None:
    payload = _identity_payload()
    payload[field] = "must-not-be-carried"

    with pytest.raises(AirflowRunIdentityError, match="DPONE_AIRFLOW_RUN_IDENTITY_INVALID"):
        AirflowRunIdentity.from_mapping(payload)


def test_run_identity_rejects_oversized_serialized_payload() -> None:
    raw = json.dumps({**_identity_payload(), "padding": "x" * (16 * 1024)})

    with pytest.raises(AirflowRunIdentityError, match="DPONE_AIRFLOW_RUN_IDENTITY_INVALID"):
        parse_airflow_run_identity_json(raw)


@pytest.mark.parametrize(
    "bundle",
    [
        {
            "backend": "git",
            "ref": "git:7ac31f2",
            "versioned": True,
            "version": None,
            "snapshot_ref": None,
        },
        {
            "backend": "s3",
            "ref": "s3://bucket/dags?X-Amz-Signature=must-not-leak",
            "versioned": False,
            "version": None,
            "snapshot_ref": None,
        },
        {
            "backend": "s3",
            "ref": "s3://bucket/dags",
            "versioned": False,
            "version": None,
            "snapshot_ref": "snapshot-latest",
        },
    ],
)
def test_run_identity_rejects_unproven_or_sensitive_bundle_identity(bundle: dict[str, object]) -> None:
    payload = _identity_payload()
    payload["airflow_bundle"] = bundle

    with pytest.raises(AirflowRunIdentityError, match="DPONE_AIRFLOW_RUN_IDENTITY_INVALID"):
        AirflowRunIdentity.from_mapping(payload)


def test_provider_builds_identity_from_verified_index_context(tmp_path: Path) -> None:
    index_path = _write_index(tmp_path)
    index = load_airflow_deployment_index(index_path)
    verified_dag_descriptor = index.dag_specs[0]

    dag_context = build_dag_run_identity_context(
        index,
        dag_spec_id=verified_dag_descriptor.id,
        dag_spec_sha256=verified_dag_descriptor.sha256,
    )
    identity = build_workload_run_identity(
        dag_context,
        workload_id="load_orders",
        pack_sha256=index.workload_packs[0].sha256,
    )

    expected = _identity_payload()
    expected["dag_spec"] = {
        "id": verified_dag_descriptor.id,
        "sha256": verified_dag_descriptor.sha256,
    }
    expected["workload_pack"] = {"id": "load_orders", "sha256": index.workload_packs[0].sha256}
    assert identity == expected
    assert identity["dag_spec"]["sha256"] == _sha256(
        index_path.parents[3] / verified_dag_descriptor.artifact_ref.removeprefix("cache://")
    )
    assert parse_airflow_run_identity_json(serialize_run_identity(identity)).to_dict() == identity
    assert index.airflow_bundle_ref == "git:7ac31f2"


def test_provider_builds_pinned_identity_for_workload_in_custom_dag(tmp_path: Path) -> None:
    index = load_airflow_deployment_index(_write_index(tmp_path))

    identity = build_workload_run_identity(
        build_task_group_run_identity_context(index),
        workload_id="load_orders",
        pack_sha256=index.workload_packs[0].sha256,
    )

    assert identity["release_id"] == RELEASE_ID
    assert identity["deployment_id"] == DEPLOYMENT_ID
    assert identity["dag_spec"] is None
    assert identity["workload_pack"] == {
        "id": "load_orders",
        "sha256": index.workload_packs[0].sha256,
    }
    assert parse_airflow_run_identity_json(serialize_run_identity(identity)).to_dict() == identity


def test_provider_rejects_signed_bundle_url_before_task_materialization(tmp_path: Path) -> None:
    index_path = _write_index(tmp_path)
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    payload["airflow_bundle_ref"] = "s3://bucket/dags?X-Amz-Signature=must-not-leak"
    index_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AirflowDeploymentIndexError) as exc:
        load_airflow_deployment_index(index_path)

    assert exc.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert "must-not-leak" not in str(exc.value)


def _write_index(tmp_path: Path) -> Path:
    cache_root = tmp_path / ".dpone-cache"
    release_root = cache_root / "releases" / RELEASE_DIR
    deployment_root = cache_root / "deployments" / "prod" / DEPLOYMENT_DIR
    context_dir = "sha256-" + "9" * 64
    context_root = cache_root / "runtime-connection-contexts" / context_dir
    dag_path = release_root / "dags" / "orders_daily.dag-spec.json"
    pack_path = release_root / "packs" / "load_orders.airflow-pack.json"
    release_path = release_root / "release-set.json"
    deployment_path = deployment_root / "deployment.json"
    binding_path = context_root / "binding-set.json"
    registry_path = context_root / "connection-registry.json"
    credential_path = context_root / "credential-runtime.json"
    dag_path.parent.mkdir(parents=True)
    pack_path.parent.mkdir(parents=True)
    deployment_root.mkdir(parents=True)
    context_root.mkdir(parents=True)
    dag_path.write_text("{}", encoding="utf-8")
    pack_path.write_text("{}", encoding="utf-8")
    release_path.write_text("{}", encoding="utf-8")
    deployment_path.write_text("{}", encoding="utf-8")
    binding_path.write_text("{}", encoding="utf-8")
    registry_path.write_text("{}", encoding="utf-8")
    credential_path.write_text("{}", encoding="utf-8")
    index_path = deployment_root / "airflow-index.json"

    def _runtime_connection_artifact(name: str, path: Path) -> dict[str, str | int]:
        return {
            "artifact_ref": f"cache://runtime-connection-contexts/{context_dir}/{name}.json",
            "sha256": _sha256(path),
            "bytes": path.stat().st_size,
        }

    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v2",
                "trust_tier": "non_production",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "orders_daily",
                        "artifact_ref": (f"cache://releases/{RELEASE_DIR}/dags/orders_daily.dag-spec.json"),
                        "sha256": _sha256(dag_path),
                        "bytes": dag_path.stat().st_size,
                    }
                ],
                "workload_packs": [
                    {
                        "id": "load_orders",
                        "artifact_ref": (f"cache://releases/{RELEASE_DIR}/packs/load_orders.airflow-pack.json"),
                        "sha256": _sha256(pack_path),
                        "bytes": pack_path.stat().st_size,
                        "pack_fingerprint": PACK_FINGERPRINT,
                    }
                ],
                "binding_set_ref": BINDING_REF,
                "connection_registry_ref": REGISTRY_REF,
                "credential_runtime_ref": CREDENTIAL_RUNTIME_REF,
                "binding_set": _runtime_connection_artifact("binding-set", binding_path),
                "connection_registry": _runtime_connection_artifact("connection-registry", registry_path),
                "credential_runtime": _runtime_connection_artifact("credential-runtime", credential_path),
                "runtime_image_ref": IMAGE_REF,
                "runtime_image_digest": IMAGE_DIGEST,
                "airflow_bundle_ref": "git:7ac31f2",
                "release": {
                    "artifact_ref": f"cache://releases/{RELEASE_DIR}/release-set.json",
                    "sha256": _sha256(release_path),
                    "bytes": release_path.stat().st_size,
                },
                "deployment": {
                    "artifact_ref": (f"cache://deployments/prod/{DEPLOYMENT_DIR}/deployment.json"),
                    "sha256": _sha256(deployment_path),
                    "bytes": deployment_path.stat().st_size,
                },
                "runtime_artifact_delivery": {
                    "mode": "init_fetch",
                    "trust_tier": "non_production",
                    "artifact_registry_ref": "dpone-prod-artifacts",
                    "identity": {
                        "method": "kubernetes_workload_identity",
                        "service_account": "dpone-runtime",
                        "namespace": "airflow-example",
                    },
                    "registry_config_ref": {
                        "kind": "kubernetes_config_map",
                        "name": "dpone-artifact-registry",
                        "key": "registry.json",
                        "sha256": "sha256:" + "5" * 64,
                    },
                    "source": {
                        "artifact_registry_ref": "dpone-prod-artifacts",
                    },
                    "verify": {
                        "checksums": "required",
                        "attestations": "optional",
                    },
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return index_path


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
