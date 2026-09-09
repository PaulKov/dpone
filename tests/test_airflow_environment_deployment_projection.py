from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TypedDict

import pytest
import yaml
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)

from dpone.contracts.airflow_deployment import release_id as compute_release_id

_STRICT_PACK_PAYLOAD = {
    "id": "load_orders",
    "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
    "workload": {"workload_id": "load_orders"},
    "airflow": {"execution": {}},
    "connection_projection": {},
    "provider_execution": {
        "schema": "dpone.airflow-provider-execution.v1",
        "kpo_kwargs": {
            "task_id": "load_orders__dpone_runtime",
            "name": "dpone-load-orders",
            "labels": {"dpone.dev/workload-id": "load_orders"},
            "env_vars": {},
        },
        "pod_spec": {"spec": {"containers": [{"name": "base"}]}},
    },
    "xcom": {
        "sidecar_image": "registry.example/airflow/xcom@sha256:" + "a" * 64,
    },
}
_STRICT_PACK_FINGERPRINT = compute_pack_fingerprint(_STRICT_PACK_PAYLOAD)
_STRICT_PACK_BYTES = json.dumps(
    {
        **_STRICT_PACK_PAYLOAD,
        "pack_fingerprint": _STRICT_PACK_FINGERPRINT,
    },
    sort_keys=True,
).encode("utf-8")


def test_environment_deployment_projection_materializes_digest_refs_and_index(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    dag_bytes = b'{"dag_id":"orders_daily"}\n'
    pack_fingerprint = _STRICT_PACK_FINGERPRINT
    pack_bytes = _STRICT_PACK_BYTES
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "artifact_ref": "dags/orders_daily.dag-spec.json",
                    "sha256": "sha256:" + hashlib.sha256(dag_bytes).hexdigest(),
                }
            ],
            "workload_packs": [
                {
                    "id": "load_orders",
                    "path": "packs/load_orders.airflow-pack.json",
                    "sha256": "sha256:" + hashlib.sha256(pack_bytes).hexdigest(),
                }
            ],
            "canonical_schemas": [],
        },
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = tmp_path / ".dpone-cache" / "releases" / release_id.replace(":", "-")
    (release_dir / "dags").mkdir(parents=True)
    (release_dir / "packs").mkdir()
    (release_dir / "dags" / "orders_daily.dag-spec.json").write_bytes(dag_bytes)
    (release_dir / "packs" / "load_orders.airflow-pack.json").write_bytes(pack_bytes)
    (release_dir / "release-set.json").write_text(
        json.dumps(release),
        encoding="utf-8",
    )
    assert compute_release_id(json.loads((release_dir / "release-set.json").read_text(encoding="utf-8"))) == release_id
    environment_dir = tmp_path / "environments" / "prod"
    environment_dir.mkdir(parents=True)
    (environment_dir / "binding-set.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {"mssql_prod": {"connection_ref": "mssql_prod"}},
                "runtime": {
                    "kubernetes_namespace": "airflow-example",
                    "service_account": "dpone-runtime",
                    "pool": "dpone_prod",
                },
                "connection_registry_ref": "platform/connection-registries/prod.yaml",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (environment_dir / "credential-runtime.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.credential-runtime.v1",
                "environment": "prod",
                "vault": {
                    "address": "https://vault.internal",
                    "namespace": "data-platform",
                    "auth": {"method": "kubernetes", "role": "dpone-runtime-prod"},
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    registry_dir = tmp_path / "platform" / "connection-registries"
    registry_dir.mkdir(parents=True)
    (registry_dir / "prod.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "mssql_prod": {
                        "type": "mssql",
                        "connection": {"host": "mssql.internal", "port": 1433, "database": "dwh"},
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "kv_version": 2,
                            "path": "dpone/prod/credentials/mssql",
                            "fields": {"username": "username", "password": "password"},
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    service = AirflowDeploymentProjectionService(root=tmp_path)
    result = service.materialize(
        release_id=release_id,
        environment="prod",
        runtime_image_digest="sha256:" + "d" * 64,
        artifact_registry_ref="dpone-prod-artifacts",
        **_init_fetch_projection_args("sha256:" + "d" * 64),
    )

    assert result.deployment["schema"] == "dpone.deployment-set.v2"
    assert result.airflow_index["schema"] == "dpone.airflow-deployment-index.v2"
    assert result.deployment["trust_tier"] == "production"
    assert result.airflow_index["trust_tier"] == "production"
    assert result.deployment["runnable"] is True
    assert result.deployment["release_ref"] == release_id
    assert result.deployment["binding_set_ref"].startswith("sha256:")
    assert result.deployment["connection_registry_ref"].startswith("sha256:")
    assert result.deployment["credential_runtime_ref"].startswith("sha256:")
    assert result.airflow_index["runtime_artifact_delivery"]["mode"] == "init_fetch"
    assert result.airflow_index["runtime_artifact_delivery"]["trust_tier"] == "production"
    assert result.airflow_index["runtime_artifact_delivery"]["artifact_registry_ref"] == "dpone-prod-artifacts"
    assert result.airflow_index["runtime_image_ref"].endswith("@sha256:" + "d" * 64)
    assert result.airflow_index["runtime_artifact_delivery"]["identity"]["namespace"] == "airflow-example"
    assert result.airflow_index["runtime_artifact_delivery"]["registry_config_ref"]["name"] == (
        "dpone-artifact-registry-4f3a"
    )
    assert result.airflow_index["runtime_artifact_delivery"]["trust_policy_ref"]["key"] == "policy.json"
    assert result.airflow_index["airflow_bundle_ref"] == "git:" + "7" * 40
    assert result.airflow_index["workload_packs"][0]["artifact_ref"].startswith(
        f"cache://releases/{release_id.replace(':', '-')}/packs/"
    )
    assert "connections" not in result.airflow_index
    assert result.airflow_index["workload_packs"][0]["pack_fingerprint"] == pack_fingerprint
    assert result.deployment["workloads"] == [
        {
            "id": "load_orders",
            "sha256": "sha256:" + hashlib.sha256(pack_bytes).hexdigest(),
            "pack_fingerprint": pack_fingerprint,
        }
    ]
    for descriptor_name in ("release", "deployment"):
        descriptor = result.airflow_index[descriptor_name]
        assert descriptor["artifact_ref"].startswith("cache://")
        assert descriptor["bytes"] > 0
        assert descriptor["sha256"].startswith("sha256:")
    runtime_connection_files = {
        "binding_set": "binding-set.json",
        "connection_registry": "connection-registry.ref",
        "credential_runtime": "credential-runtime.ref",
    }
    for descriptor_name, filename in runtime_connection_files.items():
        descriptor = result.airflow_index[descriptor_name]
        snapshot = (result.deployment_dir / filename).read_bytes()
        assert result.deployment[descriptor_name] == descriptor
        assert descriptor["artifact_ref"].startswith("cache://runtime-connection-contexts/sha256-")
        assert descriptor["artifact_ref"].endswith(f"/{descriptor_name.replace('_', '-')}.json")
        assert descriptor["bytes"] == len(snapshot)
        assert descriptor["sha256"] == "sha256:" + hashlib.sha256(snapshot).hexdigest()
        assert json.loads(snapshot)["environment"] == "prod"
    deployment_bytes = (result.deployment_dir / "deployment.json").read_bytes()
    assert result.airflow_index["deployment"]["sha256"] == "sha256:" + hashlib.sha256(deployment_bytes).hexdigest()
    deployment_schema = json.loads(Path("docs/schemas/gitops/deployment-set-v2.schema.json").read_text())
    index_schema = json.loads(Path("docs/schemas/gitops/airflow-deployment-index-v2.schema.json").read_text())
    jsonschema.validate(result.deployment, deployment_schema)
    jsonschema.validate(result.airflow_index, index_schema)
    assert (
        json.loads((result.deployment_dir / "connection-registry.ref").read_text(encoding="utf-8"))["schema"]
        == "dpone.connection-registry.v1"
    )
    assert (result.deployment_dir / "_SUCCESS").read_text(encoding="utf-8") == "ok\n"
    from dpone.runtime.airflow_artifact_delivery_models import PublishRequest
    from dpone.runtime.airflow_artifact_inventory import build_publish_inventory
    from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection

    attestation_bundle = tmp_path / "release-set.sigstore.jsonl"
    attestation_bundle.write_text('{"attestation":"offline"}\n', encoding="utf-8")
    inventory = build_publish_inventory(
        PublishRequest(
            cache_root=tmp_path / ".dpone-cache",
            release_id=release_id,
            deployment_id=str(result.deployment["deployment_id"]),
            environment="prod",
            artifact_registry_ref="dpone-prod-artifacts",
            attestation_bundle_path=attestation_bundle,
        ),
        ValidatedDeploymentProjection(
            deployment=result.deployment,
            airflow_index=result.airflow_index,
        ),
    )
    published = {f"cache://{item.key.as_posix()}": item for item in inventory.objects}
    assert any(
        item.key.as_posix().startswith("attestations/releases/") and item.key.name == "github.sigstore.jsonl"
        for item in inventory.release
    )
    release_marker_index = next(
        index
        for index, item in enumerate(inventory.release)
        if item.completion_marker and item.key.parts[0] == "releases"
    )
    attestation_index = next(
        index
        for index, item in enumerate(inventory.release)
        if item.key.as_posix().startswith("attestations/releases/")
    )
    assert attestation_index < release_marker_index
    for descriptor_name in runtime_connection_files:
        descriptor = result.deployment[descriptor_name]
        item = published[descriptor["artifact_ref"]]
        assert item.sha256 == descriptor["sha256"]
        assert item.size_bytes == descriptor["bytes"]

    repeat = service.materialize(
        release_id=release_id,
        environment="prod",
        runtime_image_digest="sha256:" + "d" * 64,
        artifact_registry_ref="dpone-prod-artifacts",
        **_init_fetch_projection_args("sha256:" + "d" * 64),
    )

    assert repeat.deployment_dir == result.deployment_dir

    (result.deployment_dir / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v2",
                "release_id": release_id,
                "deployment_id": result.deployment["deployment_id"],
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "init_fetch"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        service.materialize(
            release_id=release_id,
            environment="prod",
            runtime_image_digest="sha256:" + "d" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            **_init_fetch_projection_args("sha256:" + "d" * 64),
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_ALREADY_EXISTS"
    assert exc.value.path == result.deployment_dir.as_posix()
    assert json.loads((result.deployment_dir / "airflow-index.json").read_text(encoding="utf-8"))["dag_specs"] == []


def test_local_safe_sample_v1_projection_uses_explicit_compatibility_seam(
    tmp_path: Path,
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionService,
    )

    runtime_image_digest = "sha256:" + "d" * 64
    release_id = _write_release(tmp_path)
    _write_environment_files(tmp_path, environment="dev")

    result = AirflowDeploymentProjectionService(root=tmp_path).materialize_local_safe_sample_v1(
        release_id=release_id,
        environment="dev",
        runtime_image_digest=runtime_image_digest,
        artifact_registry_ref="local-safe-sample-artifacts",
        airflow_bundle_ref="local:dpone-safe-sample",
    )

    delivery = result.airflow_index["runtime_artifact_delivery"]
    assert result.deployment["schema"] == "dpone.deployment-set.v1"
    assert result.airflow_index["schema"] == "dpone.airflow-deployment-index.v1"
    assert result.deployment["runnable"] is True
    assert result.airflow_index["runtime_image_digest"] == runtime_image_digest
    assert delivery == {
        "mode": "init_fetch",
        "artifact_registry_ref": "local-safe-sample-artifacts",
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
        },
        "source": {"artifact_registry_ref": "local-safe-sample-artifacts"},
        "verify": {"checksums": "required", "attestations": "optional"},
    }
    assert "trust_tier" not in result.deployment
    assert "trust_tier" not in result.airflow_index
    assert "runtime_image_ref" not in result.airflow_index
    assert "registry_config_ref" not in delivery
    assert "trust_policy_ref" not in delivery
    assert (result.deployment_dir / "connection-registry.ref").read_text(encoding="utf-8").startswith("sha256:")
    assert (result.deployment_dir / "credential-runtime.ref").read_text(encoding="utf-8").startswith("sha256:")

    for payload, schema_path in (
        (result.deployment, "docs/schemas/gitops/deployment-set.schema.json"),
        (
            result.airflow_index,
            "docs/schemas/gitops/airflow-deployment-index.schema.json",
        ),
    ):
        schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
        jsonschema.validate(payload, schema)


def test_strict_dev_projection_fingerprints_evidence_delivery(
    tmp_path: Path,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionService,
    )

    runtime_image_digest = "sha256:" + "d" * 64
    release_id = _write_release(tmp_path)
    _write_environment_files(tmp_path, environment="dev")

    result = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=release_id,
        environment="dev",
        trust_tier="non_production",
        runtime_image_ref=f"registry.example/dpone-runtime@{runtime_image_digest}",
        runtime_image_digest=runtime_image_digest,
        artifact_registry_ref="dpone-dev-artifacts",
        registry_config_ref=_config_map_ref("registry"),
        trust_policy_ref=None,
        airflow_bundle_ref="git:" + "7" * 40,
        dev_evidence_pvc_claim="dpone-dbt-dev-evidence",
        dev_evidence_worker_queue="dpone_evidence_export",
    )

    expected = {
        "mode": "shared_pvc",
        "claim_name": "dpone-dbt-dev-evidence",
        "mount_path": "/var/lib/dpone/dev-evidence",
        "worker_queue": "dpone_evidence_export",
    }
    assert result.deployment["dev_evidence_delivery"] == expected
    assert result.airflow_index["dev_evidence_delivery"] == expected
    assert result.deployment["deployment_id"] == result.airflow_index["deployment_id"]


def test_local_safe_sample_v1_accepts_legacy_checksum_and_pack_shape(
    tmp_path: Path,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionService,
    )

    legacy_pack = b'{"id":"load_orders","runtime_command":["python","job.py"]}\n'
    release_id = _write_release(
        tmp_path,
        pack_bytes=legacy_pack,
        mixed_case_artifact_checksum=True,
    )
    _write_environment_files(tmp_path, environment="dev")

    result = AirflowDeploymentProjectionService(root=tmp_path).materialize_local_safe_sample_v1(
        release_id=release_id,
        environment="dev",
        runtime_image_digest="sha256:" + "d" * 64,
        artifact_registry_ref="local-safe-sample-artifacts",
    )

    indexed_pack = result.airflow_index["workload_packs"][0]
    assert indexed_pack["sha256"].startswith("sha256:")
    assert any(character in "ABCDEF" for character in indexed_pack["sha256"])
    assert "pack_fingerprint" not in indexed_pack


def test_strict_v2_rejects_legacy_mixed_case_artifact_checksum(
    tmp_path: Path,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    release_id = _write_release(
        tmp_path,
        pack_bytes=b'{"id":"load_orders"}\n',
        mixed_case_artifact_checksum=True,
    )
    _write_environment_files(tmp_path, environment="dev")

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id=release_id,
            environment="dev",
            runtime_image_digest="sha256:" + "d" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            **_init_fetch_projection_args("sha256:" + "d" * 64),
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_DIGEST_INVALID"


@pytest.mark.parametrize(
    ("environment", "trust_tier", "expected_attestations", "include_trust_policy"),
    [
        ("prod", "non_production", "optional", False),
        ("sandbox", "production", "required_for_prod", True),
    ],
)
def test_environment_name_does_not_infer_strict_v2_trust(
    tmp_path: Path,
    environment: str,
    trust_tier: str,
    expected_attestations: str,
    include_trust_policy: bool,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionService,
    )

    runtime_image_digest = "sha256:" + "d" * 64
    release_id = _write_release(tmp_path)
    _write_environment_files(tmp_path, environment=environment)
    projection_args = _init_fetch_projection_args(runtime_image_digest)
    projection_args["trust_tier"] = trust_tier
    if not include_trust_policy:
        projection_args["trust_policy_ref"] = None

    result = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=release_id,
        environment=environment,
        runtime_image_digest=runtime_image_digest,
        artifact_registry_ref="dpone-prod-artifacts",
        **projection_args,
    )

    assert result.deployment["trust_tier"] == trust_tier
    assert result.airflow_index["trust_tier"] == trust_tier
    delivery = result.airflow_index["runtime_artifact_delivery"]
    assert delivery["trust_tier"] == trust_tier
    assert delivery["verify"]["attestations"] == expected_attestations
    assert ("trust_policy_ref" in delivery) is include_trust_policy


def test_production_trust_tier_requires_digest_pinned_trust_policy(tmp_path: Path) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    runtime_image_digest = "sha256:" + "d" * 64
    release_id = _write_release(tmp_path)
    _write_environment_files(tmp_path, environment="sandbox")

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id=release_id,
            environment="sandbox",
            trust_tier="production",
            runtime_image_digest=runtime_image_digest,
            runtime_image_ref=f"registry.example/dpone-runtime@{runtime_image_digest}",
            artifact_registry_ref="dpone-prod-artifacts",
            registry_config_ref=_config_map_ref("registry"),
            trust_policy_ref=None,
            airflow_bundle_ref="git:" + "7" * 40,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_TRUST_POLICY_REQUIRED"


@pytest.mark.parametrize(
    "artifact_registry_ref",
    ["current", "cache://current/prod", "cache://registries/current/prod"],
)
def test_environment_deployment_projection_rejects_current_artifact_registry(
    tmp_path: Path,
    artifact_registry_ref: str,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id="sha256:" + "a" * 64,
            environment="prod",
            trust_tier="production",
            runtime_image_digest="sha256:" + "d" * 64,
            artifact_registry_ref=artifact_registry_ref,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_ARTIFACT_REGISTRY_UNPINNED"


def test_v2_deployment_schemas_require_complete_init_fetch_delivery() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.gitops.schema_validation import GitOpsSchemaValidator

    deployment_schema = json.loads(
        Path("docs/schemas/gitops/deployment-set-v2.schema.json").read_text(encoding="utf-8")
    )
    index_schema = json.loads(
        Path("docs/schemas/gitops/airflow-deployment-index-v2.schema.json").read_text(encoding="utf-8")
    )
    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    runtime_image_digest = "sha256:" + "c" * 64
    runtime_image_ref = f"registry.example/dpone-runtime@{runtime_image_digest}"
    registry_config_ref = _config_map_ref("registry")
    trust_policy_ref = _config_map_ref("policy")
    incomplete_delivery = {"mode": "init_fetch"}

    invalid_deployment = {
        "schema": "dpone.deployment-set.v2",
        "deployment_id": deployment_id,
        "environment": "prod",
        "trust_tier": "production",
        "release_ref": release_id,
        "runtime_artifact_delivery": incomplete_delivery,
    }
    invalid_index = {
        "schema": "dpone.airflow-deployment-index.v2",
        "release_id": release_id,
        "deployment_id": deployment_id,
        "trust_tier": "production",
        "dag_specs": [],
        "workload_packs": [],
        "runtime_artifact_delivery": incomplete_delivery,
    }

    for payload, schema in ((invalid_deployment, deployment_schema), (invalid_index, index_schema)):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(payload, schema)

    valid_delivery = {
        "mode": "init_fetch",
        "trust_tier": "production",
        "artifact_registry_ref": "dpone-prod-artifacts",
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
            "namespace": "airflow-example",
        },
        "registry_config_ref": registry_config_ref,
        "source": {"artifact_registry_ref": "dpone-prod-artifacts"},
        "trust_policy_ref": trust_policy_ref,
        "verify": {"checksums": "required", "attestations": "required_for_prod"},
    }
    workload = {
        "id": "load_orders",
        "artifact_ref": f"cache://releases/{release_id.replace(':', '-')}/packs/load_orders.json",
        "sha256": "sha256:" + "d" * 64,
        "bytes": 1,
        "pack_fingerprint": "sha256:" + "e" * 64,
    }
    exact_artifact = {
        "artifact_ref": f"cache://releases/{release_id.replace(':', '-')}/release-set.json",
        "sha256": "sha256:" + "f" * 64,
        "bytes": 1,
    }
    connection_context_root = "cache://runtime-connection-contexts/sha256-" + "9" * 64
    runtime_connection_artifacts = {
        "binding_set": {
            "artifact_ref": f"{connection_context_root}/binding-set.json",
            "sha256": "sha256:" + "4" * 64,
            "bytes": 1,
        },
        "connection_registry": {
            "artifact_ref": f"{connection_context_root}/connection-registry.json",
            "sha256": "sha256:" + "5" * 64,
            "bytes": 1,
        },
        "credential_runtime": {
            "artifact_ref": f"{connection_context_root}/credential-runtime.json",
            "sha256": "sha256:" + "6" * 64,
            "bytes": 1,
        },
    }
    valid_deployment = {
        **invalid_deployment,
        "deployment_type": "environment",
        "runnable": True,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "runtime_image_ref": runtime_image_ref,
        "runtime_image_digest": runtime_image_digest,
        "airflow_bundle_ref": "git:" + "7" * 40,
        "runtime_artifact_delivery": valid_delivery,
        **runtime_connection_artifacts,
        "workloads": [
            {
                "id": workload["id"],
                "sha256": workload["sha256"],
                "pack_fingerprint": workload["pack_fingerprint"],
            }
        ],
    }
    valid_index = {
        **invalid_index,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "runtime_image_ref": runtime_image_ref,
        "runtime_image_digest": runtime_image_digest,
        "airflow_bundle_ref": "git:" + "7" * 40,
        "runtime_artifact_delivery": valid_delivery,
        **runtime_connection_artifacts,
        "workload_packs": [workload],
        "release": exact_artifact,
        "deployment": {
            **exact_artifact,
            "artifact_ref": f"cache://deployments/prod/{deployment_id.replace(':', '-')}/deployment.json",
        },
    }

    jsonschema.validate(valid_deployment, deployment_schema)
    jsonschema.validate(valid_index, index_schema)
    python_validator = GitOpsSchemaValidator()

    for valid_payload, valid_schema in ((valid_deployment, deployment_schema), (valid_index, index_schema)):
        expected_kind = str(valid_payload["schema"])
        assert python_validator.validate(valid_payload, expected_kind=expected_kind) == ()

        without_policy = json.loads(json.dumps(valid_payload))
        without_policy["runtime_artifact_delivery"].pop("trust_policy_ref")
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(without_policy, valid_schema)
        assert any(
            issue.code == "schema_required_field_missing" and issue.path == "runtime_artifact_delivery.trust_policy_ref"
            for issue in python_validator.validate(
                without_policy,
                expected_kind=expected_kind,
            )
        )

        mismatched_attestation = json.loads(json.dumps(valid_payload))
        mismatched_attestation["runtime_artifact_delivery"]["verify"]["attestations"] = "optional"
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(mismatched_attestation, valid_schema)
        assert any(
            issue.code == "schema_const_mismatch" and issue.path == "runtime_artifact_delivery.verify.attestations"
            for issue in python_validator.validate(
                mismatched_attestation,
                expected_kind=expected_kind,
            )
        )

        mismatched_tier = json.loads(json.dumps(valid_payload))
        mismatched_tier["runtime_artifact_delivery"]["trust_tier"] = "non_production"
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(mismatched_tier, valid_schema)

        unbounded_registry_port = json.loads(json.dumps(valid_payload))
        unbounded_registry_port["runtime_image_ref"] = f"registry.example:70000/dpone-runtime@{runtime_image_digest}"
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(unbounded_registry_port, valid_schema)
        assert any(
            issue.code == "schema_pattern_mismatch" and issue.path == "runtime_image_ref"
            for issue in python_validator.validate(
                unbounded_registry_port,
                expected_kind=expected_kind,
            )
        )

        secret_bearing = json.loads(json.dumps(valid_payload))
        secret_bearing["runtime_artifact_delivery"]["token"] = "secret"
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(secret_bearing, valid_schema)


@pytest.mark.parametrize("artifact_registry_ref", ["current", "cache://registries/current/prod"])
def test_deployment_schemas_reject_current_init_fetch_artifact_registry_ref(
    artifact_registry_ref: str,
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    deployment_schema = json.loads(
        Path("docs/schemas/gitops/deployment-set-v2.schema.json").read_text(encoding="utf-8")
    )
    index_schema = json.loads(
        Path("docs/schemas/gitops/airflow-deployment-index-v2.schema.json").read_text(encoding="utf-8")
    )
    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    delivery = {
        "mode": "init_fetch",
        "trust_tier": "production",
        "artifact_registry_ref": artifact_registry_ref,
        "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
        "source": {"artifact_registry_ref": artifact_registry_ref},
        "verify": {"checksums": "required", "attestations": "required_for_prod"},
    }
    invalid_deployment = {
        "schema": "dpone.deployment-set.v2",
        "deployment_id": deployment_id,
        "environment": "prod",
        "trust_tier": "production",
        "release_ref": release_id,
        "runtime_artifact_delivery": delivery,
    }
    invalid_index = {
        "schema": "dpone.airflow-deployment-index.v2",
        "release_id": release_id,
        "deployment_id": deployment_id,
        "trust_tier": "production",
        "dag_specs": [],
        "workload_packs": [],
        "runtime_artifact_delivery": delivery,
    }

    for payload, schema in ((invalid_deployment, deployment_schema), (invalid_index, index_schema)):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(payload, schema)


def test_environment_deployment_projection_rejects_non_hex_release_id(tmp_path: Path) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id="sha256:" + "g" * 64,
            environment="prod",
            trust_tier="production",
            runtime_image_digest="sha256:" + "d" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_DIGEST_INVALID"


def test_environment_deployment_projection_rejects_noncanonical_release_id(
    tmp_path: Path,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id="sha256:" + "A" * 64,
            environment="prod",
            trust_tier="production",
            runtime_image_digest="sha256:" + "d" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_DIGEST_INVALID"


@pytest.mark.parametrize(
    "environment",
    [
        "",
        ".",
        "..",
        "../prod",
        "/prod",
        "prod/blue",
        r"prod\blue",
        "prod blue",
        "a" * 64,
    ],
)
def test_environment_deployment_projection_rejects_invalid_logical_segment_before_io(
    tmp_path: Path,
    environment: str,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id="sha256:" + "a" * 64,
            environment=environment,
            trust_tier="production",
            runtime_image_digest="sha256:" + "d" * 64,
            runtime_image_ref="registry.example/dpone-runtime@sha256:" + "d" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            registry_config_ref=_config_map_ref("registry"),
            trust_policy_ref=_config_map_ref("policy"),
            airflow_bundle_ref="git:" + "7" * 40,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_ENVIRONMENT_INVALID"
    assert exc.value.path is None
    assert not (tmp_path / ".dpone-cache" / "deployments").exists()


def test_environment_deployment_projection_rejects_non_hex_runtime_image_digest(tmp_path: Path) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    release_id = _write_release(tmp_path)
    _write_environment_files(tmp_path, environment="prod")

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id=release_id,
            environment="prod",
            trust_tier="production",
            runtime_image_digest="sha256:" + "z" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_DIGEST_INVALID"


def test_environment_deployment_projection_rejects_mismatched_runtime_image_reference(
    tmp_path: Path,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    release_id = _write_release(tmp_path)
    _write_environment_files(tmp_path, environment="prod")

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id=release_id,
            environment="prod",
            trust_tier="production",
            runtime_image_digest="sha256:" + "d" * 64,
            runtime_image_ref="registry.example/dpone-runtime@sha256:" + "e" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            registry_config_ref=_config_map_ref("registry"),
            trust_policy_ref=_config_map_ref("policy"),
            airflow_bundle_ref="git:" + "7" * 40,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_RUNTIME_IMAGE_INVALID"


def test_environment_deployment_projection_rejects_non_hex_release_artifact_digest(tmp_path: Path) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    release_id = _write_release(tmp_path, artifact_sha256="sha256:" + "g" * 64)
    _write_environment_files(tmp_path, environment="prod")

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id=release_id,
            environment="prod",
            trust_tier="production",
            runtime_image_digest="sha256:" + "d" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            airflow_bundle_ref="git:" + "7" * 40,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_DIGEST_INVALID"


@pytest.mark.parametrize(
    ("target", "expected_code", "expected_path"),
    [
        ("binding-set", "DPONE_BINDING_SET_ENVIRONMENT_MISMATCH", "environments/prod/binding-set.yaml"),
        (
            "connection-registry",
            "DPONE_CONNECTION_REGISTRY_ENVIRONMENT_MISMATCH",
            "platform/connection-registries/prod.yaml",
        ),
        (
            "credential-runtime",
            "DPONE_CREDENTIAL_RUNTIME_ENVIRONMENT_MISMATCH",
            "environments/prod/credential-runtime.yaml",
        ),
    ],
)
def test_environment_deployment_projection_rejects_environment_owned_file_mismatch(
    tmp_path: Path,
    target: str,
    expected_code: str,
    expected_path: str,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    release_id = _write_release(tmp_path)
    paths = _write_environment_files(tmp_path, environment="prod")
    payload = yaml.safe_load(paths[target].read_text(encoding="utf-8"))
    payload["environment"] = "dev"
    paths[target].write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id=release_id,
            environment="prod",
            trust_tier="production",
            runtime_image_digest="sha256:" + "d" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            airflow_bundle_ref="git:" + "7" * 40,
        )

    assert exc.value.code == expected_code
    assert exc.value.path == (tmp_path / expected_path).as_posix()
    assert not (tmp_path / ".dpone-cache" / "deployments" / "prod").exists()


def test_strict_v2_rejects_empty_artifact_without_tightening_local_v1(
    tmp_path: Path,
) -> None:
    from dpone.readiness.airflow_deployment_projection import (
        AirflowDeploymentProjectionError,
        AirflowDeploymentProjectionService,
    )

    release_id = _write_release(tmp_path, dag_bytes=b"")
    _write_environment_files(tmp_path, environment="prod")
    release_dir = tmp_path / ".dpone-cache" / "releases" / release_id.replace(":", "-")
    dag_path = release_dir / "dags" / "empty.dag-spec.json"

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id=release_id,
            environment="prod",
            runtime_image_digest="sha256:" + "d" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            **_init_fetch_projection_args("sha256:" + "d" * 64),
        )

    assert exc.value.code == "DPONE_RELEASE_ARTIFACT_EMPTY"
    assert exc.value.path == dag_path.as_posix()

    legacy = AirflowDeploymentProjectionService(root=tmp_path).materialize_local_safe_sample_v1(
        release_id=release_id,
        environment="prod",
        runtime_image_digest="sha256:" + "d" * 64,
        artifact_registry_ref="local-safe-sample-artifacts",
    )

    assert legacy.airflow_index["schema"] == "dpone.airflow-deployment-index.v1"
    assert legacy.airflow_index["dag_specs"][0]["bytes"] == 0


def _write_release(
    root: Path,
    *,
    artifact_sha256: str | None = None,
    dag_bytes: bytes | None = None,
    pack_bytes: bytes | None = None,
    mixed_case_artifact_checksum: bool = False,
) -> str:
    exact_pack_bytes = pack_bytes or _STRICT_PACK_BYTES
    pack_sha256 = artifact_sha256 or "sha256:" + hashlib.sha256(exact_pack_bytes).hexdigest()
    if mixed_case_artifact_checksum:
        pack_sha256 = "sha256:" + pack_sha256.removeprefix("sha256:").upper()
    dag_specs = (
        [
            {
                "id": "empty",
                "path": "dags/empty.dag-spec.json",
                "sha256": "sha256:" + hashlib.sha256(dag_bytes).hexdigest(),
            }
        ]
        if dag_bytes is not None
        else []
    )
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": dag_specs,
            "workload_packs": [
                {
                    "id": "load_orders",
                    "path": "packs/load_orders.airflow-pack.json",
                    "sha256": pack_sha256,
                }
            ],
            "canonical_schemas": [],
        },
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = root / ".dpone-cache" / "releases" / release_id.replace(":", "-")
    (release_dir / "packs").mkdir(parents=True)
    (release_dir / "packs" / "load_orders.airflow-pack.json").write_bytes(exact_pack_bytes)
    if dag_bytes is not None:
        (release_dir / "dags").mkdir()
        (release_dir / "dags" / "empty.dag-spec.json").write_bytes(dag_bytes)
    (release_dir / "release-set.json").write_text(json.dumps(release), encoding="utf-8")
    return release_id


def _write_environment_files(root: Path, *, environment: str) -> dict[str, Path]:
    environment_dir = root / "environments" / environment
    environment_dir.mkdir(parents=True)
    binding_set_path = environment_dir / "binding-set.yaml"
    credential_runtime_path = environment_dir / "credential-runtime.yaml"
    registry_dir = root / "platform" / "connection-registries"
    registry_dir.mkdir(parents=True)
    connection_registry_path = registry_dir / f"{environment}.yaml"
    binding_set_path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.binding-set.v1",
                "environment": environment,
                "bindings": {"mssql_prod": {"connection_ref": "mssql_prod"}},
                "runtime": {
                    "kubernetes_namespace": "airflow-example",
                    "service_account": "dpone-runtime",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    connection_registry_path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": environment,
                "connections": {
                    "mssql_prod": {
                        "type": "mssql",
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "path": "dpone/prod/credentials/mssql",
                            "fields": {"username": "username", "password": "password"},
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    credential_runtime_path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.credential-runtime.v1",
                "environment": environment,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return {
        "binding-set": binding_set_path,
        "connection-registry": connection_registry_path,
        "credential-runtime": credential_runtime_path,
    }


def _config_map_ref(label: str) -> dict[str, str]:
    return {
        "kind": "kubernetes_config_map",
        "name": f"dpone-artifact-{label}-4f3a",
        "key": f"{label}.json",
        "sha256": "sha256:" + ("1" if label == "registry" else "2") * 64,
    }


class _InitFetchProjectionArgs(TypedDict):
    trust_tier: str
    runtime_image_ref: str
    registry_config_ref: dict[str, str]
    trust_policy_ref: dict[str, str] | None
    airflow_bundle_ref: str


def _init_fetch_projection_args(runtime_image_digest: str) -> _InitFetchProjectionArgs:
    return {
        "trust_tier": "production",
        "runtime_image_ref": f"registry.example/dpone-runtime@{runtime_image_digest}",
        "registry_config_ref": _config_map_ref("registry"),
        "trust_policy_ref": _config_map_ref("policy"),
        "airflow_bundle_ref": "git:" + "7" * 40,
    }
