"""Build one minimal, valid immutable Airflow artifact projection."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

import yaml
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)

from dpone.adapters.object_storage_artifact_registry import (
    ObjectStorageArtifactRegistry,
)
from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
)
from dpone.contracts.airflow_deployment import (
    deployment_id as compute_deployment_id,
)
from dpone.contracts.airflow_deployment import (
    release_id as compute_release_id,
)
from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionService
from dpone.runtime.airflow_artifact_delivery import AirflowArtifactPublisher
from dpone.runtime.airflow_artifact_delivery_models import PublishRequest
from dpone.storage.local import LocalObjectStorageClient
from dpone.storage.models import ObjectStorageUri


def publish_test_projection(
    tmp_path: Path,
) -> tuple[LocalObjectStorageClient, str, str]:
    """Publish one complete projection to a deterministic local S3 emulator."""

    cache_root = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(cache_root)
    client = LocalObjectStorageClient(tmp_path / "registry")
    registry = ObjectStorageArtifactRegistry(
        client=client,
        root=ObjectStorageUri.parse("s3://dpone-artifacts/airflow"),
    )
    AirflowArtifactPublisher(registry=registry).publish(
        PublishRequest(
            cache_root=cache_root,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="dev",
            artifact_registry_ref="local-test",
        )
    )
    return client, release_id, deployment_id


def write_exact_test_projection(cache_root: Path) -> tuple[str, str]:
    """Materialize one strict v2 projection for exact publication tests."""

    source_root = cache_root.parent
    pack_payload = {
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
    pack_bytes = json.dumps(
        {
            **pack_payload,
            "pack_fingerprint": compute_pack_fingerprint(pack_payload),
        },
        sort_keys=True,
    ).encode("utf-8")
    dag_bytes = b'{"dag_id":"orders_daily"}\n'
    release: dict[str, object] = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "path": "dags/orders_daily.dag-spec.json",
                    "sha256": _sha256(dag_bytes),
                }
            ],
            "workload_packs": [
                {
                    "id": "load_orders",
                    "path": "packs/load_orders.airflow-pack.json",
                    "sha256": _sha256(pack_bytes),
                }
            ],
            "canonical_schemas": [],
        },
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = cache_root / "releases" / _digest_dir(release_id)
    (release_dir / "dags").mkdir(parents=True)
    (release_dir / "packs").mkdir()
    (release_dir / "dags" / "orders_daily.dag-spec.json").write_bytes(dag_bytes)
    (release_dir / "packs" / "load_orders.airflow-pack.json").write_bytes(pack_bytes)
    _write_json(release_dir / "release-set.json", release)
    _write_exact_environment(source_root)
    result = AirflowDeploymentProjectionService(
        root=source_root,
        cache_root=cache_root,
    ).materialize(
        release_id=release_id,
        environment="dev",
        trust_tier="non_production",
        runtime_image_digest="sha256:" + "d" * 64,
        runtime_image_ref="registry.example/dpone-runtime@sha256:" + "d" * 64,
        artifact_registry_ref="local-test",
        registry_config_ref=_config_map_ref("registry"),
        trust_policy_ref=None,
        airflow_bundle_ref="git:" + "7" * 40,
    )
    return release_id, str(result.deployment["deployment_id"])


def _write_projection(cache_root: Path) -> tuple[str, str]:
    dag_bytes = b'{"dag_id":"orders_daily"}\n'
    dag_sha = _sha256(dag_bytes)
    release: dict[str, object] = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "path": "dags/orders_daily.dag-spec.json",
                    "sha256": dag_sha,
                }
            ],
            "workload_packs": [],
            "canonical_schemas": [],
        },
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = cache_root / "releases" / _digest_dir(release_id)
    (release_dir / "dags").mkdir(parents=True)
    (release_dir / "dags" / "orders_daily.dag-spec.json").write_bytes(dag_bytes)
    _write_json(release_dir / "release-set.json", release)

    delivery = {
        "mode": "init_fetch",
        "artifact_registry_ref": "local-test",
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
        },
        "source": {"artifact_registry_ref": "local-test"},
        "verify": {"checksums": "required", "attestations": "optional"},
    }
    binding_set = {
        "schema": "dpone.binding-set.v1",
        "environment": "dev",
        "bindings": {},
    }
    binding_ref = canonical_fingerprint(binding_set)
    connection_ref = "sha256:" + "c" * 64
    credential_ref = "sha256:" + "e" * 64
    deployment: dict[str, object] = {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "",
        "deployment_type": "environment",
        "runnable": True,
        "environment": "dev",
        "release_ref": release_id,
        "binding_set_ref": binding_ref,
        "connection_registry_ref": connection_ref,
        "credential_runtime_ref": credential_ref,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": delivery,
    }
    deployment_id = compute_deployment_id(deployment)
    deployment["deployment_id"] = deployment_id
    index = {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": release_id,
        "deployment_id": deployment_id,
        "dag_specs": [
            {
                "id": "orders_daily",
                "artifact_ref": (f"cache://releases/{_digest_dir(release_id)}/dags/orders_daily.dag-spec.json"),
                "sha256": dag_sha,
                "bytes": len(dag_bytes),
            }
        ],
        "workload_packs": [],
        "binding_set_ref": binding_ref,
        "connection_registry_ref": connection_ref,
        "credential_runtime_ref": credential_ref,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": delivery,
    }
    deployment_dir = cache_root / "deployments" / "dev" / _digest_dir(deployment_id)
    deployment_dir.mkdir(parents=True)
    _write_json(deployment_dir / "deployment.json", deployment)
    _write_json(deployment_dir / "airflow-index.json", index)
    _write_json(deployment_dir / "binding-set.json", binding_set)
    (deployment_dir / "connection-registry.ref").write_text(
        connection_ref + "\n",
        encoding="utf-8",
    )
    (deployment_dir / "credential-runtime.ref").write_text(
        credential_ref + "\n",
        encoding="utf-8",
    )
    (deployment_dir / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    return release_id, deployment_id


def _write_exact_environment(root: Path) -> None:
    environment_dir = root / "environments" / "dev"
    environment_dir.mkdir(parents=True)
    registry_dir = root / "platform" / "connection-registries"
    registry_dir.mkdir(parents=True)
    (environment_dir / "binding-set.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "dev",
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
    (environment_dir / "credential-runtime.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.credential-runtime.v1",
                "environment": "dev",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (registry_dir / "dev.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "dev",
                "connections": {
                    "mssql_prod": {
                        "type": "mssql",
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "path": "dpone/dev/credentials/mssql",
                            "fields": {
                                "username": "username",
                                "password": "password",
                            },
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


def _config_map_ref(label: str) -> dict[str, str]:
    return {
        "kind": "kubernetes_config_map",
        "name": f"dpone-artifact-{label}-4f3a",
        "key": f"{label}.json",
        "sha256": "sha256:" + ("1" if label == "registry" else "2") * 64,
    }


def _digest_dir(digest: str) -> str:
    return digest.replace(":", "-", 1)


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(
        json.dumps(payload, sort_keys=True) + "\n",
        encoding="utf-8",
    )


__all__ = ["publish_test_projection", "write_exact_test_projection"]
