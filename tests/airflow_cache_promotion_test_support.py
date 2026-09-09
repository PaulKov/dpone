from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import deployment_id as compute_deployment_id
from dpone.contracts.airflow_deployment import release_id as compute_release_id


@dataclass(frozen=True)
class CacheFixture:
    deployment: Path
    deployment_id: str
    release_id: str


def write_cache_fixture(cache_root: Path) -> CacheFixture:
    artifact = json.dumps(
        {"dag_id": "orders_daily"},
        sort_keys=True,
    ).encode("utf-8")
    artifact_sha256 = "sha256:" + hashlib.sha256(artifact).hexdigest()
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "path": "dags/orders_daily.dag-spec.json",
                    "sha256": artifact_sha256,
                }
            ],
            "workload_packs": [],
            "canonical_schemas": [],
        },
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = cache_root / "releases" / release_id.replace(":", "-", 1)
    artifact_path = release_dir / "dags" / "orders_daily.dag-spec.json"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(artifact)
    write_json(release_dir / "release-set.json", release)

    runtime_delivery = {"mode": "local_preview"}
    deployment = {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "",
        "deployment_type": "preview",
        "runnable": False,
        "environment": "dev",
        "release_ref": release_id,
        "binding_set_ref": None,
        "connection_registry_ref": None,
        "credential_runtime_ref": None,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": runtime_delivery,
    }
    deployment_id = compute_deployment_id(deployment)
    deployment["deployment_id"] = deployment_id
    deployment_dir = cache_root / "deployments" / "dev" / deployment_id.replace(":", "-", 1)
    deployment_dir.mkdir(parents=True)
    write_json(deployment_dir / "deployment.json", deployment)
    write_json(
        deployment_dir / "airflow-index.json",
        {
            "schema": "dpone.airflow-deployment-index.v1",
            "release_id": release_id,
            "deployment_id": deployment_id,
            "binding_set_ref": None,
            "connection_registry_ref": None,
            "credential_runtime_ref": None,
            "runtime_image_digest": None,
            "airflow_bundle_ref": None,
            "runtime_artifact_delivery": runtime_delivery,
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "artifact_ref": (
                        f"cache://releases/{release_id.replace(':', '-', 1)}/dags/orders_daily.dag-spec.json"
                    ),
                    "sha256": artifact_sha256,
                    "bytes": len(artifact),
                }
            ],
            "workload_packs": [],
        },
    )
    (deployment_dir / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    return CacheFixture(
        deployment=deployment_dir,
        deployment_id=deployment_id,
        release_id=release_id,
    )


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def assert_no_promotion_side_effects(cache_root: Path) -> None:
    assert not (cache_root / "current").exists()
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current-pointer-audit.jsonl").exists()
    assert not (cache_root / "activations").exists()
