"""Lock pack/core public constants and algorithms that must not drift silently."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from dpone_airflow_pack import deployment_identity as pack_deployment_identity
from dpone_airflow_pack import mapping as pack_mapping
from dpone_airflow_pack import pack_task_runtime
from dpone_airflow_pack import run_identity as pack_run_identity
from dpone_airflow_pack.deployment_identity import deployment_identity_from_context
from dpone_airflow_pack.deployment_index_contract import (
    AirflowDeploymentIndex,
    AirflowIndexArtifact,
)
from dpone_airflow_pack.init_fetch_pod_guard import provider_env

from dpone.backfill import mapping as core_mapping
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.airflow_run_identity import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA,
    AirflowDeploymentIdentity,
    AirflowRunIdentity,
)
from dpone.contracts.airflow_run_identity import (
    AIRFLOW_RUN_IDENTITY_ENV as CORE_RUN_IDENTITY_ENV,
)
from dpone.contracts.airflow_run_identity import (
    AIRFLOW_RUN_IDENTITY_SCHEMA as CORE_RUN_IDENTITY_SCHEMA,
)
from dpone.contracts.airflow_run_identity import (
    MAX_AIRFLOW_RUN_IDENTITY_BYTES as CORE_MAX_RUN_IDENTITY_BYTES,
)


def test_run_identity_public_constants_match_core() -> None:
    assert pack_run_identity.AIRFLOW_RUN_IDENTITY_SCHEMA == CORE_RUN_IDENTITY_SCHEMA
    assert pack_run_identity.AIRFLOW_RUN_IDENTITY_ENV == CORE_RUN_IDENTITY_ENV
    assert pack_run_identity.MAX_AIRFLOW_RUN_IDENTITY_BYTES == CORE_MAX_RUN_IDENTITY_BYTES


def test_deployment_identity_public_constants_match_core() -> None:
    assert pack_deployment_identity.AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA == AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA
    assert pack_deployment_identity.AIRFLOW_DEPLOYMENT_IDENTITY_ENV == AIRFLOW_DEPLOYMENT_IDENTITY_ENV


def test_mapping_public_limits_match_core() -> None:
    assert pack_mapping.MAX_MAPPING_ITEMS == core_mapping.MAX_MAPPING_ITEMS
    assert pack_mapping.MAX_MAPPING_ACTIVE == core_mapping.MAX_MAPPING_ACTIVE
    assert pack_mapping.MAX_MAPPING_ITEM_BYTES == core_mapping.AIRFLOW_MAPPING_ITEM_MAX_BYTES
    assert pack_mapping.AIRFLOW_MAPPING_ITEM_ENV == core_mapping.AIRFLOW_MAPPING_ITEM_ENV


def test_mapping_fingerprint_matches_canonical_fingerprint() -> None:
    payload = {
        "mode": "visible",
        "chunks_total": 2,
        "limits": {"max_items": 200, "max_active": 16, "pool": "dpone_backfill"},
        "items": [
            {"item_index": 0, "first_chunk_index": 1, "last_chunk_index": 1, "chunks_count": 1},
            {"item_index": 1, "first_chunk_index": 2, "last_chunk_index": 2, "chunks_count": 1},
        ],
    }
    assert pack_mapping._fingerprint(payload) == canonical_fingerprint(payload)


def test_pack_built_run_identity_is_accepted_by_core_parser(tmp_path: Path) -> None:
    digest = "sha256:" + ("a" * 64)
    pack_digest = "sha256:" + ("b" * 64)
    pack_path = tmp_path / "packs" / "orders.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text("{}", encoding="utf-8")
    index = AirflowDeploymentIndex(
        path=tmp_path / "index.json",
        cache_root=tmp_path,
        release_id=digest,
        deployment_id=digest,
        dag_specs=(),
        workload_packs=(
            AirflowIndexArtifact(
                id="orders_daily",
                artifact_ref="cached://workloads/orders_daily",
                sha256=pack_digest,
                path=pack_path,
                bytes=2,
            ),
        ),
        binding_set_ref=digest,
        connection_registry_ref=digest,
        credential_runtime_ref=digest,
        runtime_image_digest=digest,
        airflow_bundle_ref="git:main",
    )
    context = pack_run_identity.build_dag_run_identity_context(
        index,
        dag_spec_id="orders_daily",
        dag_spec_sha256=digest,
    )
    identity = pack_run_identity.build_workload_run_identity(
        context,
        workload_id="orders_daily",
        pack_sha256=pack_digest,
    )

    parsed = AirflowRunIdentity.from_mapping(identity)
    assert parsed.release_id == digest
    assert parsed.workload_pack.id == "orders_daily"
    assert parsed.workload_pack.sha256 == pack_digest
    encoded = pack_run_identity.serialize_run_identity(identity)
    assert len(encoded.encode("utf-8")) <= CORE_MAX_RUN_IDENTITY_BYTES


def test_pack_keeps_activation_separate_from_immutable_run_identity(tmp_path: Path) -> None:
    digest = "sha256:" + ("a" * 64)
    pack_digest = "sha256:" + ("b" * 64)
    activation_id = "3f60628e-ef48-48b0-84c3-a9e27a82a7f2"
    pack_path = tmp_path / "packs" / "orders.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text("{}", encoding="utf-8")
    index = AirflowDeploymentIndex(
        path=tmp_path / "index.json",
        cache_root=tmp_path,
        release_id=digest,
        deployment_id=digest,
        activation_id=activation_id,
        workspace_authority_connection_ref="dpone_control",
        dag_specs=(),
        workload_packs=(
            AirflowIndexArtifact(
                id="orders_daily",
                artifact_ref="cached://workloads/orders_daily",
                sha256=pack_digest,
                path=pack_path,
                bytes=2,
            ),
        ),
        binding_set_ref=digest,
        connection_registry_ref=digest,
        credential_runtime_ref=digest,
        runtime_image_digest=digest,
        airflow_bundle_ref="git:main",
    )

    context = pack_run_identity.build_task_group_run_identity_context(index)
    assert context["_workspace_authority_connection_ref"] == "dpone_control"
    identity = pack_run_identity.build_workload_run_identity(
        context,
        workload_id="orders_daily",
        pack_sha256=pack_digest,
    )

    assert "activation_id" not in identity
    assert AirflowRunIdentity.from_mapping(identity).to_dict() == identity
    occurrence = deployment_identity_from_context(context)
    assert occurrence is not None
    assert AirflowDeploymentIdentity.from_mapping(occurrence).activation_id == activation_id

    kwargs: dict[str, object] = {}
    pack_task_runtime.apply_run_identity(
        kwargs=kwargs,
        pack={
            "_dpone_run_identity": identity,
            "_dpone_deployment_identity": occurrence,
            "_workspace_authority_connection_ref": context["_workspace_authority_connection_ref"],
        },
    )
    env = kwargs["env_vars"]
    assert isinstance(env, dict)
    run_identity_json = env["DPONE_AIRFLOW_RUN_IDENTITY"]
    deployment_identity_json = env["DPONE_AIRFLOW_DEPLOYMENT_IDENTITY"]
    assert isinstance(run_identity_json, str)
    assert isinstance(deployment_identity_json, str)
    assert "activation_id" not in run_identity_json
    assert AirflowDeploymentIdentity.from_mapping(json.loads(deployment_identity_json)).activation_id == activation_id
    assert env["DPONE_DBT_WORKSPACE_AUTHORITY_CONNECTION_REF"] == "dpone_control"
    assert provider_env(env) == env


def test_pack_run_identity_rejects_unsafe_bundle_ref(tmp_path: Path) -> None:
    digest = "sha256:" + ("c" * 64)
    index = AirflowDeploymentIndex(
        path=tmp_path / "index.json",
        cache_root=tmp_path,
        release_id=digest,
        deployment_id=digest,
        dag_specs=(),
        workload_packs=(),
        binding_set_ref=digest,
        connection_registry_ref=digest,
        credential_runtime_ref=digest,
        runtime_image_digest=digest,
        airflow_bundle_ref="s3://bucket/key?token=secret",
    )
    with pytest.raises(ValueError, match="DPONE_AIRFLOW_RUN_IDENTITY_INVALID"):
        pack_run_identity.build_dag_run_identity_context(
            index,
            dag_spec_id="x",
            dag_spec_sha256=digest,
        )
