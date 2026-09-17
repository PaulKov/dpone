"""Remote development delivery stays behind injected authority."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.app.release_composition import build_release_composition_service
from dpone.contracts.development_delivery_authority import DEVELOPMENT_COMPOSITION_PROFILE
from dpone.contracts.release_composition import ReleaseCompositionRequest
from dpone.readiness.airflow_artifact_delivery import (
    ArtifactRegistryOptions,
    materialize_command_result,
    publish_command_result,
)
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionService
from dpone.runtime.airflow_artifact_delivery import (
    AirflowArtifactDeliveryError,
    AirflowArtifactMaterializer,
    AirflowArtifactPublisher,
)
from dpone.runtime.airflow_artifact_delivery_models import MaterializeRequest, PublishRequest
from tests.dbt_compact_wire_v2_helpers import IMAGE, SIDECAR, prepare_projects, workspace_service
from tests.test_airflow_remote_artifact_delivery import _registry
from tests.test_dbt_airflow_release_e2e import _config_map_ref, _write_environment
from tests.test_dbt_compact_wire_v2 import _development_authority
from tests.test_release_composition_ordinary import ordinary_root


@pytest.mark.parametrize("composed", [False, True])
def test_remote_development_delivery_requires_injected_authority(tmp_path: Path, composed: bool) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=composed)
    registry = _registry(tmp_path)
    publish_request = PublishRequest(
        cache_root=cache,
        release_id=release_id,
        deployment_id=deployment_id,
        environment="prod",
        artifact_registry_ref="synthetic-artifacts",
        publication_mode="exact",
    )

    with pytest.raises(AirflowArtifactDeliveryError) as denied_publish:
        AirflowArtifactPublisher(registry=registry).publish(publish_request)
    assert denied_publish.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"

    published = AirflowArtifactPublisher(
        registry=registry,
        development_authority=authority,
    ).publish(publish_request)
    assert published.status == "published"

    materialize_request = MaterializeRequest(
        cache_root=tmp_path / "remote-cache",
        release_id=release_id,
        deployment_id=deployment_id,
        environment="prod",
        artifact_registry_ref="synthetic-artifacts",
    )
    with pytest.raises(AirflowArtifactDeliveryError) as denied_materialize:
        AirflowArtifactMaterializer(registry=registry).materialize(materialize_request)
    assert denied_materialize.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert not (materialize_request.cache_root / "releases").exists()

    installed = AirflowArtifactMaterializer(
        registry=registry,
        development_authority=authority,
    ).materialize(materialize_request)
    assert installed.projection_verified


def test_public_cli_cannot_self_authorize_development_delivery(tmp_path: Path) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=True)
    options = ArtifactRegistryOptions(
        registry_uri="s3://synthetic-artifacts/airflow",
        local_registry_root=str(tmp_path / "registry"),
    )
    request = PublishRequest(
        cache_root=cache,
        release_id=release_id,
        deployment_id=deployment_id,
        environment="prod",
        artifact_registry_ref="synthetic-artifacts",
        publication_mode="exact",
    )
    AirflowArtifactPublisher(
        registry=options.build(),
        development_authority=authority,
    ).publish(request)
    published = publish_command_result(
        cache_root=str(cache),
        release_id=release_id,
        deployment_id=deployment_id,
        environment="prod",
        artifact_registry_ref="synthetic-artifacts",
        max_object_bytes=64 * 1024 * 1024,
        max_total_bytes=512 * 1024 * 1024,
        registry_options=options,
        publication_mode="exact",
    )
    assert not published.passed
    assert published.errors[0]["code"] == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"

    materialized = materialize_command_result(
        cache_root=str(tmp_path / "cli-cache"),
        release_id=release_id,
        deployment_id=deployment_id,
        environment="prod",
        artifact_registry_ref="synthetic-artifacts",
        max_object_bytes=64 * 1024 * 1024,
        max_total_bytes=512 * 1024 * 1024,
        registry_options=options,
    )
    assert not materialized.passed
    assert materialized.errors[0]["code"] == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"


def _development_projection(tmp_path: Path, *, composed: bool) -> tuple[Path, str, str]:
    authority = _development_authority()
    workspace = tmp_path / "workspace"
    prepare_projects(workspace)
    compiled = tmp_path / "compiled"
    assert (
        workspace_service(
            tmp_path / "profiles",
            development_authority=authority,
        )
        .compile(workspace, output_dir=compiled)
        .passed
    )
    native = materialize_compact_pack_release(
        pack_root=compiled,
        cache_root=tmp_path / "native-cache",
        xcom_sidecar_image=SIDECAR,
        development_authority=authority,
    )
    assert native.passed
    source = Path(native.release_dir)
    if composed:
        ordinary = ordinary_root(tmp_path)
        service = build_release_composition_service(development_authority=authority)
        inventory = service.inventory(ordinary, xcom_sidecar_image=SIDECAR)
        output = tmp_path / "composed"
        report = service.compose(
            ReleaseCompositionRequest(
                native_root=source,
                expected_release_id=native.release_id,
                standalone_root=ordinary,
                expected_inventory_sha256=inventory["inventory_sha256"],
                output_dir=output,
                xcom_sidecar_image=SIDECAR,
                profile=DEVELOPMENT_COMPOSITION_PROFILE,
            )
        )
        assert report.passed
        source = output
    cache = tmp_path / ".dpone-cache"
    installed = materialize_compact_pack_release(
        pack_root=source,
        cache_root=cache,
        xcom_sidecar_image=SIDECAR,
        development_authority=authority,
    )
    assert installed.passed, installed.blockers
    release = json.loads(Path(installed.release_dir, "release-set.json").read_bytes())
    _write_environment(tmp_path)
    projection = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=release["release_id"],
        environment="prod",
        trust_tier="non_production",
        runtime_image_ref=IMAGE,
        runtime_image_digest=IMAGE.split("@")[-1],
        artifact_registry_ref="synthetic-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
    )
    return cache, release["release_id"], projection.deployment["deployment_id"]
