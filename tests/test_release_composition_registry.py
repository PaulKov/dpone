"""Real registry publication/materialization preserves the entire composition."""

import json

from dpone.app.release_composition import build_release_composition_service
from dpone.runtime.airflow_artifact_delivery_models import MaterializeRequest, PublishRequest
from dpone.runtime.airflow_artifact_materialization import AirflowArtifactMaterializer
from dpone.runtime.airflow_artifact_publication import AirflowArtifactPublisher
from tests.test_airflow_remote_artifact_delivery import _registry
from tests.test_dbt_compact_wire_v2 import verify_delivery
from tests.test_release_composition_delivery import composition_request as composition_request


def test_registry_roundtrip_preserves_source_readmission(composition_request):
    service = build_release_composition_service()
    report = service.compose(composition_request)
    assert report.passed, report.blockers
    state = composition_request.output_dir.parent
    installed = verify_delivery(state, composition_request.output_dir)
    cache = state / ".dpone-cache"
    deployment_path = next((cache / "deployments/prod").glob("*/deployment.json"))
    deployment = json.loads(deployment_path.read_bytes())
    registry = _registry(state)
    publication = AirflowArtifactPublisher(registry=registry).publish(
        PublishRequest(
            cache_root=cache,
            release_id=report.release_id,
            deployment_id=deployment["deployment_id"],
            environment="prod",
            artifact_registry_ref="synthetic-artifacts",
            publication_mode="exact",
        )
    )
    assert publication.status == "published"
    destination = state / "downloaded"
    request = MaterializeRequest(
        cache_root=destination,
        release_id=report.release_id,
        deployment_id=deployment["deployment_id"],
        environment="prod",
        artifact_registry_ref="synthetic-artifacts",
    )
    materializer = AirflowArtifactMaterializer(registry=registry)
    first = materializer.materialize(request)
    assert first.projection_verified and not first.activated
    remote = destination / "releases" / installed.name
    original_files = {p.relative_to(installed): p.read_bytes() for p in installed.rglob("*") if p.is_file()}
    remote_files = {p.relative_to(remote): p.read_bytes() for p in remote.rglob("*") if p.is_file()}
    assert original_files == remote_files
    assert materializer.materialize(request).status == "no_op"
    readmitted = service.install(remote, cache_root=state / "readmitted")
    assert readmitted.passed and readmitted.release_id == report.release_id
    assert not (destination / "current").exists()


def test_real_cache_promotion_rejects_composition_before_native_coordinator(composition_request):
    import pytest

    from dpone.runtime.deployment_cache_common import DeploymentCacheError
    from dpone.runtime.deployment_cache_materializer import DeploymentCacheMaterializer

    calls = []

    class NativeCoordinator:
        def __getattr__(self, name):
            calls.append(name)
            raise AssertionError("composition must not reach native coordinator")

    report = build_release_composition_service().compose(composition_request)
    assert report.passed
    state = composition_request.output_dir.parent
    verify_delivery(state, composition_request.output_dir)
    cache = state / ".dpone-cache"
    deployment = next((cache / "deployments/prod").glob("*/deployment.json")).parent
    current = cache / "current"
    assert not current.exists()
    with pytest.raises(DeploymentCacheError) as caught:
        DeploymentCacheMaterializer(cache, workspace_activation=NativeCoordinator()).promote(
            deployment, environment="prod", expect_current_absent=True
        )
    assert caught.value.code == "DPONE_COMPOSITION_ADMISSION_UNAVAILABLE"
    assert calls == []
    assert not current.exists()
