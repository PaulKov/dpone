from __future__ import annotations

import hashlib
import json
import sys
import types
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath

import pytest
from dpone_airflow_pack.semantic_refresh_dag_authority import (
    LocalSemanticRefreshDagProjectionAuthority,
    SemanticRefreshDagProjectionIdentity,
)

from dpone import __version__
from dpone.adapters.object_storage_artifact_registry import (
    ObjectStorageArtifactRegistry,
    object_storage_registry_scope_id,
)
from dpone.cli import main as cli_main
from dpone.contracts.airflow_artifact_attestation import (
    AirflowArtifactAttestationVerification,
)
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.airflow_deployment import deployment_id as compute_deployment_id
from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.dbt_release import (
    DBT_RELEASE_WIRE_CONTRACT,
    dbt_selection_fingerprint,
)
from dpone.contracts.runtime_artifact_attestation import runtime_attestation_bundle_key
from dpone.ports.artifact_registry import (
    ArtifactMetadata,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryUnavailable,
    CreateResult,
)
from dpone.readiness.airflow_artifact_delivery import ArtifactRegistryOptions, publish_command_result
from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result
from dpone.runtime.airflow_artifact_delivery import (
    AirflowArtifactDeliveryError,
    AirflowArtifactMaterializer,
    AirflowArtifactPublisher,
)
from dpone.runtime.airflow_artifact_delivery_models import MaterializeRequest, PublishRequest
from dpone.runtime.airflow_artifact_delivery_support import from_cache_error
from dpone.runtime.airflow_artifact_inventory import declared_release_artifacts
from dpone.runtime.airflow_artifact_publication import prepare_publication
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.services.airflow_artifact_attestation_consumer import (
    AirflowArtifactAttestationRejected,
)
from dpone.services.airflow_artifact_attestation_registry import (
    AirflowArtifactAttestationRegistryError,
)
from dpone.storage.local import LocalObjectStorageClient
from dpone.storage.models import ObjectStorageUri
from tests.support.airflow_artifact_projection import write_exact_test_projection


def test_publish_is_create_or_compare_and_writes_completion_markers_last(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    registry = _registry(tmp_path)
    request = _publish_request(
        source_cache,
        release_id,
        deployment_id,
        publication_mode="exact",
    )
    object_count = len(prepare_publication(request).objects)

    first = AirflowArtifactPublisher(registry=registry).publish(request)
    second = AirflowArtifactPublisher(registry=registry).publish(request)

    assert first.status == "published"
    assert first.created_objects == object_count
    assert first.existing_equal_objects == 0
    assert second.status == "no_op"
    assert second.created_objects == 0
    assert second.existing_equal_objects == object_count
    assert first.verified_objects == object_count
    assert second.verified_objects == object_count
    assert first.publication_commitment == second.publication_commitment
    payload = first.to_dict()
    assert payload["schema"] == "dpone.airflow-artifact-publish.v2"
    assert payload["publication_commitment"]["verification_mode"] == "remote_readback_sha256"
    assert set(payload["publication_commitment"]) == {
        "schema",
        "verification_mode",
        "registry_scope_id",
        "projection_verified",
        "release",
        "deployment",
        "airflow_index",
    }
    assert registry.stat(_key("releases", release_id, "_SUCCESS")).size_bytes > 0
    assert registry.stat(_key("deployments", "dev", deployment_id, "_SUCCESS")).size_bytes > 0


def test_publish_rejects_registry_outside_expected_trusted_scope(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    request = PublishRequest(
        cache_root=source_cache,
        release_id=release_id,
        deployment_id=deployment_id,
        environment="dev",
        artifact_registry_ref="local-test",
        registry_scope_id="sha256:" + "f" * 64,
        publication_mode="exact",
    )

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=_registry(tmp_path)).publish(request)

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_SCOPE_MISMATCH"


def test_exact_publication_rejects_legacy_projection_before_registry_io(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _NoIoRegistry()
    request = PublishRequest(
        cache_root=source_cache,
        release_id=release_id,
        deployment_id=deployment_id,
        environment="dev",
        artifact_registry_ref="local-test",
        publication_mode="exact",
    )

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(request)

    assert exc.value.code == "DPONE_EXACT_PUBLICATION_PROJECTION_REQUIRED"
    assert registry.calls == 0


def test_compatible_publication_keeps_historical_v1_output(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)

    payload = (
        AirflowArtifactPublisher(registry=_registry(tmp_path))
        .publish(_publish_request(source_cache, release_id, deployment_id))
        .to_dict()
    )

    assert payload["schema"] == "dpone.airflow-artifact-publish.v1"
    assert "verified_objects" not in payload
    assert "publication_commitment" not in payload


def test_compatible_publication_accepts_historical_registry_port(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _LegacyRegistry(_registry(tmp_path))

    report = AirflowArtifactPublisher(registry=registry).publish(
        _publish_request(source_cache, release_id, deployment_id)
    )

    assert report.to_dict()["schema"] == "dpone.airflow-artifact-publish.v1"
    assert registry.calls > 0


def test_exact_publication_requires_registry_authority_before_write(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    registry = _LegacyRegistry(_registry(tmp_path))

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(
            _publish_request(
                source_cache,
                release_id,
                deployment_id,
                publication_mode="exact",
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID"
    assert registry.calls == 0


def test_registry_scope_identity_is_stable_and_root_specific() -> None:
    root = ObjectStorageUri.parse("s3://example-data-bucket/dpone-artifacts/prod/example-workloads")

    first = object_storage_registry_scope_id(root)
    second = object_storage_registry_scope_id(root)
    another_root = object_storage_registry_scope_id(
        ObjectStorageUri.parse("s3://example-data-bucket/dpone-artifacts/dev/example-workloads")
    )
    another_bucket = object_storage_registry_scope_id(
        ObjectStorageUri.parse("s3://other-bucket/dpone-artifacts/prod/example-workloads")
    )
    expected = canonical_fingerprint(
        {
            "schema": "dpone.artifact-registry-scope.v1",
            "kind": "object_storage",
            "provider": root.provider_name,
            "account": root.account,
            "bucket": root.bucket,
            "root": root.key,
        }
    )

    assert first == second
    assert first == expected
    assert first.startswith("sha256:")
    assert len(first) == 71
    assert len({first, another_root, another_bucket}) == 3


def test_publish_commitment_matches_verified_root_artifact_bytes(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = write_exact_test_projection(source_cache)

    report = AirflowArtifactPublisher(registry=_registry(tmp_path)).publish(
        _publish_request(
            source_cache,
            release_id,
            deployment_id,
            publication_mode="exact",
        )
    )

    commitment = report.to_dict()["publication_commitment"]
    expected = {
        "release": source_cache / "releases" / _digest_dir(release_id) / "release-set.json",
        "deployment": (source_cache / "deployments" / "dev" / _digest_dir(deployment_id) / "deployment.json"),
        "airflow_index": (source_cache / "deployments" / "dev" / _digest_dir(deployment_id) / "airflow-index.json"),
    }
    for name, path in expected.items():
        descriptor = commitment[name]
        payload = path.read_bytes()
        assert descriptor["sha256"] == _sha256(payload)
        assert descriptor["bytes"] == len(payload)


def test_publish_fails_when_created_object_remote_readback_is_corrupt(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    registry = _CorruptingReadbackRegistry(_registry(tmp_path))

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(
            _publish_request(
                source_cache,
                release_id,
                deployment_id,
                publication_mode="exact",
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH"
    assert exc.value.details["created_objects"] == 1
    assert exc.value.details["verified_objects"] == 0
    assert exc.value.details["published_release"] is False
    assert exc.value.details["published_deployment"] is False


def test_publish_calls_registry_in_release_then_deployment_marker_order(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    recording = _RecordingRegistry(_registry(tmp_path))

    AirflowArtifactPublisher(registry=recording).publish(_publish_request(source_cache, release_id, deployment_id))

    release_marker = _key("releases", release_id, "_SUCCESS")
    deployment_marker = _key("deployments", "dev", deployment_id, "_SUCCESS")
    assert recording.created_keys[-1] == deployment_marker
    assert recording.created_keys.index(release_marker) == 2
    assert all(
        key.parts[0] == "releases" for key in recording.created_keys[: recording.created_keys.index(release_marker) + 1]
    )


def test_concurrent_publishers_converge_without_overwrite(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    request = _publish_request(source_cache, release_id, deployment_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        reports = tuple(executor.map(lambda _: AirflowArtifactPublisher(registry=registry).publish(request), range(2)))

    assert sum(report.created_objects for report in reports) == 9
    assert sum(report.existing_equal_objects for report in reports) == 9
    materialized = AirflowArtifactMaterializer(registry=registry).materialize(
        _materialize_request(tmp_path / "target-cache", release_id, deployment_id)
    )
    assert materialized.projection_verified is True


def test_concurrent_materializers_converge_without_partial_tree_conflicts(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))
    request = _materialize_request(target_cache, release_id, deployment_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        reports = tuple(
            executor.map(lambda _: AirflowArtifactMaterializer(registry=registry).materialize(request), range(2))
        )

    assert {report.status for report in reports} <= {"materialized", "no_op"}
    assert any(report.status == "materialized" for report in reports)
    assert sum(report.local_release_state == "created" for report in reports) == 1
    assert sum(report.local_deployment_state == "created" for report in reports) == 1
    DeploymentCacheProjectionValidator(target_cache).validate_details(
        target_cache / "deployments" / "dev" / _digest_dir(deployment_id),
        environment="dev",
    )


def test_partial_publication_resumes_from_exact_remote_bytes(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    registry = _FailOnceRegistry(_registry(tmp_path), fail_on_call=3)
    request = _publish_request(
        source_cache,
        release_id,
        deployment_id,
        publication_mode="exact",
    )
    object_count = len(prepare_publication(request).objects)

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(request)

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE"
    assert exc.value.details == {
        "created_objects": 2,
        "existing_equal_objects": 0,
        "verified_objects": 2,
        "published_release": False,
        "published_deployment": False,
    }
    with pytest.raises(ArtifactRegistryObjectNotFound):
        registry.stat(_key("releases", release_id, "_SUCCESS"))

    resumed = AirflowArtifactPublisher(registry=registry).publish(request)
    assert resumed.status == "published"
    assert resumed.created_objects == object_count - 2
    assert resumed.existing_equal_objects == 2
    assert registry.stat(_key("deployments", "dev", deployment_id, "_SUCCESS")).size_bytes > 0


def test_timeout_after_successful_remote_create_recovers_by_compare_on_retry(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _CreateThenTimeoutRegistry(_registry(tmp_path))
    request = _publish_request(source_cache, release_id, deployment_id)

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(request)

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE"
    recovered = AirflowArtifactPublisher(registry=registry).publish(request)
    assert recovered.status == "published"
    assert recovered.existing_equal_objects >= 1
    assert recovered.published_release is True
    assert recovered.published_deployment is True


def test_publish_failure_report_preserves_partial_remote_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    registry = _FailOnceRegistry(_registry(tmp_path), fail_on_call=5)
    monkeypatch.setattr(ArtifactRegistryOptions, "build", lambda self: registry)

    result = publish_command_result(
        cache_root=str(source_cache),
        release_id=release_id,
        deployment_id=deployment_id,
        environment="dev",
        artifact_registry_ref="local-test",
        max_object_bytes=64 * 1024 * 1024,
        max_total_bytes=512 * 1024 * 1024,
        registry_options=ArtifactRegistryOptions(
            registry_uri="s3://dpone-artifacts/airflow",
            local_registry_root=str(tmp_path / "unused"),
        ),
        publication_mode="exact",
    )

    assert result.passed is False
    assert result.details["created_objects"] == 4
    assert result.details["existing_equal_objects"] == 0
    assert result.details["verified_objects"] == 4
    assert result.details["published_release"] is True
    assert result.details["published_deployment"] is False
    assert result.details["publication_commitment"] is None


def test_attestation_publication_precedes_release_marker_and_replay_is_no_op(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        environment="prod",
        attestation_policy="required_for_prod",
        trust_tier="production",
    )
    bundle = tmp_path / "release-set.github.sigstore.jsonl"
    bundle_bytes = b'{"attestation":"verified-release-set"}\n'
    bundle.write_bytes(bundle_bytes)
    registry = _RecordingRegistry(_registry(tmp_path))
    request = _publish_request(
        source_cache,
        release_id,
        deployment_id,
        environment="prod",
        attestation_bundle_path=bundle,
    )
    attestation_key = _attestation_key(source_cache, release_id)
    release_marker = _key("releases", release_id, "_SUCCESS")

    first = AirflowArtifactPublisher(registry=registry).publish(request)

    assert first.status == "published"
    assert first.published_release is True
    assert first.published_deployment is True
    assert registry.created_keys.index(attestation_key) < registry.created_keys.index(release_marker)
    assert _remote_object_path(tmp_path, attestation_key).read_bytes() == bundle_bytes

    registry.created_keys.clear()
    second = AirflowArtifactPublisher(registry=registry).publish(request)

    assert second.status == "no_op"
    assert second.created_objects == 0
    assert second.existing_equal_objects == first.created_objects
    assert registry.created_keys.count(attestation_key) == 1
    assert _remote_object_path(tmp_path, attestation_key).read_bytes() == bundle_bytes


def test_attestation_immutable_conflict_stops_before_completion_markers(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        environment="prod",
        attestation_policy="required_for_prod",
        trust_tier="production",
    )
    original_bundle = tmp_path / "original.github.sigstore.jsonl"
    original_bytes = b'{"attestation":"original"}\n'
    original_bundle.write_bytes(original_bytes)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(
        _publish_request(
            source_cache,
            release_id,
            deployment_id,
            environment="prod",
            attestation_bundle_path=original_bundle,
        )
    )
    changed_bundle = tmp_path / "changed.github.sigstore.jsonl"
    changed_bundle.write_bytes(b'{"attestation":"different-bytes-at-the-same-key"}\n')
    recording = _RecordingRegistry(registry)
    attestation_key = _attestation_key(source_cache, release_id)

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=recording).publish(
            _publish_request(
                source_cache,
                release_id,
                deployment_id,
                environment="prod",
                attestation_bundle_path=changed_bundle,
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_IMMUTABILITY_CONFLICT"
    assert recording.created_keys[-1] == attestation_key
    assert all(not key.name == "_SUCCESS" for key in recording.created_keys)
    assert _remote_object_path(tmp_path, attestation_key).read_bytes() == original_bytes


def test_attestation_partial_failure_before_release_marker_recovers_on_retry(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        environment="prod",
        attestation_policy="required_for_prod",
        trust_tier="production",
    )
    bundle = tmp_path / "release-set.github.sigstore.jsonl"
    bundle_bytes = b'{"attestation":"retry-safe"}\n'
    bundle.write_bytes(bundle_bytes)
    registry = _FailOnceRegistry(_registry(tmp_path), fail_on_call=4)
    request = _publish_request(
        source_cache,
        release_id,
        deployment_id,
        environment="prod",
        attestation_bundle_path=bundle,
    )
    attestation_key = _attestation_key(source_cache, release_id)
    release_marker = _key("releases", release_id, "_SUCCESS")
    deployment_marker = _key("deployments", "prod", deployment_id, "_SUCCESS")

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(request)

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE"
    assert exc.value.details["created_objects"] == 3
    assert exc.value.details["published_release"] is False
    assert exc.value.details["published_deployment"] is False
    assert attestation_key in registry.created_keys
    assert release_marker not in registry.created_keys
    with pytest.raises(ArtifactRegistryObjectNotFound):
        registry.stat(release_marker)
    with pytest.raises(ArtifactRegistryObjectNotFound):
        registry.stat(deployment_marker)
    assert _remote_object_path(tmp_path, attestation_key).read_bytes() == bundle_bytes

    resumed = AirflowArtifactPublisher(registry=registry).publish(request)

    assert resumed.status == "published"
    assert resumed.existing_equal_objects == 3
    assert resumed.published_release is True
    assert resumed.published_deployment is True
    assert registry.stat(release_marker).size_bytes > 0
    assert registry.stat(deployment_marker).size_bytes > 0
    assert _remote_object_path(tmp_path, attestation_key).read_bytes() == bundle_bytes


@pytest.mark.parametrize(
    ("bundle_case", "expected_code"),
    [
        ("missing", "DPONE_ARTIFACT_ATTESTATION_BUNDLE_INVALID"),
        ("empty", "DPONE_ARTIFACT_ATTESTATION_BUNDLE_INVALID"),
        ("oversized", "DPONE_ARTIFACT_REGISTRY_OBJECT_TOO_LARGE"),
    ],
)
def test_invalid_attestation_bundle_fails_before_registry_io(
    tmp_path: Path,
    bundle_case: str,
    expected_code: str,
) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        environment="prod",
        attestation_policy="required_for_prod",
        trust_tier="production",
    )
    bundle = tmp_path / "release-set.github.sigstore.jsonl"
    if bundle_case == "empty":
        bundle.touch()
    elif bundle_case == "oversized":
        bundle.write_bytes(b"x" * (64 * 1024 + 1))
    registry = _NoIoRegistry()

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(
            _publish_request(
                source_cache,
                release_id,
                deployment_id,
                environment="prod",
                attestation_bundle_path=bundle,
                max_object_bytes=64 * 1024,
            )
        )

    assert registry.calls == 0
    assert exc.value.code == expected_code


def test_release_v2_core_projection_can_publish_before_deployment_attestation(
    tmp_path: Path,
) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        release_schema="dpone.release-set.v2",
    )
    report = AirflowArtifactPublisher(registry=_registry(tmp_path)).publish(
        _publish_request(source_cache, release_id, deployment_id)
    )

    assert report.status == "published"
    assert report.published_release is True
    assert report.published_deployment is True


def test_release_v1_preserves_publication_compatibility_without_attestation(
    tmp_path: Path,
) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        environment="prod",
        trust_tier="production",
    )

    report = AirflowArtifactPublisher(registry=_registry(tmp_path)).publish(
        _publish_request(source_cache, release_id, deployment_id, environment="prod")
    )

    assert report.status == "published"


def test_publish_rejects_different_bytes_at_existing_content_addressed_key(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    request = _publish_request(source_cache, release_id, deployment_id)
    AirflowArtifactPublisher(registry=registry).publish(request)
    remote_release_set = (
        tmp_path
        / "registry"
        / "s3"
        / "dpone-artifacts"
        / "airflow"
        / "releases"
        / _digest_dir(release_id)
        / "release-set.json"
    )
    remote_release_set.write_text("{}\n", encoding="utf-8")

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(request)

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_IMMUTABILITY_CONFLICT"


def test_publish_validates_all_local_bytes_before_registry_io(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    dag_path = source_cache / "releases" / _digest_dir(release_id) / "dags" / "orders_daily.dag-spec.json"
    dag_path.write_bytes(dag_path.read_bytes().replace(b"orders_daily", b"broken_daily"))
    registry = _NoIoRegistry()

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))

    assert exc.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"
    assert registry.calls == 0


def test_publish_uses_verified_snapshot_when_source_changes_during_remote_io(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    dag_path = source_cache / "releases" / _digest_dir(release_id) / "dags" / "orders_daily.dag-spec.json"
    original = dag_path.read_bytes()
    registry = _MutatingRegistry(_registry(tmp_path), mutate_path=dag_path)

    report = AirflowArtifactPublisher(registry=registry).publish(
        _publish_request(source_cache, release_id, deployment_id)
    )

    assert report.status == "published"
    downloaded = tmp_path / "published-dag.json"
    registry.download_file(
        _key("releases", release_id, "dags/orders_daily.dag-spec.json"),
        downloaded,
        max_bytes=len(original),
    )
    assert downloaded.read_bytes() == original


def test_materialize_downloads_exact_pins_without_activating_current(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))

    report = AirflowArtifactMaterializer(registry=registry).materialize(
        MaterializeRequest(
            cache_root=target_cache,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="dev",
            artifact_registry_ref="local-test",
        )
    )

    assert report.status == "materialized"
    assert report.projection_verified is True
    assert report.activated is False
    assert report.downloaded_objects == 8
    assert not (target_cache / "current").exists()
    deployment_dir = target_cache / "deployments" / "dev" / _digest_dir(deployment_id)
    validated = DeploymentCacheProjectionValidator(target_cache).validate_details(
        deployment_dir,
        environment="dev",
    )
    assert validated.deployment_id == deployment_id
    assert validated.release_id == release_id

    repeated = AirflowArtifactMaterializer(registry=registry).materialize(
        MaterializeRequest(
            cache_root=target_cache,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="dev",
            artifact_registry_ref="local-test",
        )
    )
    assert repeated.status == "no_op"
    assert repeated.local_release_state == "no_op"
    assert repeated.local_deployment_state == "no_op"


def test_publish_and_materialize_preserve_exact_semantic_refresh_sidecar(
    tmp_path: Path,
) -> None:
    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    deployment_dir = source_cache / "deployments" / "dev" / _digest_dir(deployment_id)
    identity = SemanticRefreshDagProjectionIdentity(
        dag_projection_sha256="sha256:" + "1" * 64,
        release_id=release_id,
        deployment_id=deployment_id,
        topology_sha256="sha256:" + "2" * 64,
        template_pack_fingerprint="sha256:" + "3" * 64,
        plan_bundle_sha256="sha256:" + "4" * 64,
        workflow_plan_sha256="sha256:" + "5" * 64,
        pre_release_bundle_sha256="sha256:" + "6" * 64,
        package_artifacts_sha256="sha256:" + "7" * 64,
    )
    filename = "semantic-refresh-" + "1" * 64 + ".dag-projection.json"
    sidecar = deployment_dir / filename
    sidecar_bytes = b'{"schema":"test.semantic-refresh-sidecar"}\n'
    sidecar.write_bytes(sidecar_bytes)
    index_path = deployment_dir / "airflow-index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    index["semantic_refresh_dag_projections"] = [
        {
            "projection_id": "semantic_refresh_v2::daily_events",
            "workflow_name": "daily_events",
            "dag_id": "daily_events",
            "dag_projection_sha256": identity.dag_projection_sha256,
            "artifact_ref": (f"cache://deployments/dev/{_digest_dir(deployment_id)}/{filename}"),
            "artifact_sha256": _sha256(sidecar_bytes),
            "artifact_bytes": len(sidecar_bytes),
            "authority": LocalSemanticRefreshDagProjectionAuthority.build(identity).to_mapping(),
        }
    ]
    index_path.write_text(json.dumps(index, sort_keys=True), encoding="utf-8")
    registry = _registry(tmp_path)

    AirflowArtifactPublisher(registry=registry).publish(
        _publish_request(
            source_cache,
            release_id,
            deployment_id,
            publication_mode="exact",
        )
    )
    AirflowArtifactMaterializer(registry=registry).materialize(
        _materialize_request(target_cache, release_id, deployment_id)
    )

    assert (target_cache / "deployments" / "dev" / _digest_dir(deployment_id) / filename).read_bytes() == sidecar_bytes


def test_remote_delivery_requires_separate_reviewed_cache_sync_for_activation(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))
    AirflowArtifactMaterializer(registry=registry).materialize(
        _materialize_request(target_cache, release_id, deployment_id)
    )
    deployment_dir = target_cache / "deployments" / "dev" / _digest_dir(deployment_id)

    promoted = cache_sync_result(
        cache_root=target_cache,
        deployment_dir=deployment_dir,
        environment="dev",
        promoted_by="ci://test/dpone-airflow",
        allowed_promoters=("ci://test/dpone-airflow",),
        expect_current_absent=True,
        confirm_promote=True,
    )

    assert promoted.passed is True
    assert promoted.details is not None
    assert promoted.details["deployment_id"] == deployment_id
    assert (target_cache / "current").is_symlink()


def test_materialize_fails_closed_when_remote_completion_marker_is_missing(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    request = _publish_request(source_cache, release_id, deployment_id)
    AirflowArtifactPublisher(registry=registry).publish(request)
    marker = (
        tmp_path
        / "registry"
        / "s3"
        / "dpone-artifacts"
        / "airflow"
        / "deployments"
        / "dev"
        / _digest_dir(deployment_id)
        / "_SUCCESS"
    )
    marker.unlink()

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            MaterializeRequest(
                cache_root=tmp_path / "target-cache",
                release_id=release_id,
                deployment_id=deployment_id,
                environment="dev",
                artifact_registry_ref="local-test",
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_INCOMPLETE"
    assert not (tmp_path / "target-cache" / "current").exists()


def test_materialize_rejects_downloaded_release_checksum_drift(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))
    remote_dag = (
        tmp_path
        / "registry"
        / "s3"
        / "dpone-artifacts"
        / "airflow"
        / "releases"
        / _digest_dir(release_id)
        / "dags"
        / "orders_daily.dag-spec.json"
    )
    remote_dag.write_bytes(remote_dag.read_bytes().replace(b"orders", b"broken"))

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(tmp_path / "target-cache", release_id, deployment_id)
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH"
    assert not (tmp_path / "target-cache" / "releases" / _digest_dir(release_id)).exists()


def test_materialize_enforces_remote_object_budget_before_download(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            MaterializeRequest(
                cache_root=tmp_path / "target-cache",
                release_id=release_id,
                deployment_id=deployment_id,
                environment="dev",
                artifact_registry_ref="local-test",
                max_object_bytes=3,
                max_total_bytes=1024,
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_OBJECT_TOO_LARGE"


def test_materialize_enforces_total_budget_before_install(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    publish_request = _publish_request(source_cache, release_id, deployment_id)
    inventory = prepare_publication(publish_request)
    AirflowArtifactPublisher(registry=registry).publish(publish_request)
    total_without_markers = sum(item.size_bytes for item in inventory.objects if not item.completion_marker)
    max_object = max(item.size_bytes for item in inventory.objects)

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            MaterializeRequest(
                cache_root=tmp_path / "target-cache",
                release_id=release_id,
                deployment_id=deployment_id,
                environment="dev",
                artifact_registry_ref="local-test",
                max_object_bytes=max_object,
                max_total_bytes=total_without_markers - 1,
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_TOTAL_LIMIT_EXCEEDED"
    assert not (tmp_path / "target-cache" / "releases" / _digest_dir(release_id)).exists()


def test_materialize_rejects_negative_remote_metadata_before_download(tmp_path: Path) -> None:
    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    registry = _NegativeMetadataRegistry()

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(tmp_path / "target-cache", release_id, deployment_id)
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_METADATA_INVALID"
    assert registry.download_calls == 0


def test_materialize_rejects_symlinked_cache_root_before_registry_io(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    cache_root = tmp_path / "target-cache"
    cache_root.symlink_to(outside, target_is_directory=True)
    registry = _NoIoRegistry()

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(cache_root, "sha256:" + "a" * 64, "sha256:" + "b" * 64)
        )

    assert exc.value.code == "DPONE_CACHE_PATH_ESCAPE"
    assert registry.calls == 0
    assert list(outside.iterdir()) == []


def test_materialize_detects_cache_root_swap_before_local_install(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    release_id, deployment_id = _write_projection(source_cache)
    delegate = _registry(tmp_path)
    AirflowArtifactPublisher(registry=delegate).publish(_publish_request(source_cache, release_id, deployment_id))
    outside = tmp_path / "outside"
    outside.mkdir()
    registry = _CacheRootSwappingRegistry(delegate, cache_root=target_cache, outside=outside)

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(target_cache, release_id, deployment_id)
        )

    assert exc.value.code == "DPONE_CACHE_PATH_ESCAPE"
    assert list(outside.iterdir()) == []
    assert not (tmp_path / "moved-cache" / "releases").exists()


def test_materialize_install_remains_anchored_if_cache_root_changes_after_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.runtime.airflow_artifact_materialization as materialization

    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    moved_cache = tmp_path / "moved-cache"
    outside = tmp_path / "outside"
    outside.mkdir()
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))
    real_materialize = materialization.materialize_immutable_local_tree_at
    swapped = False

    def swap_root_then_install(*args, **kwargs):
        nonlocal swapped
        if not swapped:
            swapped = True
            target_cache.rename(moved_cache)
            target_cache.symlink_to(outside, target_is_directory=True)
        return real_materialize(*args, **kwargs)

    monkeypatch.setattr(materialization, "materialize_immutable_local_tree_at", swap_root_then_install)

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(target_cache, release_id, deployment_id)
        )

    assert exc.value.code == "DPONE_CACHE_PATH_ESCAPE"
    assert list(outside.iterdir()) == []
    assert (moved_cache / "releases" / _digest_dir(release_id) / "release-set.json").exists()


def test_artifact_delivery_normalizes_trusted_symlinked_cache_parent(tmp_path: Path) -> None:
    physical_parent = tmp_path / "physical"
    physical_parent.mkdir()
    logical_parent = tmp_path / "logical"
    logical_parent.symlink_to(physical_parent, target_is_directory=True)

    request = MaterializeRequest(
        cache_root=logical_parent / "cache",
        release_id="sha256:" + "a" * 64,
        deployment_id="sha256:" + "b" * 64,
        environment="dev",
        artifact_registry_ref="local-test",
    )

    assert request.cache_root == physical_parent / "cache"


def test_materialize_revalidates_binding_set_fingerprint(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))
    remote_binding = (
        tmp_path
        / "registry"
        / "s3"
        / "dpone-artifacts"
        / "airflow"
        / "deployments"
        / "dev"
        / _digest_dir(deployment_id)
        / "binding-set.json"
    )
    remote_binding.write_text('{"schema":"dpone.binding-set.v1","environment":"other"}\n', encoding="utf-8")

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(tmp_path / "target-cache", release_id, deployment_id)
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH"


def test_materialize_never_overwrites_conflicting_local_release(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))
    local_release = target_cache / "releases" / _digest_dir(release_id)
    local_release.mkdir(parents=True)
    (local_release / "release-set.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(target_cache, release_id, deployment_id)
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_IMMUTABILITY_CONFLICT"
    assert (local_release / "release-set.json").read_text(encoding="utf-8") == "{}\n"
    assert not (target_cache / "deployments" / "dev" / _digest_dir(deployment_id)).exists()


def test_materialize_does_not_replace_existing_empty_release_directory(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))
    local_release = target_cache / "releases" / _digest_dir(release_id)
    local_release.mkdir(parents=True)

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(target_cache, release_id, deployment_id)
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_IMMUTABILITY_CONFLICT"
    assert list(local_release.iterdir()) == []
    assert not (target_cache / "deployments" / "dev" / _digest_dir(deployment_id)).exists()


def test_materialize_failure_records_release_installed_before_deployment_conflict(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))
    local_deployment = target_cache / "deployments" / "dev" / _digest_dir(deployment_id)
    local_deployment.mkdir(parents=True)
    (local_deployment / "deployment.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(target_cache, release_id, deployment_id)
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_IMMUTABILITY_CONFLICT"
    assert exc.value.details["downloaded_objects"] == 8
    assert exc.value.details["downloaded_bytes"] > 0
    assert exc.value.details["local_release_state"] == "created"
    assert exc.value.details["local_deployment_state"] == "not_installed"
    assert (target_cache / "releases" / _digest_dir(release_id) / "release-set.json").is_file()
    assert (local_deployment / "deployment.json").read_text(encoding="utf-8") == "{}\n"


def test_materialize_production_attestation_policy_fails_before_local_install(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        environment="prod",
        attestation_policy="required_for_prod",
    )
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(
        _publish_request(source_cache, release_id, deployment_id, environment="prod")
    )
    target_cache = tmp_path / "target-cache"

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(registry=registry).materialize(
            _materialize_request(target_cache, release_id, deployment_id, environment="prod")
        )

    assert exc.value.code == "DPONE_ARTIFACT_ATTESTATION_REQUIRED"
    assert not (target_cache / "releases" / _digest_dir(release_id)).exists()
    assert not (target_cache / "deployments" / "prod" / _digest_dir(deployment_id)).exists()
    assert not (target_cache / "current").exists()


def test_materialize_redacts_invalid_attestation_verifier_failure(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        environment="prod",
        attestation_policy="required_for_prod",
    )
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(
        _publish_request(source_cache, release_id, deployment_id, environment="prod")
    )
    target_cache = tmp_path / "target-cache"

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(
            registry=registry,
            attestation_verifier=_FailingAttestationVerifier(),
        ).materialize(_materialize_request(target_cache, release_id, deployment_id, environment="prod"))

    assert exc.value.code == "DPONE_ARTIFACT_ATTESTATION_REQUIRED"
    assert "private-verifier-payload" not in str(exc.value)
    assert not (target_cache / "releases" / _digest_dir(release_id)).exists()


def test_materialize_rejects_multiple_attestation_authorities_before_cache_io(
    tmp_path: Path,
) -> None:
    target_cache = tmp_path / "target-cache"

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(
            registry=_registry(tmp_path),
            attestation_verifier=_FailingAttestationVerifier(),
            deployment_attestation_verifier=_DeploymentAttestationVerifier(),
        ).materialize(
            _materialize_request(
                target_cache,
                "sha256:" + "1" * 64,
                "sha256:" + "2" * 64,
                environment="prod",
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_ATTESTATION_AUTHORITY_CONFLICT"
    assert not target_cache.exists()


def test_materialize_preserves_stable_attestation_rejection_code(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        environment="prod",
        attestation_policy="required_for_prod",
    )
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(
        _publish_request(source_cache, release_id, deployment_id, environment="prod")
    )

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(
            registry=registry,
            attestation_verifier=_RejectedAttestationVerifier(),
        ).materialize(
            _materialize_request(
                tmp_path / "target-cache",
                release_id,
                deployment_id,
                environment="prod",
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_ATTESTATION_SOURCE_NOT_ALLOWED"


@pytest.mark.parametrize(
    "code",
    [
        "DPONE_ARTIFACT_ATTESTATION_NOT_FOUND",
        "DPONE_ARTIFACT_ATTESTATION_INVALID",
    ],
)
def test_materialize_preserves_stable_attestation_registry_code(
    tmp_path: Path,
    code: str,
) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(
        source_cache,
        environment="prod",
        attestation_policy="required_for_prod",
    )
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(
        _publish_request(source_cache, release_id, deployment_id, environment="prod")
    )

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactMaterializer(
            registry=registry,
            attestation_verifier=_RegistryFailingAttestationVerifier(code),
        ).materialize(
            _materialize_request(
                tmp_path / "target-cache",
                release_id,
                deployment_id,
                environment="prod",
            )
        )

    assert exc.value.code == code


def test_materialize_ignores_unknown_remote_objects(tmp_path: Path) -> None:
    source_cache = tmp_path / "source-cache"
    release_id, deployment_id = _write_projection(source_cache)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(registry=registry).publish(_publish_request(source_cache, release_id, deployment_id))
    extra = tmp_path / "extra"
    extra.write_text("must not be fetched\n", encoding="utf-8")
    registry.create_file(PurePosixPath("releases", _digest_dir(release_id), "unknown.json"), extra)

    report = AirflowArtifactMaterializer(registry=registry).materialize(
        _materialize_request(tmp_path / "target-cache", release_id, deployment_id)
    )

    assert report.downloaded_objects == 8
    assert not (tmp_path / "target-cache" / "releases" / _digest_dir(release_id) / "unknown.json").exists()


def test_local_registry_cli_publishes_then_materializes_without_activation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_cache = tmp_path / "source-cache"
    target_cache = tmp_path / "target-cache"
    registry_root = tmp_path / "registry"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    common = [
        "--release-id",
        release_id,
        "--deployment-id",
        deployment_id,
        "--environment",
        "dev",
        "--artifact-registry-ref",
        "local-test",
        "--registry-uri",
        "s3://dpone-artifacts/airflow",
        "--local-registry-root",
        str(registry_root),
        "--format",
        "json",
    ]

    publish_code, publish_stdout, publish_stderr = _run_cli(
        [
            "airflow",
            "publish",
            "--cache-root",
            str(source_cache),
            "--publication-mode",
            "exact",
            *common,
        ],
        capsys,
    )
    materialize_code, materialize_stdout, materialize_stderr = _run_cli(
        ["airflow", "cache-materialize", "--cache-root", str(target_cache), *common],
        capsys,
    )

    assert publish_code == 0
    assert publish_stderr == ""
    publish_payload = json.loads(publish_stdout)
    assert publish_payload["status"] == "published"
    assert materialize_code == 0, materialize_stdout
    assert materialize_stderr == ""
    materialize_payload = json.loads(materialize_stdout)
    assert materialize_payload["status"] == "materialized"
    assert not (target_cache / "current").exists()

    jsonschema = pytest.importorskip("jsonschema")
    root = Path(__file__).resolve().parents[1] / "docs" / "schemas" / "gitops"
    jsonschema.validate(
        publish_payload,
        json.loads((root / "airflow-artifact-publish-v2.schema.json").read_text(encoding="utf-8")),
    )
    jsonschema.validate(
        materialize_payload,
        json.loads((root / "airflow-cache-materialize.schema.json").read_text(encoding="utf-8")),
    )


def test_artifact_delivery_cli_rejects_multiple_storage_modes_before_side_effects(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / "cache"

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "publish",
            "--cache-root",
            str(cache_root),
            "--release-id",
            "sha256:" + "a" * 64,
            "--deployment-id",
            "sha256:" + "b" * 64,
            "--environment",
            "dev",
            "--artifact-registry-ref",
            "local-test",
            "--registry-uri",
            "s3://bucket/root",
            "--local-registry-root",
            str(tmp_path / "registry"),
            "--identity-mode",
            "workload_identity",
        ],
        capsys,
    )

    assert code == 2
    assert stdout == ""
    assert "not allowed with argument" in stderr
    assert not cache_root.exists()


def test_artifact_delivery_cli_rejects_inline_params_credentials(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "publish",
            "--release-id",
            "sha256:" + "a" * 64,
            "--deployment-id",
            "sha256:" + "b" * 64,
            "--environment",
            "prod",
            "--artifact-registry-ref",
            "prod-artifacts",
            "--registry-uri",
            "s3://dpone-artifacts/airflow",
            "--connection-type",
            "params",
            "--connection-id",
            '{"password":"do-not-print"}',
        ],
        capsys,
    )

    assert code == 2
    assert stdout == ""
    assert "do-not-print" not in stderr


def test_artifact_delivery_help_exposes_only_logical_credential_references(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(["airflow", "publish", "--help"], capsys)

    assert code == 0
    assert stderr == ""
    assert "--connection-id" in stdout
    assert "endpoint-bound registry authority" in stdout
    assert "params" not in stdout
    assert "--vault-path" not in stdout
    assert "--vault-mount-point" not in stdout


@pytest.mark.parametrize(
    ("connection_type", "connection_id"),
    [
        ("airflow", '{"password":"do-not-print"}'),
        ("vault", "secret/path?token=do-not-print"),
        ("env", "x" * 129),
    ],
)
def test_artifact_publish_rejects_secret_shaped_connection_ids_without_echo(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    connection_type: str,
    connection_id: str,
) -> None:
    cache_root = tmp_path / "cache"
    release_id, deployment_id = _write_projection(cache_root)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "publish",
            "--cache-root",
            str(cache_root),
            "--release-id",
            release_id,
            "--deployment-id",
            deployment_id,
            "--environment",
            "dev",
            "--artifact-registry-ref",
            "local-test",
            "--registry-uri",
            "s3://bucket/root",
            "--connection-type",
            connection_type,
            "--connection-id",
            connection_id,
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    assert "do-not-print" not in stdout
    assert connection_id not in stdout
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID"


@pytest.mark.parametrize(
    "connection_id",
    [
        "".join(("sk_", "live_", "a" * 24)),
        "".join(("sk-", "proj-", "a" * 24)),
        "".join(("gh", "p_", "a" * 28)),
        "".join(("xo", "xb-", "123456789012-", "abcdefghijklmnop")),
    ],
    ids=["stripe-like", "openai-like", "github-like", "slack-like"],
)
def test_artifact_publish_rejects_token_shaped_logical_connection_ids_without_echo(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    connection_id: str,
) -> None:
    cache_root = tmp_path / "cache"
    release_id, deployment_id = _write_projection(cache_root)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "publish",
            "--cache-root",
            str(cache_root),
            "--release-id",
            release_id,
            "--deployment-id",
            deployment_id,
            "--environment",
            "dev",
            "--artifact-registry-ref",
            "local-test",
            "--registry-uri",
            "s3://bucket/root",
            "--connection-type",
            "vault",
            "--connection-id",
            connection_id,
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    assert connection_id not in stdout
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID"


def test_azure_workload_identity_builds_account_scoped_client(monkeypatch: pytest.MonkeyPatch) -> None:
    import dpone.readiness.airflow_artifact_delivery as delivery

    credential = object()
    captured: dict[str, object] = {}

    class FakeWorkloadIdentityCredential:
        def __new__(cls):
            return credential

    class FakeAzureBlobObjectStorageClient:
        def __init__(self, *, account_url: str, credential: object) -> None:
            captured.update(account_url=account_url, credential=credential)

    azure = types.ModuleType("azure")
    identity = types.ModuleType("azure.identity")
    identity.WorkloadIdentityCredential = FakeWorkloadIdentityCredential
    azure.identity = identity
    monkeypatch.setitem(sys.modules, "azure", azure)
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    monkeypatch.setattr(delivery, "AzureBlobObjectStorageClient", FakeAzureBlobObjectStorageClient)

    registry = ArtifactRegistryOptions(
        registry_uri="azure://platformaccount/dpone-artifacts/airflow",
        identity_mode="workload_identity",
    ).build()

    assert registry.root.account == "platformaccount"
    assert captured == {
        "account_url": "https://platformaccount.blob.core.windows.net",
        "credential": credential,
    }


def test_azure_workload_identity_rejects_accountless_compatibility_uri() -> None:
    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        ArtifactRegistryOptions(
            registry_uri="az://dpone-artifacts/airflow",
            identity_mode="workload_identity",
        ).build()

    assert exc.value.code == "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID"


@pytest.mark.parametrize(
    "artifacts",
    [
        {"dag_specs": [], "workload_packs": [], "canonical_schemas": []},
        {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [
                {
                    "id": "manifest",
                    "path": "schemas/manifest.schema.json",
                    "sha256": "sha256:" + "a" * 64,
                }
            ],
        },
    ],
)
def test_release_inventory_rejects_empty_or_schema_only_release(artifacts: dict[str, object]) -> None:
    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        declared_release_artifacts({"artifacts": artifacts})

    assert exc.value.code == "DPONE_RELEASE_ARTIFACTS_INVALID"


def test_cache_error_mapping_never_exposes_private_staging_path() -> None:
    private_path = "/private/tmp/dpone-materialize-secret/staged.json"

    mapped = from_cache_error(
        DeploymentCacheError(
            "DPONE_DEPLOYMENT_CACHE_INVALID",
            "staged projection is invalid",
            path=private_path,
            details={"artifact": "airflow-index.json"},
        )
    )

    assert private_path not in str(mapped)
    assert private_path not in json.dumps(mapped.details)
    assert mapped.details == {"artifact": "airflow-index.json"}


def test_artifact_delivery_invalid_identity_failure_matches_public_json_schema(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "publish",
            "--release-id",
            "not-a-digest",
            "--deployment-id",
            "also-invalid",
            "--environment",
            "dev",
            "--artifact-registry-ref",
            "local-test",
            "--registry-uri",
            "s3://dpone-artifacts/airflow",
            "--local-registry-root",
            str(tmp_path / "registry"),
            "--publication-mode",
            "exact",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["release_id"] is None
    assert payload["deployment_id"] is None
    assert payload["errors"][0]["code"] == "DPONE_RELEASE_ID_INVALID"
    assert payload["errors"][0]["fixes"][0]["command"] == "dpone airflow publish --help"
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "docs"
            / "schemas"
            / "gitops"
            / "airflow-artifact-publish-v2.schema.json"
        ).read_text(encoding="utf-8")
    )
    jsonschema.validate(payload, schema)


def test_artifact_delivery_text_failure_is_actionable(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "publish",
            "--release-id",
            "invalid",
            "--deployment-id",
            "sha256:" + "b" * 64,
            "--environment",
            "dev",
            "--artifact-registry-ref",
            "local-test",
            "--registry-uri",
            "s3://dpone-artifacts/airflow",
            "--local-registry-root",
            "/tmp/dpone-artifact-proof",
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    assert "DPONE_RELEASE_ID_INVALID" in stdout
    assert "release_id must be a canonical sha256 digest" in stdout
    assert "fix manual: dpone airflow publish --help" in stdout


def test_artifact_delivery_redacts_invalid_secret_like_registry_ref(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-materialize",
            "--release-id",
            "sha256:" + "a" * 64,
            "--deployment-id",
            "sha256:" + "b" * 64,
            "--environment",
            "dev",
            "--artifact-registry-ref",
            "password=do-not-print",
            "--registry-uri",
            "s3://dpone-artifacts/airflow",
            "--local-registry-root",
            str(tmp_path / "registry"),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    assert "do-not-print" not in stdout
    payload = json.loads(stdout)
    assert payload["artifact_registry_ref"] is None
    assert payload["errors"][0]["code"] == "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID"


def _registry(tmp_path: Path) -> ObjectStorageArtifactRegistry:
    return ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(tmp_path / "registry"),
        root=ObjectStorageUri.parse("s3://dpone-artifacts/airflow"),
    )


def _publish_request(
    cache_root: Path,
    release_id: str,
    deployment_id: str,
    *,
    environment: str = "dev",
    attestation_bundle_path: Path | None = None,
    max_object_bytes: int = 64 * 1024 * 1024,
    publication_mode: str = "compatible",
) -> PublishRequest:
    return PublishRequest(
        cache_root=cache_root,
        release_id=release_id,
        deployment_id=deployment_id,
        environment=environment,
        artifact_registry_ref="local-test",
        attestation_bundle_path=attestation_bundle_path,
        max_object_bytes=max_object_bytes,
        publication_mode=publication_mode,
    )


def _materialize_request(
    cache_root: Path,
    release_id: str,
    deployment_id: str,
    *,
    environment: str = "dev",
) -> MaterializeRequest:
    return MaterializeRequest(
        cache_root=cache_root,
        release_id=release_id,
        deployment_id=deployment_id,
        environment=environment,
        artifact_registry_ref="local-test",
    )


def _write_projection(
    cache_root: Path,
    *,
    environment: str = "dev",
    attestation_policy: str = "optional",
    trust_tier: str | None = None,
    release_schema: str = "dpone.release-set.v1",
) -> tuple[str, str]:
    dag_bytes = b'{"dag_id":"orders_daily"}\n'
    dag_sha = _sha256(dag_bytes)
    runtime_payload_bytes = b"test dbt project bundle\n"
    runtime_payload_sha = _sha256(runtime_payload_bytes)
    route_certification = {
        "variant_id": "mssql_clickhouse_snapshot",
        "route_id": "mssql_to_clickhouse",
        "transport": "typed_binary_streaming",
        "schema_evolution": "strict",
        "airflow_runtime_mode": "kubernetes_executor",
        "snapshot_id": "sha256:" + "1" * 64,
        "support": "supported",
        "certification_level": "production-certified",
        "evidence_status": "PASS",
        "evidence_refs": ["sha256:" + "2" * 64],
        "evidence_reason_codes": [],
    }
    source_snapshot = "sha256:" + "3" * 64
    selections = ["sha256:" + "4" * 64]
    release = {
        "schema": release_schema,
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
            **(
                {
                    "runtime_payloads": [
                        {
                            "id": "dbt_project",
                            "kind": "dbt_project_bundle",
                            "path": "runtime/dbt/project.tar.gz",
                            "sha256": runtime_payload_sha,
                            "bytes": len(runtime_payload_bytes),
                            "media_type": "application/vnd.dpone.dbt-project-bundle+gzip",
                        }
                    ]
                }
                if release_schema == "dpone.release-set.v2"
                else {}
            ),
        },
        **(
            {
                "producer": {
                    "dpone_version": __version__,
                    "wire_contract": DBT_RELEASE_WIRE_CONTRACT,
                },
                "selection_authority": "dbt_cli",
                "selection_fingerprint": dbt_selection_fingerprint(
                    source_snapshot_sha256=source_snapshot,
                    selection_fingerprints=tuple(selections),
                    route_certifications=(route_certification,),
                ),
                "provenance": {
                    "source": "dpone dbt compile",
                    "source_snapshot_sha256": source_snapshot,
                    "selection_fingerprints": selections,
                    "route_certifications": [route_certification],
                },
            }
            if release_schema == "dpone.release-set.v2"
            else {}
        ),
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = cache_root / "releases" / _digest_dir(release_id)
    (release_dir / "dags").mkdir(parents=True)
    (release_dir / "dags" / "orders_daily.dag-spec.json").write_bytes(dag_bytes)
    if release_schema == "dpone.release-set.v2":
        (release_dir / "runtime" / "dbt").mkdir(parents=True)
        (release_dir / "runtime" / "dbt" / "project.tar.gz").write_bytes(runtime_payload_bytes)
    _write_json(release_dir / "release-set.json", release)

    runtime_delivery = {
        "mode": "init_fetch",
        "artifact_registry_ref": "local-test",
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
        },
        "source": {"artifact_registry_ref": "local-test"},
        "verify": {"checksums": "required", "attestations": attestation_policy},
    }
    binding_set = {"schema": "dpone.binding-set.v1", "environment": environment, "bindings": {}}
    binding_set_ref = canonical_fingerprint(binding_set)
    connection_registry_ref = "sha256:" + "c" * 64
    credential_runtime_ref = "sha256:" + "e" * 64
    deployment = {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "",
        "deployment_type": "environment",
        "runnable": True,
        "environment": environment,
        "release_ref": release_id,
        "binding_set_ref": binding_set_ref,
        "connection_registry_ref": connection_registry_ref,
        "credential_runtime_ref": credential_runtime_ref,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": runtime_delivery,
    }
    if trust_tier is not None:
        deployment["trust_tier"] = trust_tier
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
        **(
            {
                "runtime_payloads": [
                    {
                        "id": "dbt_project",
                        "artifact_ref": (f"cache://releases/{_digest_dir(release_id)}/runtime/dbt/project.tar.gz"),
                        "sha256": runtime_payload_sha,
                        "bytes": len(runtime_payload_bytes),
                    }
                ]
            }
            if release_schema == "dpone.release-set.v2"
            else {}
        ),
        "binding_set_ref": binding_set_ref,
        "connection_registry_ref": connection_registry_ref,
        "credential_runtime_ref": credential_runtime_ref,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": runtime_delivery,
    }
    if trust_tier is not None:
        index["trust_tier"] = trust_tier
    deployment_dir = cache_root / "deployments" / environment / _digest_dir(deployment_id)
    deployment_dir.mkdir(parents=True)
    _write_json(deployment_dir / "deployment.json", deployment)
    _write_json(deployment_dir / "airflow-index.json", index)
    _write_json(
        deployment_dir / "binding-set.json",
        binding_set,
    )
    (deployment_dir / "connection-registry.ref").write_text(connection_registry_ref + "\n", encoding="utf-8")
    (deployment_dir / "credential-runtime.ref").write_text(credential_runtime_ref + "\n", encoding="utf-8")
    (deployment_dir / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    return release_id, deployment_id


def _key(*parts: str):
    from pathlib import PurePosixPath

    return PurePosixPath(*(part.replace(":", "-", 1) for part in parts))


def _digest_dir(digest: str) -> str:
    return digest.replace(":", "-", 1)


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _attestation_key(cache_root: Path, release_id: str) -> PurePosixPath:
    release_set = cache_root / "releases" / _digest_dir(release_id) / "release-set.json"
    return runtime_attestation_bundle_key(
        release_id=release_id,
        release_set_sha256=_sha256(release_set.read_bytes()),
    )


def _remote_object_path(tmp_path: Path, key: PurePosixPath) -> Path:
    return tmp_path / "registry" / "s3" / "dpone-artifacts" / "airflow" / key


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


class _RecordingRegistry:
    def __init__(self, delegate: ObjectStorageArtifactRegistry) -> None:
        self._delegate = delegate
        self.created_keys: list[PurePosixPath] = []

    @property
    def authority_scope_id(self) -> str:
        return self._delegate.authority_scope_id

    def create_file(self, key: PurePosixPath, source: Path):
        self.created_keys.append(key)
        return self._delegate.create_file(key, source)

    def stat(self, key: PurePosixPath):
        return self._delegate.stat(key)

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        self._delegate.download_file(key, destination, max_bytes=max_bytes)


class _NoIoRegistry:
    def __init__(self) -> None:
        self.calls = 0

    @property
    def authority_scope_id(self) -> str:
        return "sha256:" + "0" * 64

    def create_file(self, key: PurePosixPath, source: Path) -> CreateResult:
        del key, source
        self.calls += 1
        raise AssertionError("registry I/O must not occur")

    def stat(self, key: PurePosixPath) -> ArtifactMetadata:
        del key
        self.calls += 1
        raise AssertionError("registry I/O must not occur")

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        del key, destination, max_bytes
        self.calls += 1
        raise AssertionError("registry I/O must not occur")


class _LegacyRegistry:
    """Historical registry port without the optional exact authority identity."""

    def __init__(self, delegate: ObjectStorageArtifactRegistry) -> None:
        self._delegate = delegate
        self.calls = 0

    @property
    def scope_id(self) -> str:
        """Expose the old valid digest without claiming exact authority."""

        return self._delegate.scope_id

    def create_file(self, key: PurePosixPath, source: Path) -> CreateResult:
        self.calls += 1
        return self._delegate.create_file(key, source)

    def stat(self, key: PurePosixPath) -> ArtifactMetadata:
        self.calls += 1
        return self._delegate.stat(key)

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        self.calls += 1
        self._delegate.download_file(key, destination, max_bytes=max_bytes)


class _MutatingRegistry(_RecordingRegistry):
    def __init__(self, delegate: ObjectStorageArtifactRegistry, *, mutate_path: Path) -> None:
        super().__init__(delegate)
        self._mutate_path = mutate_path
        self._mutated = False

    def create_file(self, key: PurePosixPath, source: Path):
        if not self._mutated:
            self._mutate_path.write_text('{"dag_id":"changed-during-upload"}\n', encoding="utf-8")
            self._mutated = True
        return super().create_file(key, source)


class _NegativeMetadataRegistry:
    def __init__(self) -> None:
        self.download_calls = 0

    def create_file(self, key: PurePosixPath, source: Path) -> CreateResult:
        del key, source
        raise AssertionError("create is not part of materialization")

    def stat(self, key: PurePosixPath) -> ArtifactMetadata:
        return ArtifactMetadata(key=key, size_bytes=-1)

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        del key, destination, max_bytes
        self.download_calls += 1
        raise AssertionError("invalid metadata must block download")


class _FailOnceRegistry(_RecordingRegistry):
    def __init__(self, delegate: ObjectStorageArtifactRegistry, *, fail_on_call: int) -> None:
        super().__init__(delegate)
        self._fail_on_call = fail_on_call
        self._calls = 0

    def create_file(self, key: PurePosixPath, source: Path):
        self._calls += 1
        if self._calls == self._fail_on_call:
            raise ArtifactRegistryUnavailable("simulated bounded outage")
        return super().create_file(key, source)


class _CreateThenTimeoutRegistry(_RecordingRegistry):
    def __init__(self, delegate: ObjectStorageArtifactRegistry) -> None:
        super().__init__(delegate)
        self._failed = False

    def create_file(self, key: PurePosixPath, source: Path):
        result = super().create_file(key, source)
        if not self._failed:
            self._failed = True
            raise ArtifactRegistryUnavailable("simulated timeout after committed create")
        return result


class _CorruptingReadbackRegistry(_RecordingRegistry):
    def __init__(self, delegate: ObjectStorageArtifactRegistry) -> None:
        super().__init__(delegate)
        self._corrupted = False

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        super().download_file(key, destination, max_bytes=max_bytes)
        if not self._corrupted:
            destination.write_bytes(b"corrupt")
            self._corrupted = True


class _CacheRootSwappingRegistry(_RecordingRegistry):
    def __init__(
        self,
        delegate: ObjectStorageArtifactRegistry,
        *,
        cache_root: Path,
        outside: Path,
    ) -> None:
        super().__init__(delegate)
        self._cache_root = cache_root
        self._outside = outside
        self._swapped = False

    def stat(self, key: PurePosixPath):
        if not self._swapped:
            self._cache_root.rename(self._cache_root.parent / "moved-cache")
            self._cache_root.symlink_to(self._outside, target_is_directory=True)
            self._swapped = True
        return super().stat(key)


class _FailingAttestationVerifier:
    def verify(self, _projection) -> None:
        raise RuntimeError("private-verifier-payload")


class _RejectedAttestationVerifier:
    def verify(self, _projection) -> None:
        raise AirflowArtifactAttestationRejected(
            AirflowArtifactAttestationVerification(
                decision="invalid",
                code="DPONE_ARTIFACT_ATTESTATION_SOURCE_NOT_ALLOWED",
                message="artifact source is not allowed by policy",
                attestation_id="sha256:" + "1" * 64,
                policy_fingerprint="sha256:" + "2" * 64,
                public_key_id=None,
                public_key_sha256=None,
                verifier_version="3.0.4",
                verified_at="2026-07-29T10:00:00Z",
            )
        )


class _RegistryFailingAttestationVerifier:
    def __init__(self, code: str) -> None:
        self._code = code

    def verify(self, _projection) -> None:
        raise AirflowArtifactAttestationRegistryError(
            self._code,
            "redacted registry failure",
        )


class _DeploymentAttestationVerifier:
    policy_sha256 = "sha256:" + "3" * 64

    def verify(self, _subject) -> AirflowArtifactAttestationVerification:
        raise AssertionError("authority conflict must fail before verification")


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err
