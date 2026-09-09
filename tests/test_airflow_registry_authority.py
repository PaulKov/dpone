from __future__ import annotations

import types
from pathlib import Path

import pytest

from dpone.adapters.object_storage_artifact_registry import (
    ObjectStorageArtifactRegistry,
    object_storage_registry_authority_scope_id,
    object_storage_registry_scope_id,
)
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.ports.artifact_registry import ArtifactRegistryKeyError
from dpone.readiness.airflow_artifact_delivery import ArtifactRegistryOptions
from dpone.runtime.airflow_artifact_delivery import (
    AirflowArtifactDeliveryError,
    AirflowArtifactPublisher,
)
from dpone.runtime.airflow_artifact_delivery_models import PublishRequest
from dpone.storage.adapters import GCSObjectStorageClient, S3ObjectStorageClient
from dpone.storage.azure_adapter import AzureBlobObjectStorageClient
from dpone.storage.local import LocalObjectStorageClient
from dpone.storage.models import ObjectStorageUri
from tests.support.airflow_artifact_projection import write_exact_test_projection


def test_exact_registry_scope_binds_canonical_endpoint_authority() -> None:
    root = ObjectStorageUri.parse("s3://example-data-bucket/dpone-artifacts/prod/example-workloads")

    yandex = object_storage_registry_authority_scope_id(
        root,
        endpoint_authority="HTTPS://STORAGE.YANDEXCLOUD.NET:443/",
    )
    aws = object_storage_registry_authority_scope_id(
        root,
        endpoint_authority="https://s3.amazonaws.com",
    )
    expected = canonical_fingerprint(
        {
            "schema": "dpone.artifact-registry-scope.v2",
            "kind": "object_storage",
            "provider": root.provider_name,
            "endpoint_authority": "https://storage.yandexcloud.net",
            "account": root.account,
            "bucket": root.bucket,
            "root": root.key,
        }
    )

    assert yandex == expected
    assert yandex != aws


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://user:secret@storage.example.test",
        "https://storage.example.test/../path",
        "https://storage.example.test/path%2Fsecret",
        "https://storage.example.test?token=secret",
        "ftp://storage.example.test",
        "https://storage.example.test#fragment",
    ],
)
def test_exact_registry_scope_rejects_unsafe_endpoint_authority(endpoint: str) -> None:
    root = ObjectStorageUri.parse("s3://example-data-bucket/dpone-artifacts/prod")

    with pytest.raises(ArtifactRegistryKeyError):
        object_storage_registry_authority_scope_id(root, endpoint_authority=endpoint)


def test_registry_uses_endpoint_observed_from_s3_sdk() -> None:
    endpoint = "https://storage.yandexcloud.net"
    sdk_client = types.SimpleNamespace(meta=types.SimpleNamespace(endpoint_url=endpoint))
    root = ObjectStorageUri.parse("s3://example-data-bucket/dpone-artifacts/prod")

    registry = ObjectStorageArtifactRegistry(
        client=S3ObjectStorageClient(client=sdk_client),
        root=root,
    )

    assert registry.scope_id == object_storage_registry_scope_id(root)
    assert registry.authority_scope_id == object_storage_registry_authority_scope_id(
        root,
        endpoint_authority=endpoint,
    )


def test_registry_options_preserve_observed_s3_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.readiness.airflow_artifact_delivery as delivery

    endpoint = "https://storage.yandexcloud.net"
    sdk_client = types.SimpleNamespace(meta=types.SimpleNamespace(endpoint_url=endpoint))
    storage_client = S3ObjectStorageClient(client=sdk_client)
    monkeypatch.setattr(
        delivery.ObjectStorageConnectionResolver,
        "build_client",
        lambda _self, *, ref, uri: storage_client,
    )
    root = ObjectStorageUri.parse("s3://example-data-bucket/dpone-artifacts/prod")

    registry = ArtifactRegistryOptions(
        registry_uri=str(root),
        connection_type="env",
        connection_id="s3_dpone_artifacts_writer",
    ).build()

    assert registry.scope_id == object_storage_registry_scope_id(root)
    assert registry.authority_scope_id == object_storage_registry_authority_scope_id(
        root,
        endpoint_authority=endpoint,
    )


def test_registry_options_name_root_only_identity_as_legacy() -> None:
    options = ArtifactRegistryOptions(
        registry_uri="s3://example-data-bucket/dpone-artifacts/prod",
        connection_type="env",
        connection_id="s3_dpone_artifacts_writer",
    )

    assert options.legacy_scope_id == object_storage_registry_scope_id(ObjectStorageUri.parse(options.registry_uri))
    assert options.scope_id == options.legacy_scope_id


def test_first_party_cloud_adapters_expose_observed_endpoint_authority() -> None:
    gcs_client = types.SimpleNamespace(_connection=types.SimpleNamespace(API_BASE_URL="https://storage.googleapis.com"))
    azure_client = types.SimpleNamespace(url="https://platformaccount.blob.core.windows.net")

    assert GCSObjectStorageClient(client=gcs_client).endpoint_authority == ("https://storage.googleapis.com")
    assert AzureBlobObjectStorageClient(service_client=azure_client).endpoint_authority == (
        "https://platformaccount.blob.core.windows.net"
    )


def test_pinned_azure_sdk_authority_strips_sas_and_preserves_emulator_path() -> None:
    azure_blob = pytest.importorskip("azure.storage.blob")
    service_client = azure_blob.BlobServiceClient

    standard = service_client(account_url="https://platformaccount.blob.core.windows.net", credential=None)
    sas = service_client(
        account_url="https://platformaccount.blob.core.windows.net?sv=2024-01-01&sig=secret",
        credential=None,
    )
    azurite = service_client(
        account_url="http://127.0.0.1:10000/devstoreaccount1",
        credential=None,
    )

    assert AzureBlobObjectStorageClient(service_client=standard).endpoint_authority == (
        "https://platformaccount.blob.core.windows.net/"
    )
    assert AzureBlobObjectStorageClient(service_client=sas).endpoint_authority == (
        "https://platformaccount.blob.core.windows.net/"
    )
    assert AzureBlobObjectStorageClient(service_client=azurite).endpoint_authority == (
        "http://127.0.0.1:10000/devstoreaccount1/"
    )


def test_azure_emulator_path_is_part_of_exact_registry_authority() -> None:
    root = ObjectStorageUri.parse("azure://devstoreaccount1/dpone-artifacts/prod")

    first = object_storage_registry_authority_scope_id(
        root,
        endpoint_authority="http://127.0.0.1:10000/devstoreaccount1/",
    )
    second = object_storage_registry_authority_scope_id(
        root,
        endpoint_authority="http://127.0.0.1:10000/another-account/",
    )

    assert first != second


def test_compatible_scope_does_not_evaluate_invalid_exact_authority(tmp_path: Path) -> None:
    root = ObjectStorageUri.parse("s3://dpone-artifacts/airflow")
    registry = ObjectStorageArtifactRegistry(
        client=_InvalidAuthorityLocalClient(tmp_path / "objects"),
        root=root,
    )

    assert registry.scope_id == object_storage_registry_scope_id(root)
    with pytest.raises(ArtifactRegistryKeyError):
        _ = registry.authority_scope_id


def test_exact_publication_classifies_invalid_sdk_authority_before_write(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    release_id, deployment_id = write_exact_test_projection(cache_root)
    object_root = tmp_path / "objects"
    registry = ObjectStorageArtifactRegistry(
        client=_InvalidAuthorityLocalClient(object_root),
        root=ObjectStorageUri.parse("s3://dpone-artifacts/airflow"),
    )

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(
            PublishRequest(
                cache_root=cache_root,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="dev",
                artifact_registry_ref="local-test",
                publication_mode="exact",
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID"
    assert not object_root.exists()


def test_exact_s3_publication_rejects_endpoint_drift_before_write(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    release_id, deployment_id = write_exact_test_projection(cache_root)
    root = ObjectStorageUri.parse("s3://example-data-bucket/dpone-artifacts/prod")
    sdk_client = _NoWriteS3Client("https://storage.yandexcloud.net")
    registry = ObjectStorageArtifactRegistry(
        client=S3ObjectStorageClient(client=sdk_client),
        root=root,
    )

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(
            PublishRequest(
                cache_root=cache_root,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="dev",
                artifact_registry_ref="local-test",
                registry_scope_id=object_storage_registry_authority_scope_id(
                    root,
                    endpoint_authority="https://s3.amazonaws.com",
                ),
                publication_mode="exact",
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_SCOPE_MISMATCH"
    assert sdk_client.put_calls == 0


def test_local_registry_scope_binds_actual_filesystem_root(tmp_path: Path) -> None:
    first = ArtifactRegistryOptions(
        registry_uri="s3://dpone-artifacts/airflow",
        local_registry_root=str(tmp_path / "first"),
    ).build()
    second = ArtifactRegistryOptions(
        registry_uri="s3://dpone-artifacts/airflow",
        local_registry_root=str(tmp_path / "second"),
    ).build()

    assert first.scope_id == second.scope_id
    assert first.authority_scope_id != second.authority_scope_id


def test_exact_publication_rejects_legacy_root_scope_before_write(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    release_id, deployment_id = write_exact_test_projection(cache_root)
    object_root = tmp_path / "objects"
    root = ObjectStorageUri.parse("s3://dpone-artifacts/airflow")
    registry = ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(object_root),
        root=root,
    )

    with pytest.raises(AirflowArtifactDeliveryError) as exc:
        AirflowArtifactPublisher(registry=registry).publish(
            PublishRequest(
                cache_root=cache_root,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="dev",
                artifact_registry_ref="local-test",
                registry_scope_id=object_storage_registry_scope_id(root),
                publication_mode="exact",
            )
        )

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_SCOPE_MISMATCH"
    assert not object_root.exists()


class _NoWriteS3Client:
    def __init__(self, endpoint_url: str) -> None:
        self.meta = types.SimpleNamespace(endpoint_url=endpoint_url)
        self.put_calls = 0

    def put_object(self, **_kwargs: object) -> None:
        self.put_calls += 1
        raise AssertionError("registry write must not happen before scope validation")


class _InvalidAuthorityLocalClient(LocalObjectStorageClient):
    @property
    def endpoint_authority(self) -> str:
        return "https://storage.example.test?sig=secret"
