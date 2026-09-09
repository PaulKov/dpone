from __future__ import annotations

import json
from pathlib import Path

from dpone.staging.object_storage import ObjectStorageStagingPlan, ObjectStorageStagingService
from dpone.storage import (
    AzureBlobObjectStorageClient,
    GCSObjectStorageClient,
    LocalObjectStorageClient,
    ObjectStorageProvider,
    ObjectStorageUri,
    S3ObjectStorageClient,
)


def test_object_storage_uri_parses_s3_gcs_and_azure_forms() -> None:
    s3 = ObjectStorageUri.parse("s3://bucket-a/staging/orders/file.tsv")
    gcs = ObjectStorageUri.parse("gs://bucket-b/staging/orders/file.parquet")
    azure_short = ObjectStorageUri.parse("az://container-a/staging/orders/file.tsv")
    azure_account = ObjectStorageUri.parse("azure://account-a/container-b/staging/orders/file.tsv")

    assert s3.provider == ObjectStorageProvider.S3
    assert s3.bucket == "bucket-a"
    assert s3.key == "staging/orders/file.tsv"
    assert str(s3) == "s3://bucket-a/staging/orders/file.tsv"
    assert gcs.provider == ObjectStorageProvider.GCS
    assert gcs.bucket == "bucket-b"
    assert azure_short.provider == ObjectStorageProvider.AZURE_BLOB
    assert azure_short.bucket == "container-a"
    assert azure_short.key == "staging/orders/file.tsv"
    assert azure_account.account == "account-a"
    assert azure_account.bucket == "container-b"
    assert str(azure_account) == "azure://account-a/container-b/staging/orders/file.tsv"


def test_local_object_storage_client_uploads_downloads_and_cleans_prefix(tmp_path: Path) -> None:
    source = tmp_path / "orders.tsv"
    source.write_text("id\tamount\n1\t10\n", encoding="utf-8")
    client = LocalObjectStorageClient(root_dir=tmp_path / "object-store")

    result = client.put_file(source, ObjectStorageUri.parse("s3://dpone-stage/runs/01/orders.tsv"))

    assert result.uri == "s3://dpone-stage/runs/01/orders.tsv"
    assert result.size_bytes == source.stat().st_size
    assert len(result.sha256) == 64
    assert client.exists(ObjectStorageUri.parse(result.uri))

    downloaded = tmp_path / "downloaded.tsv"
    client.get_file(ObjectStorageUri.parse(result.uri), downloaded)
    assert downloaded.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")

    deleted = client.delete_prefix(ObjectStorageUri.parse("s3://dpone-stage/runs/01/"))
    assert deleted == 1
    assert not client.exists(ObjectStorageUri.parse(result.uri))


def test_object_storage_staging_service_writes_manifest_with_checksums(tmp_path: Path) -> None:
    file_a = tmp_path / "part-000.tsv"
    file_b = tmp_path / "part-001.tsv"
    file_a.write_text("id\tamount\n1\t10\n", encoding="utf-8")
    file_b.write_text("id\tamount\n2\t20\n", encoding="utf-8")
    service = ObjectStorageStagingService(client=LocalObjectStorageClient(root_dir=tmp_path / "store"))
    plan = ObjectStorageStagingPlan(
        base_uri="gs://dpone-stage/orders",
        run_id="01JZDPONEOBJECTSTAGING000",
        dataset="landing",
        table="orders",
        file_format="tsv",
        compression="none",
        cleanup_policy="delete_on_success",
    )

    manifest = service.stage_files(plan, [file_a, file_b])

    assert manifest.provider == "gcs"
    assert manifest.base_uri == "gs://dpone-stage/orders"
    assert manifest.object_count == 2
    assert manifest.total_size_bytes == file_a.stat().st_size + file_b.stat().st_size
    assert [item.file_name for item in manifest.objects] == ["part-000.tsv", "part-001.tsv"]
    assert {len(item.sha256) for item in manifest.objects} == {64}

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest.to_dict(), indent=2), encoding="utf-8")
    restored = manifest.from_dict(json.loads(manifest_path.read_text(encoding="utf-8")))
    assert restored.to_dict() == manifest.to_dict()

    deleted = service.cleanup(manifest)
    assert deleted == 2


def test_object_storage_staging_service_accepts_canonical_cleanup_policy(tmp_path: Path) -> None:
    file_a = tmp_path / "part-000.tsv"
    file_a.write_text("id\n1\n", encoding="utf-8")
    service = ObjectStorageStagingService(client=LocalObjectStorageClient(root_dir=tmp_path / "store"))
    manifest = service.stage_files(
        ObjectStorageStagingPlan(
            base_uri="s3://dpone-stage/orders",
            run_id="run-1",
            dataset="landing",
            table="orders",
            file_format="tsv",
            cleanup_policy="eager",
        ),
        [file_a],
    )

    assert service.cleanup(manifest) == 1


def test_cloud_storage_clients_are_lazy_and_accept_fake_clients(tmp_path: Path) -> None:
    source = tmp_path / "payload.txt"
    source.write_text("hello", encoding="utf-8")

    s3 = S3ObjectStorageClient(client=_FakeS3Client())
    gcs = GCSObjectStorageClient(client=_FakeGCSClient())
    azure = AzureBlobObjectStorageClient(service_client=_FakeAzureServiceClient())

    assert s3.put_file(source, ObjectStorageUri.parse("s3://bucket/key.txt")).uri == "s3://bucket/key.txt"
    assert gcs.put_file(source, ObjectStorageUri.parse("gs://bucket/key.txt")).uri == "gs://bucket/key.txt"
    assert azure.put_file(source, ObjectStorageUri.parse("az://container/key.txt")).uri == "az://container/key.txt"


class _FakeS3Client:
    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        assert Path(filename).is_file()
        assert bucket
        assert key


class _FakeGCSClient:
    def bucket(self, bucket_name: str):
        assert bucket_name
        return _FakeGCSBucket()


class _FakeGCSBucket:
    def blob(self, key: str):
        assert key
        return _FakeGCSBlob()


class _FakeGCSBlob:
    def upload_from_filename(self, filename: str) -> None:
        assert Path(filename).is_file()


class _FakeAzureServiceClient:
    def get_blob_client(self, *, container: str, blob: str):
        assert container
        assert blob
        return _FakeAzureBlobClient()


class _FakeAzureBlobClient:
    def upload_blob(self, data, *, overwrite: bool) -> None:
        assert overwrite is True
        data.read()
