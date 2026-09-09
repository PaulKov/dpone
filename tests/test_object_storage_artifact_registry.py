from __future__ import annotations

import sys
import types
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path, PurePosixPath
from threading import Event
from typing import Any

import pytest

from dpone.adapters.object_storage_artifact_registry import (
    ObjectStorageArtifactRegistry,
    parse_artifact_registry_root,
)
from dpone.ports.artifact_registry import ArtifactRegistryKeyError
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.object_storage_access import ObjectStorageConnectionRef
from dpone.runtime.object_storage_connection_resolver import ObjectStorageConnectionResolver
from dpone.storage.adapters import AzureBlobObjectStorageClient, GCSObjectStorageClient, S3ObjectStorageClient
from dpone.storage.local import LocalObjectStorageClient
from dpone.storage.models import ObjectStorageReadLimitExceeded, ObjectStorageUri, ObjectStorageWriteConflict


def test_object_storage_registry_creates_once_and_reports_existing_object(tmp_path: Path) -> None:
    source = tmp_path / "release-set.json"
    source.write_text('{"schema":"dpone.release-set.v1"}\n', encoding="utf-8")
    registry = ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(tmp_path / "objects"),
        root=ObjectStorageUri.parse("s3://dpone-artifacts/airflow"),
    )
    key = PurePosixPath("releases/sha256-abc/release-set.json")

    created = registry.create_file(key, source)
    existing = registry.create_file(key, source)

    assert created.created is True
    assert existing.created is False
    assert existing.metadata.size_bytes == source.stat().st_size
    destination = tmp_path / "downloaded.json"
    registry.download_file(key, destination, max_bytes=source.stat().st_size)
    assert destination.read_bytes() == source.read_bytes()


def test_local_registry_conditional_create_is_atomic_under_concurrency(tmp_path: Path) -> None:
    source = tmp_path / "release-set.json"
    source.write_text('{"schema":"dpone.release-set.v1"}\n', encoding="utf-8")
    registry = ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(tmp_path / "objects"),
        root=ObjectStorageUri.parse("s3://dpone-artifacts/airflow"),
    )
    key = PurePosixPath("releases/sha256-abc/release-set.json")

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(lambda _: registry.create_file(key, source), range(2)))

    assert sorted(result.created for result in results) == [False, True]


def test_local_conditional_create_does_not_expose_partial_final_object(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.storage.local_fs as local_fs

    source = tmp_path / "large-object"
    source.write_bytes(b"complete-object")
    client = LocalObjectStorageClient(tmp_path / "objects")
    uri = ObjectStorageUri.parse("s3://bucket/release/object")
    link_started = Event()
    allow_link = Event()
    real_link = local_fs.os.link

    def delayed_link(
        source_path: str,
        target_path: str,
        *,
        src_dir_fd: int,
        dst_dir_fd: int,
        follow_symlinks: bool,
    ) -> None:
        link_started.set()
        assert allow_link.wait(timeout=5)
        real_link(
            source_path,
            target_path,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(local_fs.os, "link", delayed_link)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.put_file_if_absent, source, uri)
        assert link_started.wait(timeout=5)
        with pytest.raises(FileNotFoundError):
            client.stat(uri)
        allow_link.set()
        created = future.result(timeout=5)

    assert created.size_bytes == len(b"complete-object")
    assert client.stat(uri).size_bytes == len(b"complete-object")


def test_local_conditional_create_stays_anchored_when_registry_root_is_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.storage.local_fs as local_fs

    source = tmp_path / "source"
    source.write_bytes(b"anchored-object")
    root = tmp_path / "objects"
    detached_root = tmp_path / "detached-objects"
    outside = tmp_path / "outside"
    outside.mkdir()
    client = LocalObjectStorageClient(root)
    uri = ObjectStorageUri.parse("s3://bucket/release/object")
    install_started = Event()
    allow_install = Event()
    real_link = local_fs.os.link

    def delayed_link(
        source_name: str,
        target_name: str,
        *,
        src_dir_fd: int,
        dst_dir_fd: int,
        follow_symlinks: bool,
    ) -> None:
        install_started.set()
        assert allow_install.wait(timeout=5)
        real_link(
            source_name,
            target_name,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(local_fs.os, "link", delayed_link)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.put_file_if_absent, source, uri)
        assert install_started.wait(timeout=5)
        root.rename(detached_root)
        root.symlink_to(outside, target_is_directory=True)
        allow_install.set()
        created = future.result(timeout=5)

    assert created.size_bytes == len(b"anchored-object")
    assert not (outside / "s3" / "bucket" / "release" / "object").exists()
    assert (detached_root / "s3" / "bucket" / "release" / "object").read_bytes() == b"anchored-object"


def test_local_registry_rejects_root_replacement_between_object_operations(tmp_path: Path) -> None:
    first_source = tmp_path / "release-object"
    first_source.write_bytes(b"release")
    marker_source = tmp_path / "completion-marker"
    marker_source.write_bytes(b"complete")
    root = tmp_path / "objects"
    detached_root = tmp_path / "detached-objects"
    client = LocalObjectStorageClient(root)

    client.put_file_if_absent(first_source, ObjectStorageUri.parse("s3://bucket/releases/object"))
    root.rename(detached_root)
    root.mkdir()

    with pytest.raises(ValueError, match="changed between operations"):
        client.put_file_if_absent(marker_source, ObjectStorageUri.parse("s3://bucket/releases/_SUCCESS"))

    assert (detached_root / "s3" / "bucket" / "releases" / "object").read_bytes() == b"release"
    assert not (root / "s3" / "bucket" / "releases" / "_SUCCESS").exists()


def test_local_object_storage_rejects_symlink_escape_below_root(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.write_text("protected\n", encoding="utf-8")
    root = tmp_path / "objects"
    outside = tmp_path / "outside"
    outside.mkdir()
    provider_dir = root / "s3"
    provider_dir.mkdir(parents=True)
    (provider_dir / "bucket").symlink_to(outside, target_is_directory=True)
    client = LocalObjectStorageClient(root)

    with pytest.raises(ValueError, match="symlink"):
        client.put_file_if_absent(source, ObjectStorageUri.parse("s3://bucket/escape"))

    assert not (outside / "escape").exists()


def test_local_object_storage_rejects_symlinked_registry_root_parent(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"must-stay-confined")
    outside = tmp_path / "outside"
    outside.mkdir()
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(outside, target_is_directory=True)
    client = LocalObjectStorageClient(linked_parent / "registry")

    with pytest.raises(ValueError, match="symlink"):
        client.put_file_if_absent(source, ObjectStorageUri.parse("s3://bucket/releases/object"))

    assert not (outside / "registry").exists()


@pytest.mark.parametrize(
    "raw_key",
    [
        "",
        "/releases/sha256-abc/release-set.json",
        "../outside",
        "releases/../outside",
        "current/airflow-index.json",
        "releases/latest/release-set.json",
        r"releases\\sha256-abc\\release-set.json",
        "releases/sha256-abc/object?signature=secret",
        "releases/sha256-abc/object#fragment",
    ],
)
def test_object_storage_registry_rejects_unsafe_or_mutable_keys(tmp_path: Path, raw_key: str) -> None:
    registry = ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(tmp_path / "objects"),
        root=ObjectStorageUri.parse("s3://dpone-artifacts/airflow"),
    )

    with pytest.raises(ArtifactRegistryKeyError):
        registry.stat(PurePosixPath(raw_key))


def test_object_storage_registry_rejects_mutable_root(tmp_path: Path) -> None:
    with pytest.raises(ArtifactRegistryKeyError):
        ObjectStorageArtifactRegistry(
            client=LocalObjectStorageClient(tmp_path / "objects"),
            root=ObjectStorageUri.parse("s3://dpone-artifacts/current"),
        )


@pytest.mark.parametrize(
    "uri",
    [
        "s3://bucket",
        "s3://bucket/",
        "s3://bucket//root",
        "gs://bucket//root",
        "az://container//root",
        "azure://account//container/root",
    ],
)
def test_artifact_registry_root_rejects_empty_or_repeated_path_components(uri: str) -> None:
    with pytest.raises(ArtifactRegistryKeyError):
        parse_artifact_registry_root(uri)


@pytest.mark.parametrize(
    "uri",
    [
        "s3://bucket/root",
        "gs://bucket/root",
        "az://container/root",
        "azure://account/container/root",
    ],
)
def test_artifact_registry_root_accepts_canonical_provider_uris(uri: str) -> None:
    assert parse_artifact_registry_root(uri).key == "root"


@pytest.mark.parametrize(
    "root",
    [
        ObjectStorageUri.parse("s3://dpone-artifacts/root/./child"),
        ObjectStorageUri.parse("s3://dpone-artifacts/root//child"),
        ObjectStorageUri(
            ObjectStorageUri.parse("s3://dpone-artifacts/root").provider, "dpone-artifacts", r"root\child"
        ),
    ],
)
def test_object_storage_registry_rejects_noncanonical_root(tmp_path: Path, root: ObjectStorageUri) -> None:
    with pytest.raises(ArtifactRegistryKeyError):
        ObjectStorageArtifactRegistry(
            client=LocalObjectStorageClient(tmp_path / "objects"),
            root=root,
        )


@pytest.mark.parametrize(
    "uri",
    [
        "s3://user:secret@bucket/airflow",
        r"s3://bucket\\alias/airflow",
    ],
)
def test_object_storage_registry_rejects_credentialed_or_unsafe_authority(tmp_path: Path, uri: str) -> None:
    with pytest.raises((ArtifactRegistryKeyError, ValueError)):
        ObjectStorageArtifactRegistry(
            client=LocalObjectStorageClient(tmp_path / "objects"),
            root=ObjectStorageUri.parse(uri),
        )


def test_s3_conditional_create_uses_if_none_match_and_sha_metadata(tmp_path: Path) -> None:
    source = tmp_path / "object"
    source.write_bytes(b"content")
    fake = _ConditionalS3Client()
    client = S3ObjectStorageClient(client=fake)
    uri = ObjectStorageUri.parse("s3://bucket/release/object")

    created = client.put_file_if_absent(source, uri)

    assert created.size_bytes == 7
    assert fake.last_put["IfNoneMatch"] == "*"
    assert len(fake.last_put["Metadata"]["dpone-sha256"]) == 64
    with pytest.raises(ObjectStorageWriteConflict):
        client.put_file_if_absent(source, uri)


def test_s3_client_exposes_actual_sdk_endpoint_authority() -> None:
    fake = _ConditionalS3Client(endpoint_url="https://storage.example.test")

    client = S3ObjectStorageClient(client=fake)

    assert client.endpoint_url == "https://storage.example.test"


def test_gcs_conditional_create_uses_zero_generation_precondition(tmp_path: Path) -> None:
    source = tmp_path / "object"
    source.write_bytes(b"content")
    fake = _ConditionalGCSClient()
    client = GCSObjectStorageClient(client=fake)
    uri = ObjectStorageUri.parse("gs://bucket/release/object")

    client.put_file_if_absent(source, uri)

    assert fake.object_blob.last_upload["if_generation_match"] == 0
    assert len(fake.object_blob.metadata["dpone-sha256"]) == 64


def test_gcs_conditional_create_maps_precondition_conflict(tmp_path: Path) -> None:
    source = tmp_path / "object"
    source.write_bytes(b"content")
    fake = _ConditionalGCSClient()
    fake.object_blob.conflict = True
    client = GCSObjectStorageClient(client=fake)

    with pytest.raises(ObjectStorageWriteConflict):
        client.put_file_if_absent(source, ObjectStorageUri.parse("gs://bucket/release/object"))


def test_azure_conditional_create_disables_overwrite_and_sets_sha_metadata(tmp_path: Path) -> None:
    source = tmp_path / "object"
    source.write_bytes(b"content")
    fake = _ConditionalAzureServiceClient()
    client = AzureBlobObjectStorageClient(service_client=fake)
    uri = ObjectStorageUri.parse("az://container/release/object")

    client.put_file_if_absent(source, uri)

    assert fake.blob.last_upload["overwrite"] is False
    assert len(fake.blob.last_upload["metadata"]["dpone-sha256"]) == 64


def test_azure_conditional_create_maps_existing_blob_conflict(tmp_path: Path) -> None:
    source = tmp_path / "object"
    source.write_bytes(b"content")
    fake = _ConditionalAzureServiceClient()
    fake.blob.conflict = True
    client = AzureBlobObjectStorageClient(service_client=fake)

    with pytest.raises(ObjectStorageWriteConflict):
        client.put_file_if_absent(source, ObjectStorageUri.parse("az://container/release/object"))


@pytest.mark.parametrize("provider", ["s3", "gcs", "azure"])
def test_cloud_bounded_download_aborts_before_writing_metadata_mismatch(tmp_path: Path, provider: str) -> None:
    payload = b"declared" + b"unexpected"
    target = tmp_path / f"{provider}.object"
    if provider == "s3":
        client = S3ObjectStorageClient(client=_DownloadS3Client(payload))
        uri = ObjectStorageUri.parse("s3://bucket/object")
    elif provider == "gcs":
        client = GCSObjectStorageClient(client=_DownloadGCSClient(payload))
        uri = ObjectStorageUri.parse("gs://bucket/object")
    else:
        client = AzureBlobObjectStorageClient(service_client=_DownloadAzureServiceClient(payload))
        uri = ObjectStorageUri.parse("az://container/object")

    with pytest.raises(ObjectStorageReadLimitExceeded):
        client.get_file_bounded(uri, target, max_bytes=len(b"declared"))

    assert not target.exists()


def test_gcs_logical_connection_uses_resolved_service_account_instead_of_ambient_adc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    class FakeCredentials:
        project_id = "service-account-project"

        @classmethod
        def from_service_account_info(cls, info):
            created["service_account_info"] = info
            return cls()

    class FakeClient:
        def __init__(self, *, project, credentials):
            created["project"] = project
            created["credentials"] = credentials

    google = types.ModuleType("google")
    cloud = types.ModuleType("google.cloud")
    storage = types.ModuleType("google.cloud.storage")
    oauth2 = types.ModuleType("google.oauth2")
    service_account = types.ModuleType("google.oauth2.service_account")
    storage.Client = FakeClient
    service_account.Credentials = FakeCredentials
    cloud.storage = storage
    oauth2.service_account = service_account
    google.cloud = cloud
    google.oauth2 = oauth2
    for name, module in {
        "google": google,
        "google.cloud": cloud,
        "google.cloud.storage": storage,
        "google.oauth2": oauth2,
        "google.oauth2.service_account": service_account,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    resolver = ObjectStorageConnectionResolver(
        credentials_manager=_StaticCredentialsManager(
            CredentialsConfig(
                project_id="binding-project",
                service_account_info={"client_email": "artifact-reader@example.com"},
            )
        )
    )
    resolver.build_client(
        ref=ObjectStorageConnectionRef(connection_type="vault", connection_id="gcs_artifacts"),
        uri=ObjectStorageUri.parse("gs://bucket/releases"),
    )

    assert created["service_account_info"] == {"client_email": "artifact-reader@example.com"}
    assert created["project"] == "binding-project"
    assert isinstance(created["credentials"], FakeCredentials)


class _StaticCredentialsManager:
    def __init__(self, credentials: CredentialsConfig) -> None:
        self._credentials = credentials

    def get_credentials(self, *_args, **_kwargs) -> CredentialsConfig:
        return self._credentials


class _ConflictError(RuntimeError):
    status_code = 412


class _ConditionalS3Client:
    def __init__(self, endpoint_url: str | None = None) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.last_put: dict[str, Any] = {}
        self.meta = types.SimpleNamespace(endpoint_url=endpoint_url)

    def put_object(self, *, Body, **kwargs) -> None:
        key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if key in self.objects:
            raise _ConflictError
        self.last_put = kwargs
        self.objects[key] = Body.read()


class _ConditionalGCSClient:
    def __init__(self) -> None:
        self.object_blob = _ConditionalGCSBlob()

    def bucket(self, _name: str):
        return self

    def blob(self, _key: str):
        return self.object_blob


class _ConditionalGCSBlob:
    def __init__(self) -> None:
        self.metadata: dict[str, str] = {}
        self.last_upload: dict[str, Any] = {}
        self.conflict = False

    def upload_from_filename(self, _filename: str, **kwargs) -> None:
        if self.conflict:
            raise _ConflictError
        self.last_upload = kwargs


class _ConditionalAzureServiceClient:
    def __init__(self) -> None:
        self.blob = _ConditionalAzureBlobClient()

    def get_blob_client(self, *, container: str, blob: str):
        assert container and blob
        return self.blob


class _ConditionalAzureBlobClient:
    def __init__(self) -> None:
        self.last_upload: dict[str, Any] = {}
        self.conflict = False

    def upload_blob(self, data, **kwargs) -> None:
        if self.conflict:
            raise _ConflictError
        data.read()
        self.last_upload = kwargs


class _DownloadS3Client:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def get_object(self, **_kwargs):
        return {"Body": BytesIO(self._payload)}


class _DownloadGCSClient:
    def __init__(self, payload: bytes) -> None:
        self._blob = _DownloadGCSBlob(payload)

    def bucket(self, _name: str):
        return self

    def blob(self, _key: str):
        return self._blob


class _DownloadGCSBlob:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def download_to_file(self, target) -> None:
        target.write(self._payload)


class _DownloadAzureServiceClient:
    def __init__(self, payload: bytes) -> None:
        self._blob = _DownloadAzureBlob(payload)

    def get_blob_client(self, **_kwargs):
        return self._blob


class _DownloadAzureBlob:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def download_blob(self):
        return self

    def chunks(self):
        yield self._payload
