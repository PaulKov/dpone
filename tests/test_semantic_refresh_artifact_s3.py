"""S3 create-only/version/KMS authority adapter tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
from types import SimpleNamespace

import pytest

from dpone.adapters.semantic_refresh_artifact_s3 import S3CreateOnlyArtifactStore
from dpone.adapters.semantic_refresh_artifact_s3_resolver import (
    S3ArtifactStorePolicy,
    S3CreateOnlyArtifactStoreResolver,
)
from dpone.ports.semantic_refresh_artifact_store import (
    ArtifactCreateConflict,
    ArtifactStoreBinding,
    ArtifactStoreUnavailable,
)

_KMS_KEY_ARN = "arn:aws:kms:eu-central-1:123456789012:key/11111111-2222-3333-4444-555555555555"
_ENDPOINT = "https://s3.eu-central-1.amazonaws.com"
_PREFIX = "operations/operation-1"
_RETENTION_ISSUED_AT = "2026-08-08T00:00:00Z"
_RETENTION_UNTIL = "2026-08-15T00:00:00Z"
_PAYLOAD_SHA256 = "sha256:" + sha256(b"payload").hexdigest()


def _clock() -> datetime:
    return datetime(2026, 8, 8, tzinfo=timezone.utc)  # noqa: UP017


class _ConditionalConflict(Exception):
    response = {"ResponseMetadata": {"HTTPStatusCode": 412}}


class _Client:
    def __init__(self) -> None:
        self.meta = SimpleNamespace(endpoint_url=_ENDPOINT)
        self.created: dict[str, object] | None = None
        self.conflict = False
        self.encryption = "aws:kms"
        self.versioning = "Enabled"
        self.object_lock = "Enabled"
        self.version_id = "version-1"
        self.sha256 = _PAYLOAD_SHA256
        self.lock_mode = "GOVERNANCE"
        self.retention_until = "2026-08-15T00:00:00Z"
        self.object_lock_retention = "2026-08-15T00:00:00Z"
        self.extra_version = False
        self.capability_evidence_sha256 = "sha256:" + "1" * 64

    def get_bucket_versioning(self, **_kwargs: object) -> dict[str, str]:
        return {"Status": self.versioning}

    def get_object_lock_configuration(self, **_kwargs: object) -> dict[str, object]:
        return {"ObjectLockConfiguration": {"ObjectLockEnabled": self.object_lock}}

    def put_object(self, **kwargs: object) -> dict[str, str]:
        if self.conflict:
            raise _ConditionalConflict
        self.created = kwargs
        return {"VersionId": self.version_id}

    def head_object(self, **kwargs: object) -> dict[str, object]:
        if "VersionId" in kwargs:
            assert kwargs["VersionId"] == self.version_id
        return {
            "VersionId": self.version_id,
            "ContentLength": 7,
            "ServerSideEncryption": self.encryption,
            "SSEKMSKeyId": _KMS_KEY_ARN,
            "Metadata": {
                "dpone-sha256": self.sha256,
                "dpone-encryption-scope": "business-sensitive",
                "dpone-retention-until": self.retention_until,
                "dpone-capability-evidence-sha256": self.capability_evidence_sha256,
                "dpone-encryption-policy-sha256": "sha256:" + "2" * 64,
                "dpone-retention-policy-id": "retention-7d",
                "dpone-retention-policy-sha256": "sha256:" + "3" * 64,
                "dpone-retention-days": "7",
                "dpone-retention-issued-at": _RETENTION_ISSUED_AT,
                "dpone-writer-scope": "business-sensitive",
            },
            "ObjectLockMode": self.lock_mode,
            "ObjectLockRetainUntilDate": self.object_lock_retention,
        }

    def get_object(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["VersionId"] == "version-1"
        return {"Body": BytesIO(b"payload")}

    def list_object_versions(self, **kwargs: object) -> dict[str, object]:
        key = str(kwargs["Prefix"])
        versions = [
            {
                "Key": key,
                "VersionId": self.version_id,
                "IsLatest": True,
            }
        ]
        if self.extra_version:
            versions.append({"Key": key, "VersionId": "older-version", "IsLatest": False})
        return {"IsTruncated": False, "Versions": versions, "DeleteMarkers": []}


def _policy(**overrides: object) -> S3ArtifactStorePolicy:
    values: dict[str, object] = {
        "provider_profile": "s3_create_only_versioned_kms_object_lock_v1",
        "endpoint_authority_id": _ENDPOINT,
        "bucket_or_container_authority_id": "dpone-semantic-refresh",
        "kms_key_authority_id": _KMS_KEY_ARN,
        "capability_evidence_sha256": "sha256:" + "1" * 64,
        "writer_scope": "business-sensitive",
        "artifact_prefix": _PREFIX,
        "encryption_policy_sha256": "sha256:" + "2" * 64,
        "retention_policy_id": "retention-7d",
        "retention_policy_sha256": "sha256:" + "3" * 64,
        "retention_days": 7,
        "retention_issued_at": _RETENTION_ISSUED_AT,
        "retention_until": _RETENTION_UNTIL,
        "max_artifact_bytes": 1_000,
        "conditional_create_authorized": True,
        "require_object_lock": True,
        "object_lock_mode": "GOVERNANCE",
    }
    values.update(overrides)
    return S3ArtifactStorePolicy(**values)  # type: ignore[arg-type]


def _store(client: _Client) -> S3CreateOnlyArtifactStore:
    return S3CreateOnlyArtifactStore(
        client=client,
        bucket="dpone-semantic-refresh",
        operation_prefix=_PREFIX,
        policy=_policy(),
        clock=_clock,
    )


def _binding(*, artifact_prefix: str = _PREFIX) -> ArtifactStoreBinding:
    policy = _policy()
    return ArtifactStoreBinding(
        provider="s3",
        provider_profile=policy.provider_profile,
        endpoint_authority_id=policy.endpoint_authority_id,
        bucket_or_container_authority_id=policy.bucket_or_container_authority_id,
        kms_key_authority_id=policy.kms_key_authority_id,
        capability_evidence_sha256=policy.capability_evidence_sha256,
        writer_scope=policy.writer_scope,
        artifact_prefix=artifact_prefix,
        encryption_policy_sha256=policy.encryption_policy_sha256,
        retention_policy_id=policy.retention_policy_id,
        retention_policy_sha256=policy.retention_policy_sha256,
        retention_days=policy.retention_days,
        retention_issued_at=policy.retention_issued_at,
        retention_until=policy.retention_until,
        max_artifact_bytes=policy.max_artifact_bytes,
    )


def test_s3_create_is_conditional_kms_bound_and_version_pinned() -> None:
    client = _Client()
    store = _store(client)

    ref = store.create(
        key="operations/operation-1/chunks/000000.parquet",
        content=b"payload",
        sha256=_PAYLOAD_SHA256,
        encryption_scope="business-sensitive",
        retention_until="2026-08-15T00:00:00Z",
    )

    assert client.created is not None
    assert client.created["IfNoneMatch"] == "*"
    assert client.created["ServerSideEncryption"] == "aws:kms"
    assert client.created["SSEKMSKeyId"] == _KMS_KEY_ARN
    assert client.created["ObjectLockMode"] == "GOVERNANCE"
    assert isinstance(client.created["ObjectLockRetainUntilDate"], datetime)
    assert client.created["ObjectLockRetainUntilDate"].isoformat() == "2026-08-15T00:00:00+00:00"
    assert ref.version == "version-1"
    assert store.head(key=ref.key) == ref
    assert store.read_version(key=ref.key, version=ref.version) == b"payload"


def test_s3_conditional_conflict_and_encryption_drift_fail_closed() -> None:
    client = _Client()
    store = _store(client)
    client.conflict = True
    with pytest.raises(ArtifactCreateConflict):
        store.create(
            key="operations/operation-1/manifest.json",
            content=b"payload",
            sha256=_PAYLOAD_SHA256,
            encryption_scope="business-sensitive",
            retention_until="2026-08-15T00:00:00Z",
        )

    client.conflict = False
    client.encryption = "AES256"
    with pytest.raises(ArtifactStoreUnavailable, match="encryption"):
        store.head(key="operations/operation-1/manifest.json")


def test_s3_adapter_rejects_keys_outside_exact_operation_prefix() -> None:
    with pytest.raises(ValueError, match="outside"):
        _store(_Client()).head(key="operations/other/manifest.json")


def test_s3_create_rejects_ambiguous_exact_key_version_history() -> None:
    client = _Client()
    client.extra_version = True

    with pytest.raises(ArtifactStoreUnavailable, match="version history is ambiguous"):
        _store(client).create(
            key="operations/operation-1/manifest.json",
            content=b"payload",
            sha256=_PAYLOAD_SHA256,
            encryption_scope="business-sensitive",
            retention_until="2026-08-15T00:00:00Z",
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("version_id", "", "version-pinned"),
        ("sha256", "sha256:" + "b" * 64, "metadata differs"),
        ("lock_mode", "COMPLIANCE", "object lock mode"),
        ("object_lock_retention", "2026-08-14T00:00:00Z", "retention authority"),
    ],
)
def test_s3_exact_provider_metadata_drift_fails_closed(
    field: str,
    value: str,
    message: str,
) -> None:
    client = _Client()
    setattr(client, field, value)

    with pytest.raises(ArtifactStoreUnavailable, match=message):
        _store(client).create(
            key="operations/operation-1/manifest.json",
            content=b"payload",
            sha256=_PAYLOAD_SHA256,
            encryption_scope="business-sensitive",
            retention_until="2026-08-15T00:00:00Z",
        )


@pytest.mark.parametrize(
    ("versioning", "conditional_create_authorized", "object_lock", "message"),
    [
        ("Suspended", True, "Enabled", "versioning"),
        ("Enabled", False, "Enabled", "conditional create"),
        ("Enabled", True, "Disabled", "object lock"),
    ],
)
def test_s3_capabilities_fail_closed_before_create(
    versioning: str,
    conditional_create_authorized: bool,
    object_lock: str,
    message: str,
) -> None:
    client = _Client()
    client.versioning = versioning
    client.object_lock = object_lock
    store = S3CreateOnlyArtifactStore(
        client=client,
        bucket="dpone-semantic-refresh",
        operation_prefix=_PREFIX,
        policy=_policy(conditional_create_authorized=conditional_create_authorized),
        clock=_clock,
    )

    with pytest.raises(ArtifactStoreUnavailable, match=message):
        store.create(
            key="operations/operation-1/manifest.json",
            content=b"payload",
            sha256=_PAYLOAD_SHA256,
            encryption_scope="business-sensitive",
            retention_until="2026-08-15T00:00:00Z",
        )
    assert client.created is None


def test_s3_v2_policy_rejects_encryption_or_retention_downgrade() -> None:
    with pytest.raises(ValueError, match="aws:kms"):
        S3CreateOnlyArtifactStore(
            client=_Client(),
            bucket="dpone-semantic-refresh",
            operation_prefix=_PREFIX,
            encryption_algorithm="AES256",
            policy=_policy(),
            clock=_clock,
        )
    with pytest.raises(ValueError, match="shorter than retention_days"):
        _policy(retention_issued_at="2026-08-09T00:00:00Z")
    with pytest.raises(ValueError, match="Object Lock"):
        S3CreateOnlyArtifactStore(
            client=_Client(),
            bucket="dpone-semantic-refresh",
            operation_prefix=_PREFIX,
            policy=_policy(require_object_lock=False, object_lock_mode=None),
            clock=_clock,
        )


@pytest.mark.parametrize(
    ("configured", "message"),
    [
        ({"bucket": "other-bucket"}, "bucket differs"),
        ({"operation_prefix": "operations/other"}, "prefix differs"),
    ],
)
def test_s3_adapter_rejects_configured_coordinates_outside_protected_authority(
    configured: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        S3CreateOnlyArtifactStore(
            client=_Client(),
            bucket=configured.get("bucket", "dpone-semantic-refresh"),
            operation_prefix=configured.get("operation_prefix", _PREFIX),
            policy=_policy(),
            clock=_clock,
        )

    client = _Client()
    client.meta.endpoint_url = "https://untrusted.example"
    with pytest.raises(ValueError, match="endpoint differs"):
        S3CreateOnlyArtifactStore(
            client=client,
            bucket="dpone-semantic-refresh",
            operation_prefix=_PREFIX,
            policy=_policy(),
            clock=_clock,
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"encryption_scope": "other-writer"}, "writer scope differs"),
        ({"retention_until": "2026-08-16T00:00:00Z"}, "retention_until differs"),
        ({"content": b"payload-too-large"}, "byte budget"),
    ],
)
def test_s3_create_rejects_values_outside_protected_policy(
    kwargs: dict[str, object],
    message: str,
) -> None:
    values: dict[str, object] = {
        "key": f"{_PREFIX}/manifest.json",
        "content": b"payload",
        "sha256": _PAYLOAD_SHA256,
        "encryption_scope": "business-sensitive",
        "retention_until": _RETENTION_UNTIL,
    }
    values.update(kwargs)
    policy = _policy(max_artifact_bytes=7 if "content" in kwargs else 1_000)

    with pytest.raises(ValueError, match=message):
        S3CreateOnlyArtifactStore(
            client=_Client(),
            bucket="dpone-semantic-refresh",
            operation_prefix=_PREFIX,
            policy=policy,
            clock=_clock,
        ).create(**values)  # type: ignore[arg-type]


def test_s3_exact_object_remains_reconcilable_as_fixed_retention_window_advances() -> None:
    client = _Client()
    current = datetime(2026, 8, 8, tzinfo=timezone.utc)  # noqa: UP017
    store = S3CreateOnlyArtifactStore(
        client=client,
        bucket="dpone-semantic-refresh",
        operation_prefix=_PREFIX,
        policy=_policy(),
        clock=lambda: current,
    )
    key = f"{_PREFIX}/manifest.json"
    created = store.create(
        key=key,
        content=b"payload",
        sha256=_PAYLOAD_SHA256,
        encryption_scope="business-sensitive",
        retention_until=_RETENTION_UNTIL,
    )
    current = datetime(2026, 8, 14, tzinfo=timezone.utc)  # noqa: UP017
    client.conflict = True

    with pytest.raises(ArtifactCreateConflict):
        store.create(
            key=key,
            content=b"payload",
            sha256=_PAYLOAD_SHA256,
            encryption_scope="business-sensitive",
            retention_until=_RETENTION_UNTIL,
        )

    assert store.head(key=key) == created
    assert store.read_version(key=key, version=created.version) == b"payload"


def test_s3_read_rejects_expired_protected_retention_before_object_io() -> None:
    client = _Client()
    store = S3CreateOnlyArtifactStore(
        client=client,
        bucket="dpone-semantic-refresh",
        operation_prefix=_PREFIX,
        policy=_policy(),
        clock=lambda: datetime(2026, 8, 15, tzinfo=timezone.utc),  # noqa: UP017
    )

    with pytest.raises(ArtifactStoreUnavailable, match="retention has expired"):
        store.read_version(key=f"{_PREFIX}/manifest.json", version="version-1")


def test_s3_head_rejects_protected_capability_metadata_drift() -> None:
    client = _Client()
    client.capability_evidence_sha256 = "sha256:" + "9" * 64

    with pytest.raises(ArtifactStoreUnavailable, match="protected object metadata"):
        _store(client).head(key=f"{_PREFIX}/manifest.json")


def test_s3_exact_read_revalidates_capabilities_and_kms_metadata() -> None:
    client = _Client()
    client.versioning = "Suspended"
    with pytest.raises(ArtifactStoreUnavailable, match="versioning"):
        _store(client).read_version(key=f"{_PREFIX}/manifest.json", version="version-1")

    client.versioning = "Enabled"
    client.encryption = "AES256"
    with pytest.raises(ArtifactStoreUnavailable, match="encryption"):
        _store(client).read_version(key=f"{_PREFIX}/manifest.json", version="version-1")


def test_s3_resolver_supports_distinct_operation_prefixes_under_exact_provider_policy() -> None:
    client = _Client()
    resolver = S3CreateOnlyArtifactStoreResolver(
        client=client,
        provider_policy=_policy(object_lock_mode="COMPLIANCE"),
        clock=_clock,
    )

    first = resolver.resolve(_binding(artifact_prefix="operations/operation-1"))
    second = resolver.resolve(_binding(artifact_prefix="operations/operation-2"))

    with pytest.raises(ValueError, match="outside"):
        first.head(key="operations/operation-2/manifest.json")
    with pytest.raises(ValueError, match="outside"):
        second.head(key="operations/operation-1/manifest.json")
    assert client.created is None


@pytest.mark.parametrize(
    "field_name",
    [
        "kms_key_authority_id",
        "encryption_policy_sha256",
        "retention_policy_sha256",
        "capability_evidence_sha256",
        "retention_issued_at",
    ],
)
def test_s3_resolver_rejects_provider_policy_swap_before_put(field_name: str) -> None:
    client = _Client()
    resolver = S3CreateOnlyArtifactStoreResolver(
        client=client,
        provider_policy=_policy(object_lock_mode="COMPLIANCE"),
        clock=_clock,
    )
    if field_name == "kms_key_authority_id":
        value = "arn:aws:kms:eu-central-1:123456789012:key/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    elif field_name == "retention_issued_at":
        value = "2026-08-07T00:00:00Z"
    else:
        value = "sha256:" + "9" * 64

    with pytest.raises(ValueError, match="differs from protected operation"):
        resolver.resolve(replace(_binding(), **{field_name: value}))

    assert client.created is None
