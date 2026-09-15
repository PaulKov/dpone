"""Shared legacy policy compatibility and strict native authority metadata."""

import pickle
from dataclasses import asdict, replace

import pytest

from dpone.adapters.semantic_refresh_artifact_s3_resolver import S3ArtifactStorePolicy as LegacyPolicy
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_originals import (
    NativeOriginalStorageAuthority,
    NativeOriginalStorageAuthorityError,
    decode_native_original_storage_authority,
    encode_native_original_storage_authority,
)
from dpone.contracts.s3_artifact_store_policy import S3ArtifactStorePolicy

D = "sha256:" + "a" * 64


def authority():
    return NativeOriginalStorageAuthority(
        provider="s3",
        provider_profile="s3_create_only_versioned_kms_object_lock_v1",
        endpoint_authority_id="https://s3.example.invalid",
        bucket_or_container_authority_id="synthetic",
        kms_key_authority_id="arn:aws:kms:us-east-1:123456789012:key/synthetic",
        capability_evidence_sha256=D,
        writer_scope="native/test",
        artifact_prefix="originals/test",
        encryption_policy_sha256=D,
        retention_policy_id="policy1",
        retention_policy_sha256=D,
        retention_days=1,
        retention_issued_at="2026-09-15T00:00:00Z",
        retention_until="2026-09-16T00:00:00Z",
        max_artifact_bytes=1048576,
        conditional_create_authorized=True,
        require_object_lock=True,
        object_lock_mode="COMPLIANCE",
    )


def test_shared_policy_retains_legacy_identity_signature_and_pickle():
    assert S3ArtifactStorePolicy is LegacyPolicy
    assert S3ArtifactStorePolicy.__module__ == "dpone.adapters.semantic_refresh_artifact_s3_resolver"
    values = asdict(authority())
    values.pop("provider")
    values.pop("object_lock_mode")
    values["require_object_lock"] = False
    values["conditional_create_authorized"] = False
    legacy = LegacyPolicy(**values)
    assert legacy.object_lock_mode is None
    assert pickle.loads(pickle.dumps(legacy, protocol=4)) == legacy
    assert type(pickle.loads(pickle.dumps(legacy))) is LegacyPolicy


def test_native_exact_fields_and_roundtrip():
    value = authority()
    payload = encode_native_original_storage_authority(value)
    restored = decode_native_original_storage_authority(payload)
    assert restored == value
    raw = decode_native_delivery_json(payload)
    assert set(raw) == set(asdict(value)) | {"schema"} and len(raw) == 19
    assert raw["schema"] == "dpone.native-original-storage-authority.v1"
    for name in raw:
        for altered in ({k: v for k, v in raw.items() if k != name}, {**raw, name: None}):
            with pytest.raises(NativeOriginalStorageAuthorityError):
                decode_native_original_storage_authority(encode_native_delivery_json(altered))
    with pytest.raises(NativeOriginalStorageAuthorityError):
        decode_native_original_storage_authority(encode_native_delivery_json({**raw, "extra": 1}))


@pytest.mark.parametrize(
    "field,value",
    [
        ("provider", "azure"),
        ("provider_profile", "unknown"),
        ("kms_key_authority_id", "not-an-arn"),
        ("capability_evidence_sha256", "invalid"),
        ("conditional_create_authorized", False),
        ("conditional_create_authorized", 1),
        ("require_object_lock", False),
        ("object_lock_mode", "GOVERNANCE"),
        ("object_lock_mode", None),
        ("retention_days", True),
        ("retention_days", 0),
        ("max_artifact_bytes", 0),
        ("max_artifact_bytes", True),
        ("retention_until", "2026-09-15T23:59:59Z"),
        ("retention_issued_at", "2026-09-15T00:00:00+00:00"),
        ("artifact_prefix", "../escape"),
        ("artifact_prefix", "trailing/"),
        ("retention_days", 10**100),
        ("endpoint_authority_id", "x" * 4097),
    ],
)
def test_invalid_native_policy_rejected(field, value):
    with pytest.raises(NativeOriginalStorageAuthorityError):
        replace(authority(), **{field: value})


def test_canonical_bytes_and_frozen_revalidation():
    value = authority()
    payload = encode_native_original_storage_authority(value)
    with pytest.raises(NativeOriginalStorageAuthorityError):
        decode_native_original_storage_authority(b" " + payload)
    object.__setattr__(value, "conditional_create_authorized", False)
    with pytest.raises(NativeOriginalStorageAuthorityError):
        encode_native_original_storage_authority(value)


@pytest.mark.parametrize(
    "first",
    [
        "dpone.contracts.s3_artifact_store_policy",
        "dpone.contracts.native_originals",
        "dpone.adapters.semantic_refresh_artifact_s3_resolver",
    ],
)
def test_import_order_and_pickle_in_fresh_interpreter(first):
    import subprocess
    import sys

    script = """
import importlib, pickle, sys
importlib.import_module(sys.argv[1])
from dpone.contracts.s3_artifact_store_policy import S3ArtifactStorePolicy
if sys.argv[1].startswith('dpone.contracts.'):
    assert 'dpone.adapters.semantic_refresh_artifact_s3_resolver' not in sys.modules
from dpone.adapters.semantic_refresh_artifact_s3_resolver import S3ArtifactStorePolicy as Legacy
assert Legacy is S3ArtifactStorePolicy
assert pickle.loads(pickle.dumps(S3ArtifactStorePolicy)) is Legacy
"""
    result = subprocess.run([sys.executable, "-I", "-c", script, first], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
