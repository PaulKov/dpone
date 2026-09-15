"""Exact object-version binding without storage authentication or mutation."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeGenerationOriginalSubject,
    NativeOriginalBinding,
    NativeOriginalBindingError,
    decode_native_original_binding,
    encode_native_original_binding,
)
from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef

D = "sha256:" + "a" * 64
E = "sha256:" + "b" * 64


def binding():
    authority = DbtWorkspaceRuntimeAuthority.build(
        environment="dev",
        release_id=D,
        deployment_id=D,
        release_sha256=D,
        deployment_sha256=D,
        binding_set_sha256=D,
        connection_registry_sha256=D,
        credential_runtime_sha256=D,
    )
    return NativeOriginalBinding(
        NativeGenerationOriginalSubject(authority, UUID("12345678-1234-5678-1234-567812345678")),
        "generation_stored_file_v1",
        OriginalRef("authority/store.json", D),
        ArtifactObjectRef("objects/export", "opaque/+version==", 0, D, "kms:scope", "2026-09-15T10:00:00+03:00"),
        E,
        "generation/export",
    )


def test_roundtrip_preserves_existing_types_and_distinct_payload_hash():
    value = binding()
    restored = decode_native_original_binding(encode_native_original_binding(value))
    assert restored == value
    assert type(restored.object_ref) is ArtifactObjectRef
    assert type(restored.storage_authority) is OriginalRef
    assert restored.object_ref.size_bytes == 0
    assert restored.payload_sha256 != restored.object_ref.sha256
    assert restored.object_ref.version == "opaque/+version=="
    assert restored.object_ref.retention_until == "2026-09-15T10:00:00+03:00"


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", ""),
        ("version", 1),
        ("size_bytes", True),
        ("size_bytes", -1),
        ("key", "../escape"),
        ("sha256", "bad"),
        ("encryption_scope", ""),
        ("retention_until", "2026-09-15"),
        ("retention_until", "2026-09-15T10:00:00"),
    ],
)
def test_invalid_provider_fields_fail_at_native_boundary_only(field, value):
    original = binding()
    legacy = replace(original.object_ref, **{field: value})
    assert getattr(legacy, field) == value  # Existing DTO compatibility is unchanged.
    with pytest.raises(NativeOriginalBindingError):
        replace(original, object_ref=legacy)


def test_every_wire_field_required_and_closed():
    raw = decode_native_delivery_json(encode_native_original_binding(binding()))
    assert set(raw) == {"schema", "subject", "kind", "storage_authority", "object_ref", "payload_sha256", "locator"}
    for field in raw:
        for altered in ({k: v for k, v in raw.items() if k != field}, {**raw, field: None}):
            with pytest.raises(NativeOriginalBindingError):
                decode_native_original_binding(encode_native_delivery_json(altered))
    for name in ("storage_authority", "object_ref"):
        for field in raw[name]:
            altered = {**raw, name: {k: v for k, v in raw[name].items() if k != field}}
            with pytest.raises(NativeOriginalBindingError):
                decode_native_original_binding(encode_native_delivery_json(altered))
        with pytest.raises(NativeOriginalBindingError):
            decode_native_original_binding(encode_native_delivery_json({**raw, name: {**raw[name], "extra": 1}}))


@pytest.mark.parametrize("field,value", [("kind", "unknown"), ("locator", "../escape"), ("payload_sha256", "bad")])
def test_invalid_binding_fields(field, value):
    with pytest.raises(NativeOriginalBindingError):
        replace(binding(), **{field: value})


def test_noncanonical_bytes_and_duplicate_keys_rejected():
    data = encode_native_original_binding(binding())
    for invalid in (b" " + data, data[:-1] + b',"kind":"generation_stored_file_v1"}'):
        with pytest.raises(NativeOriginalBindingError):
            decode_native_original_binding(invalid)


def test_complete_provider_tuple_changes_identity():
    value = binding()
    for field, new in [
        ("key", "objects/other"),
        ("version", "another"),
        ("size_bytes", 1),
        ("sha256", E),
        ("encryption_scope", "other"),
        ("retention_until", "2027-01-01T00:00:00Z"),
    ]:
        changed = replace(value, object_ref=replace(value.object_ref, **{field: new}))
        assert changed != value
        assert encode_native_original_binding(changed) != encode_native_original_binding(value)


def test_encoding_revalidates_bypassed_frozen_value():
    value = binding()
    object.__setattr__(value.object_ref, "size_bytes", True)
    with pytest.raises(NativeOriginalBindingError):
        encode_native_original_binding(value)


def test_invalid_nested_value_does_not_invoke_copy_hooks():
    class Untrusted:
        def __deepcopy__(self, memo):
            raise AssertionError("user copy hook must not run")

    value = binding()
    object.__setattr__(value.object_ref, "version", Untrusted())
    with pytest.raises(NativeOriginalBindingError):
        encode_native_original_binding(value)


@pytest.mark.parametrize(
    "kind",
    [
        "generation_storage_root_v1",
        "generation_stored_file_v1",
        "generation_seal_resolution_v1",
        "trusted_dbt_command_plan_v1",
        "trusted_dbt_invocation_completion_v1",
        "trusted_dbt_toolchain_v1",
        "trusted_dbt_qualification_v1",
        "trusted_dbt_owned_root_v1",
    ],
)
def test_closed_supported_kinds_roundtrip(kind):
    value = replace(binding(), kind=kind)
    assert decode_native_original_binding(encode_native_original_binding(value)) == value


def test_provider_key_is_not_normalized_as_a_locator():
    value = binding()
    key = "objects//./preserve"
    changed = replace(value, object_ref=replace(value.object_ref, key=key))
    assert decode_native_original_binding(encode_native_original_binding(changed)).object_ref.key == key


def test_complete_top_level_tuple_changes_identity():
    value = binding()
    changes = {
        "subject": replace(value.subject, generation_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
        "kind": "generation_storage_root_v1",
        "storage_authority": OriginalRef("authority/other.json", E),
        "object_ref": replace(value.object_ref, version="another"),
        "payload_sha256": D,
        "locator": "generation/another",
    }
    assert binding() == value
    for field, changed in changes.items():
        other = replace(value, **{field: changed})
        assert other != value
        assert encode_native_original_binding(other) != encode_native_original_binding(value)


def test_binding_keeps_native_primitive_bounds():
    value = binding()
    with pytest.raises(NativeOriginalBindingError):
        replace(value, object_ref=replace(value.object_ref, version="v" * 4097))
    with pytest.raises(NativeOriginalBindingError):
        replace(value, object_ref=replace(value.object_ref, size_bytes=10**128))


def test_no_generic_kind_subject_authority_inference():
    from dpone.contracts.native_originals import NativePlatformOriginalSubject

    value = replace(
        binding(),
        subject=NativePlatformOriginalSubject(binding().subject.authority, D),
        kind="generation_storage_root_v1",
    )
    assert decode_native_original_binding(encode_native_original_binding(value)) == value
