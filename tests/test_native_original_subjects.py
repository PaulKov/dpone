"""Closed canonical subject identity, without storage or execution authority."""

from dataclasses import FrozenInstanceError, replace
from uuid import UUID

import pytest

from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_originals import (
    NativeDeliveryOriginalSubject,
    NativeGenerationOriginalSubject,
    NativeOriginalSubjectError,
    NativePlatformOriginalSubject,
    decode_native_original_subject,
    encode_native_original_subject,
)

DIGEST = "sha256:" + "a" * 64
OTHER_DIGEST = "sha256:" + "b" * 64
GENERATION = UUID("12345678-1234-5678-1234-567812345678")


def authority(environment="dev"):
    return DbtWorkspaceRuntimeAuthority.build(
        environment=environment,
        release_id=DIGEST,
        deployment_id=DIGEST,
        release_sha256=DIGEST,
        deployment_sha256=DIGEST,
        binding_set_sha256=DIGEST,
        connection_registry_sha256=DIGEST,
        credential_runtime_sha256=DIGEST,
    )


def subjects():
    return (
        NativeGenerationOriginalSubject(authority(), GENERATION),
        NativeDeliveryOriginalSubject(authority(), DIGEST, OTHER_DIGEST),
        NativePlatformOriginalSubject(authority(), DIGEST),
    )


@pytest.mark.parametrize("index", range(3))
def test_roundtrip_preserves_exact_variant_and_existing_authority(index):
    subject = subjects()[index]
    payload = encode_native_original_subject(subject)
    restored = decode_native_original_subject(payload)
    assert restored == subject
    assert type(restored) is type(subject)
    assert type(restored.authority) is DbtWorkspaceRuntimeAuthority
    assert encode_native_original_subject(restored) == payload
    assert decode_native_delivery_json(payload)["schema"] == "dpone.native-original-subject.v1"
    with pytest.raises(FrozenInstanceError):
        subject.authority = authority("prod")


@pytest.mark.parametrize("index", range(3))
def test_all_fields_required_and_unknown_fields_rejected(index):
    wire = decode_native_delivery_json(encode_native_original_subject(subjects()[index]))
    for field in tuple(wire):
        missing = {key: value for key, value in wire.items() if key != field}
        with pytest.raises(NativeOriginalSubjectError):
            decode_native_original_subject(encode_native_delivery_json(missing))
        with pytest.raises(NativeOriginalSubjectError):
            decode_native_original_subject(encode_native_delivery_json({**wire, field: None}))
    for field in {"generation_id", "operation_id", "attempt_id", "platform_policy_sha256", "unknown"} - set(wire):
        with pytest.raises(NativeOriginalSubjectError):
            decode_native_original_subject(encode_native_delivery_json({**wire, field: DIGEST}))


@pytest.mark.parametrize("index", range(3))
def test_nested_authority_is_closed_and_its_fingerprint_is_checked(index):
    wire = decode_native_delivery_json(encode_native_original_subject(subjects()[index]))
    nested = wire["authority"]
    for key in nested:
        for replacement in (None, False, 1, OTHER_DIGEST):
            altered = {**wire, "authority": {**nested, key: replacement}}
            with pytest.raises(NativeOriginalSubjectError):
                decode_native_original_subject(encode_native_delivery_json(altered))
        missing = {name: value for name, value in nested.items() if name != key}
        with pytest.raises(NativeOriginalSubjectError):
            decode_native_original_subject(encode_native_delivery_json({**wire, "authority": missing}))
    with pytest.raises(NativeOriginalSubjectError):
        decode_native_original_subject(encode_native_delivery_json({**wire, "authority": {**nested, "extra": 1}}))


@pytest.mark.parametrize(
    "generation",
    [str(GENERATION).upper().replace("1234", "ABCD", 1), str(GENERATION).replace("-", ""), "not-a-uuid", None, 1],
)
def test_uuid_wire_requires_canonical_lowercase_hyphenated_form(generation):
    wire = decode_native_delivery_json(encode_native_original_subject(subjects()[0]))
    with pytest.raises(NativeOriginalSubjectError):
        decode_native_original_subject(encode_native_delivery_json({**wire, "generation_id": generation}))


@pytest.mark.parametrize("index", range(3))
def test_noncanonical_and_duplicate_json_never_define_a_subject(index):
    payload = encode_native_original_subject(subjects()[index])
    for altered in (
        b" " + payload,
        payload + b"\n",
        payload.replace(b":", b": ", 1),
        payload[:-1] + b',"scope":"GENERATION"}',
        b"\xef\xbb\xbf" + payload,
    ):
        with pytest.raises(NativeOriginalSubjectError):
            decode_native_original_subject(altered)


@pytest.mark.parametrize("scope", ["generation", "UNKNOWN", False, None])
def test_unknown_scope_is_rejected(scope):
    wire = decode_native_delivery_json(encode_native_original_subject(subjects()[0]))
    with pytest.raises(NativeOriginalSubjectError):
        decode_native_original_subject(encode_native_delivery_json({**wire, "scope": scope}))


def test_each_identity_coordinate_changes_canonical_subject():
    generation, delivery, platform = subjects()
    for original, changed in (
        (generation, replace(generation, generation_id=UUID(int=2))),
        (delivery, replace(delivery, operation_id=OTHER_DIGEST)),
        (delivery, replace(delivery, attempt_id=DIGEST)),
        (platform, replace(platform, platform_policy_sha256=OTHER_DIGEST)),
        *((subject, replace(subject, authority=authority("prod"))) for subject in subjects()),
    ):
        assert encode_native_original_subject(original) != encode_native_original_subject(changed)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: NativeGenerationOriginalSubject(authority(), str(GENERATION)),
        lambda: NativeDeliveryOriginalSubject(authority(), "a" * 64, DIGEST),
        lambda: NativePlatformOriginalSubject(authority(), False),
        lambda: NativeGenerationOriginalSubject({}, GENERATION),
    ],
)
def test_direct_construction_rejects_wrong_domain_types(factory):
    with pytest.raises(NativeOriginalSubjectError):
        factory()


def test_encoding_revalidates_bypassed_frozen_field_types():
    subject = subjects()[0]
    object.__setattr__(subject, "generation_id", str(GENERATION))
    with pytest.raises(NativeOriginalSubjectError):
        encode_native_original_subject(subject)


@pytest.mark.parametrize(
    "payload", [b"[]", b'{"scope":true}', b'{"schema":"\xff"}', b"x" * (1048576 + 1), b'{"schema":1.0}', "{}"]
)
def test_subject_decoder_preserves_native_primitive_rejections(payload):
    with pytest.raises(NativeOriginalSubjectError):
        decode_native_original_subject(payload)


def test_subject_bytes_have_literal_closed_variant_fields():
    from json import loads

    for subject, expected in zip(
        subjects(),
        (
            {"generation_id": str(GENERATION)},
            {"operation_id": DIGEST, "attempt_id": OTHER_DIGEST},
            {"platform_policy_sha256": DIGEST},
        ),
        strict=True,
    ):
        value = loads(encode_native_original_subject(subject))
        assert set(value) == {"schema", "scope", "authority", *expected}
        assert {key: value[key] for key in expected} == expected
        assert set(value["authority"]) == {
            "environment",
            "release_id",
            "deployment_id",
            "release_sha256",
            "deployment_sha256",
            "binding_set_sha256",
            "connection_registry_sha256",
            "credential_runtime_sha256",
            "authority_subject_sha256",
        }
