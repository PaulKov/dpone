"""Canonical wire rejection tests; synthetic registration grants no authority."""

import hashlib
import json

import pytest

from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import (
    decode_physical_runtime_registration,
    encode_physical_runtime_registration,
    physical_runtime_registration_digest,
)
from dpone.contracts.dbt_mssql_physical_registration_values import PhysicalRegistrationError
from tests.support.dbt_mssql_physical_registration import registration_document, registration_inputs


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()


def test_complete_canonical_roundtrip_and_external_digest():
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    expected = canonical(registration_document())
    assert hashlib.sha256(expected).hexdigest() == "a01bc11d3c97354b18e818a81e593d19e1dda6fb8a1dcf54827d31a5acd20c57"
    assert encode_physical_runtime_registration(registration) == expected
    assert decode_physical_runtime_registration(expected) == registration
    assert physical_runtime_registration_digest(registration) == "sha256:" + hashlib.sha256(expected).hexdigest()


@pytest.mark.parametrize("key", ["schema", *registration_inputs()])
def test_every_top_level_field_is_required(key):
    document = MssqlPhysicalRuntimeRegistration(**registration_inputs()).to_dict()
    del document[key]
    with pytest.raises(PhysicalRegistrationError):
        decode_physical_runtime_registration(canonical(document))


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("program",),
        ("limits",),
        ("principals",),
        ("principals", "metadata"),
        ("principals", "metadata", "control"),
        ("principals", "observer"),
        ("trusted_profile",),
        ("control_database",),
        ("capacity_authority",),
    ],
)
def test_closed_nested_records_reject_unknown_fields(path):
    document = MssqlPhysicalRuntimeRegistration(**registration_inputs()).to_dict()
    node = document
    for key in path:
        node = node[key]
    node["unexpected"] = True
    with pytest.raises(PhysicalRegistrationError):
        decode_physical_runtime_registration(canonical(document))


@pytest.mark.parametrize(
    "transform", [lambda b: b + b"\n", lambda b: b" " + b, lambda b: b.replace(b'"schema":', b'"schema": ', 1)]
)
def test_alternate_json_bytes_are_not_canonical(transform):
    payload = canonical(MssqlPhysicalRuntimeRegistration(**registration_inputs()).to_dict())
    with pytest.raises(PhysicalRegistrationError):
        decode_physical_runtime_registration(transform(payload))


def record_paths(record, prefix=()):
    for key, value in record.items():
        path = (*prefix, key)
        yield path, value
        if type(value) is dict:
            yield from record_paths(value, path)


def parent_at(document, path):
    node = document
    for key in path[:-1]:
        node = node[key]
    return node


@pytest.mark.parametrize("path,value", list(record_paths(registration_document())))
def test_every_nested_field_is_required_and_rejects_wrong_types(path, value):
    document = registration_document()
    parent = parent_at(document, path)
    del parent[path[-1]]
    with pytest.raises(PhysicalRegistrationError):
        decode_physical_runtime_registration(canonical(document))
    for invalid in (None, True, [], 1 if type(value) is not int else "1"):
        parent[path[-1]] = invalid
        with pytest.raises(PhysicalRegistrationError):
            decode_physical_runtime_registration(canonical(document))


@pytest.mark.parametrize("mode", ["SHARE_METADATA", "SHARE_BUILD"])
def test_shared_observer_roundtrip_is_explicit_and_digest_sensitive(mode):
    document = registration_document()
    document["principals"]["observer"] = {"mode": mode, "permission_contract_sha256": "sha256:" + "b" * 64}
    registration = decode_physical_runtime_registration(canonical(document))
    assert encode_physical_runtime_registration(registration) == canonical(document)
    before = physical_runtime_registration_digest(registration)
    document["principals"]["observer"]["permission_contract_sha256"] = "sha256:" + "c" * 64
    assert physical_runtime_registration_digest(decode_physical_runtime_registration(canonical(document))) != before
    document["principals"]["observer"]["mapping"] = document["principals"]["build"]
    with pytest.raises(PhysicalRegistrationError):
        decode_physical_runtime_registration(canonical(document))


@pytest.mark.parametrize(
    "path,value",
    [
        (("registration_id",), "20000000-0000-0000-0000-000000000002"),
        (("capacity_authority", "locator"), "other/capacity"),
        (("trusted_profile", "reference", "locator"), "other/profile"),
        (("trusted_toolchain", "reference", "locator"), "other/toolchain"),
        (("program", "package_bundle_sha256"), "sha256:" + "b" * 64),
        (("limits", "max_definition_utf16_bytes"), 2147483647),
        (("control_schema",), "another_control"),
        (("local_schema",), "another_local"),
    ],
)
def test_external_digest_covers_changed_complete_document(path, value):
    document = registration_document()
    before = physical_runtime_registration_digest(decode_physical_runtime_registration(canonical(document)))
    parent_at(document, path)[path[-1]] = value
    result = decode_physical_runtime_registration(canonical(document))
    after = physical_runtime_registration_digest(result)
    assert after != before
    assert after == "sha256:" + hashlib.sha256(canonical(document)).hexdigest()


@pytest.mark.parametrize(
    "payload", [b"{}", b"null", b"[]", b'{"a":1,"a":2}', b"\xef\xbb\xbf{}", b"\xff", bytearray(b"{}"), "{}", None]
)
def test_bad_transport_is_normalized_to_input_free_error(payload):
    with pytest.raises(PhysicalRegistrationError, match="^invalid canonical physical registration$"):
        decode_physical_runtime_registration(payload)


@pytest.mark.parametrize("scope", ["GENERATION", "DELIVERY"])
@pytest.mark.parametrize(
    "path", [("platform_subject",), ("trusted_profile", "subject"), ("trusted_toolchain", "subject")]
)
def test_valid_nonplatform_subjects_are_not_substitutes(scope, path):
    document = registration_document()
    subject = parent_at(document, path)[path[-1]]
    subject.pop("platform_policy_sha256")
    subject["scope"] = scope
    if scope == "GENERATION":
        subject["generation_id"] = "30000000-0000-0000-0000-000000000001"
    else:
        subject.update(operation_id="sha256:" + "5" * 64, attempt_id="sha256:" + "6" * 64)
    with pytest.raises(PhysicalRegistrationError):
        decode_physical_runtime_registration(canonical(document))


@pytest.mark.parametrize(
    "path,value",
    [
        (("registration_id",), "20000000000000000000000000000001"),
        (("registration_id",), "ABCDEF00-0000-0000-0000-000000000001"),
        (("control_database", "create_token"), "2023-02-29T12:00:00.1234567"),
        (("control_database", "create_token"), "2024-02-29T12:00:00.123456"),
        (("control_database", "create_token"), "2024-02-29T24:00:00.1234567"),
        (("control_database", "database_guid"), "{10000000-0000-0000-0000-000000000001}"),
        (("control_database", "database_name"), "😀" * 65),
        (("control_database", "database_name"), "example\n"),
        (("control_schema",), "x" * 129),
        (("local_schema",), "quoted-name"),
        (("service_authority_sha256",), "sha256:" + "A" * 64),
        (("capacity_authority", "locator"), "../outside"),
        (("trusted_profile", "reference", "locator"), "/absolute"),
        (("program", "macro_authority_sha256"), "sha256:" + "a" * 63),
    ],
)
def test_invalid_scalar_boundaries_reject_through_public_decoder(path, value):
    document = registration_document()
    parent_at(document, path)[path[-1]] = value
    with pytest.raises(PhysicalRegistrationError):
        decode_physical_runtime_registration(canonical(document))


def test_maximum_sql_identifier_width_and_definition_budget_roundtrip():
    document = registration_document()
    for name in ("control_database", "model_database"):
        document[name]["database_name"] = "😀" * 64
    document["control_schema"] = "x" * 128
    document["limits"]["max_definition_utf16_bytes"] = 2147483647
    document["limits"]["max_metadata_bytes"] = 1
    result = decode_physical_runtime_registration(canonical(document))
    assert encode_physical_runtime_registration(result) == canonical(document)
