"""Untrusted coordinator startup bytes cannot redefine admitted launch inputs."""

import json
from dataclasses import asdict, replace
from hashlib import sha256

import pytest

from dpone.contracts.mssql_tds_coordinator_ipc import (
    TdsCoordinatorStartup,
    decode_startup,
    encode_registration,
    encode_startup,
    registration_digest,
)
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

PROCESS = TdsProcessIdentity("a" * 64, "00000000-0000-0000-0000-000000000001", 123, 456)


def receipt():
    return TdsCoordinatorStartup(PROCESS, "b" * 64, "/admitted/source", b"\x01" + b"\x00" * 31)


def test_fixed_shape_preserves_trailing_zero_and_does_not_invent_authentication():
    startup = receipt()
    expected = {
        "schema": "dpone.tds.coordinator-startup.v1",
        "process": asdict(PROCESS),
        "implementation_sha256": "b" * 64,
        "package_root": "/admitted/source",
        "launch_nonce": "01" + "00" * 31,
    }
    assert strict_json_object(encode_startup(startup)) == expected
    assert decode_startup(canonical_json_bytes(expected)) == startup
    # An otherwise valid different source is only a structural receipt. The
    # owning launcher must compare it with its independently admitted build.
    expected["implementation_sha256"] = "c" * 64
    assert decode_startup(canonical_json_bytes(expected)).implementation_sha256 == "c" * 64


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", 1),
        ("schema", "dpone.tds.worker-startup.v1"),
        ("implementation_sha256", "B" * 64),
        ("implementation_sha256", True),
        ("package_root", "relative"),
        ("package_root", "/bad\x00root"),
        ("package_root", "/" + "x" * 4096),
        ("package_root", "/bad\ud800"),
        ("launch_nonce", "00" * 32),
        ("launch_nonce", "01"),
        ("launch_nonce", "AA" * 32),
        ("launch_nonce", "01 " * 32),
        ("process", None),
        ("unexpected", "secret"),
    ],
)
def test_malformed_or_noncanonical_fields_fail_with_static_diagnostic(field, value):
    body = strict_json_object(encode_startup(receipt()))
    body[field] = value
    with pytest.raises(ValueError, match="^mssql_native.tds_coordinator_startup_invalid$"):
        decode_startup(json.dumps(body, ensure_ascii=True).encode("ascii"))


@pytest.mark.parametrize("field,value", [("pid", True), ("start_ticks", -1), ("extra", 1)])
def test_process_identity_has_closed_typed_fields(field, value):
    body = strict_json_object(encode_startup(receipt()))
    body["process"][field] = value
    with pytest.raises(ValueError, match="coordinator_startup_invalid"):
        decode_startup(canonical_json_bytes(body))


@pytest.mark.parametrize("payload", [b"[]", b"{}", b"{", b" " * 16385, b"\xff"])
def test_invalid_envelope_is_rejected(payload):
    with pytest.raises(ValueError, match="coordinator_startup_invalid"):
        decode_startup(payload)


def test_duplicate_fields_rejected_even_when_values_match():
    body = encode_startup(receipt())
    body = b'{"schema":"dpone.tds.coordinator-startup.v1",' + body[1:]
    with pytest.raises(ValueError, match="coordinator_startup_invalid"):
        decode_startup(body)


def test_registration_persists_complete_startup_and_build_binding():
    startup = receipt()
    body = encode_registration(startup, "c" * 64)
    assert strict_json_object(body) == {
        "schema": "dpone.tds.coordinator-registration.v1",
        "startup": strict_json_object(encode_startup(startup)),
        "admission_sha256": "c" * 64,
    }
    assert registration_digest(startup, "c" * 64) == sha256(body).hexdigest()
    assert registration_digest(startup, "d" * 64) != registration_digest(startup, "c" * 64)


@pytest.mark.parametrize(
    "change",
    [
        {"process": replace(PROCESS, pid=124)},
        {"implementation_sha256": "d" * 64},
        {"package_root": "/other/admitted/source"},
        {"launch_nonce": b"\x02" + b"\x00" * 31},
    ],
)
def test_registration_binds_every_startup_observation(change):
    original = receipt()
    assert registration_digest(replace(original, **change), "c" * 64) != registration_digest(original, "c" * 64)


@pytest.mark.parametrize("digest", [None, True, "C" * 64, "c" * 63])
def test_registration_rejects_invalid_admission_digest(digest):
    with pytest.raises(ValueError):
        encode_registration(receipt(), digest)
