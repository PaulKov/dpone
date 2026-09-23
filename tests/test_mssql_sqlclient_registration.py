"""Registration and intent are strict nonsecret records, not proof of admission."""

from dataclasses import asdict, replace

import pytest

from dpone.contracts.mssql_sqlclient_registration import (
    build_credential_intent,
    decode_credential_intent,
    decode_registration,
    encode_credential_intent,
    encode_registration,
    validate_credential_intent,
)
from dpone.contracts.strict_json import canonical_json_bytes
from tests.mssql_sqlclient_evidence_fixtures import intent, registration


def test_roundtrip_and_original_intent():
    r = registration()
    c = intent(r)
    assert decode_registration(encode_registration(r)) == r
    assert decode_credential_intent(encode_credential_intent(c)) == c
    validate_credential_intent(c, r)


def test_secret_free_intent_builder_matches_existing_job_projection():
    r = registration()
    built = build_credential_intent(
        r,
        session_nonce=bytes.fromhex("ab" * 32),
        tls_profile="verified",
        capability_evidence_sha256="c" * 64,
    )

    validate_credential_intent(built, r)
    assert built.capability_evidence_sha256 == "c" * 64
    assert "password" not in encode_credential_intent(built).decode()


@pytest.mark.parametrize(
    "field,value",
    [
        ("batch_rows", 1),
        ("batch_rows", 65536),
        ("max_input_batch_bytes", 1 << 20),
        ("max_input_batch_bytes", 256 << 20),
        ("input_mode", "arrow"),
    ],
)
def test_valid_boundaries(field, value):
    r = replace(registration(), **{field: value})
    assert decode_registration(encode_registration(r)) == r
    validate_credential_intent(intent(r), r)


@pytest.mark.parametrize(
    "field,value",
    [
        ("batch_rows", True),
        ("batch_rows", 0),
        ("batch_rows", 65537),
        ("max_input_batch_bytes", (1 << 20) - 1),
        ("max_input_batch_bytes", (256 << 20) + 1),
        ("input_mode", "ROWS"),
    ],
)
def test_invalid_boundaries(field, value):
    with pytest.raises(ValueError):
        replace(registration(), **{field: value})


@pytest.mark.parametrize("nested", ["binding", "launch", "ready", "input"])
def test_closed_nested_shapes(nested):
    data = asdict(registration())
    data[nested]["canary-secret"] = "canary-secret"
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_evidence_invalid$"):
        decode_registration(canonical_json_bytes(data))


@pytest.mark.parametrize("field", ["registration_sha256", "job_binding_sha256"])
def test_syntactic_reference_is_not_ack_or_job_authority(field):
    c = replace(intent(), **{field: "1" * 64})
    assert decode_credential_intent(encode_credential_intent(c)) == c
    with pytest.raises(ValueError):
        validate_credential_intent(c, registration())


@pytest.mark.parametrize("nonce", [None, "00" * 32, "AB" * 32, "ab" * 31, True])
def test_invalid_nonce(nonce):
    with pytest.raises(ValueError):
        replace(intent(), session_nonce=nonce)


def test_fd_and_ready_drift():
    r = registration()
    with pytest.raises(ValueError):
        replace(r, input=replace(r.input, fd=9))
    with pytest.raises(ValueError):
        replace(r, ready=replace(r.ready, launch_sha256="a" * 64))
    object.__setattr__(r.input.expected, "rows", True)
    with pytest.raises(ValueError):
        encode_registration(r)


def test_empty_intent_and_nonsecret_job_projection_parity():
    from hashlib import sha256

    from dpone.contracts.mssql_native_chunks import TdsInputReceipt
    from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest
    from dpone.contracts.mssql_sqlclient_job import SqlClientJob, job_binding_digest
    from dpone.contracts.mssql_sqlclient_launch import launch_digest
    from dpone.contracts.mssql_tds_result import attempt_identity_digest
    from tests.test_mssql_sqlclient_launch_contract import ready

    r = registration()
    empty = TdsInputReceipt(0, 0, sha256(b"").hexdigest())
    source = replace(r.input, expected=empty, file_identity=replace(r.input.file_identity, size=0))
    identity = replace(r.binding.identity, file_sha256=empty.file_sha256)
    started = replace(
        r.launch, attempt_sha256=attempt_identity_digest(identity), input_binding_sha256=input_descriptor_digest(source)
    )
    binding = replace(
        r.binding,
        identity=identity,
        attempt_sha256=started.attempt_sha256,
        launch_sha256=launch_digest(started),
        input_binding_sha256=started.input_binding_sha256,
    )
    r = replace(r, binding=binding, launch=started, ready=ready(started), input=source)
    c = intent(r)
    validate_credential_intent(c, r)
    # The empty Job contains no credential object; parity exercises the existing
    # producer rather than using only a test's mirrored projection algorithm.
    job = SqlClientJob(
        1,
        binding.launch_sha256,
        identity,
        binding.ownership,
        binding.object_identity,
        source,
        r.input_mode,
        r.batch_rows,
        r.max_input_batch_bytes,
        None,
        None,
    )
    assert c.job_binding_sha256 == job_binding_digest(job)
    for changes in ({"session_nonce": "ab" * 32}, {"tls_profile": "verified"}, {"input_empty": 1}):
        with pytest.raises(ValueError):
            replace(c, **changes)
