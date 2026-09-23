"""Job admission rejects drift before worker effects; all inputs are synthetic."""

import json
from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, TdsInputReceipt
from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest
from dpone.contracts.mssql_sqlclient_job import SqlClientJob, decode_job, encode_job, job_binding_digest, validate_job
from dpone.contracts.mssql_sqlclient_launch import launch_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from tests.test_mssql_sqlclient_credentials import sample
from tests.test_mssql_sqlclient_input_descriptor import descriptor
from tests.test_mssql_sqlclient_launch_contract import launch
from tests.test_mssql_sqlclient_session_control import OBJECT, OWNER
from tests.test_mssql_tds_lifecycle import identity


def job():
    source = descriptor()
    attempt = identity(file_sha256=source.expected.file_sha256)
    started = replace(
        launch(), attempt_sha256=attempt_identity_digest(attempt), input_binding_sha256=input_descriptor_digest(source)
    )
    value = SqlClientJob(
        1, launch_digest(started), attempt, OWNER, OBJECT, source, "rows", 65536, 64 << 20, sample(), "ab" * 32
    )
    return started, value


def validate(started, value, **changes):
    expected = dict(
        launch=started,
        ownership=OWNER,
        object_identity=OBJECT,
        policy=NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30),
        session_nonce="ab" * 32,
        tls_profile="verified",
        allow_disposable_test=False,
        now_ns=1,
    )
    expected.update(changes)
    validate_job(value, **expected)


def test_roundtrip_binding_and_no_secret_repr():
    started, value = job()
    assert decode_job(encode_job(value)) == value
    validate(started, value)
    assert "synthetic" not in repr(value)
    assert job_binding_digest(value) == job_binding_digest(
        replace(value, credentials=replace(sample(), password="another-secret"))
    )
    projection = json.loads(encode_job(value))
    del projection["credentials"]
    projection["credentials_present"] = True
    expected = json.dumps(projection, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    assert job_binding_digest(value) == sha256(b"dpone.sqlclient.job-binding.v1\0" + expected).hexdigest()


@pytest.mark.parametrize(
    "field,value",
    [
        ("launch_sha256", "f" * 64),
        ("ownership", replace(OWNER, fence=2)),
        ("object_identity", replace(OBJECT, object_id=124)),
        ("input_mode", "arrow"),
        ("batch_rows", 1),
        ("max_input_batch_bytes", 1 << 20),
        ("session_nonce", "cd" * 32),
    ],
)
def test_original_job_binding_drift(field, value):
    started, original = job()
    with pytest.raises(ValueError):
        validate(started, replace(original, **{field: value}))


@pytest.mark.parametrize("now", [True, -1, 20_000_000_000, 20_000_000_001])
def test_original_deadline(now):
    started, value = job()
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_job_invalid$"):
        validate(started, value, now_ns=now)


def test_empty_has_no_credentials_or_nonce():
    _, value = job()
    receipt = TdsInputReceipt(0, 0, sha256(b"").hexdigest())
    source = replace(value.input, expected=receipt, file_identity=replace(value.input.file_identity, size=0))
    attempt = replace(value.identity, file_sha256=receipt.file_sha256)
    empty = replace(value, input=source, identity=attempt, credentials=None, session_nonce=None)
    assert decode_job(encode_job(empty)) == empty
    with pytest.raises(ValueError):
        replace(empty, credentials=sample())
    with pytest.raises(ValueError):
        replace(value, credentials=None)


def test_disposable_tls_requires_explicit_trusted_admission():
    started, value = job()
    value = replace(value, credentials=replace(sample(), tls_profile="disposable_test"))
    with pytest.raises(ValueError):
        validate(started, value, tls_profile="disposable_test")
    validate(started, value, tls_profile="disposable_test", allow_disposable_test=True)
    with pytest.raises(ValueError):
        validate(started, value, allow_disposable_test=True)


@pytest.mark.parametrize(
    "mutation", ["extra", "missing", "duplicate", "trailing", "nested_extra", "bool", "file_hash", "database"]
)
def test_closed_job_and_consistency(mutation):
    _, value = job()
    raw = encode_job(value)
    data = json.loads(raw)
    if mutation == "duplicate":
        raw = b'{"schema_version":1,' + raw[1:]
    elif mutation == "trailing":
        raw += b"{}"
    else:
        if mutation == "extra":
            data["secret-canary"] = "secret-canary"
        if mutation == "missing":
            del data["credentials"]
        if mutation == "nested_extra":
            data["credentials"]["secret-canary"] = "secret-canary"
        if mutation == "bool":
            data["batch_rows"] = True
        if mutation == "file_hash":
            data["identity"]["file_sha256"] = "f" * 64
        if mutation == "database":
            data["credentials"]["database"] = "different"
        raw = json.dumps(data).encode()
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_job_invalid$"):
        decode_job(raw)


def test_oversize_rejected_before_parser(monkeypatch):
    import dpone.contracts.mssql_sqlclient_job as module

    def forbidden(*args):
        pytest.fail("oversize parsed")

    monkeypatch.setattr(module, "strict_json_object", forbidden)
    with pytest.raises(ValueError):
        decode_job(b"x" * ((1 << 20) + 1))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("input_mode", "ROWS"),
        ("batch_rows", 0),
        ("batch_rows", 65537),
        ("max_input_batch_bytes", (1 << 20) - 1),
        ("max_input_batch_bytes", (256 << 20) + 1),
        ("session_nonce", "0" * 64),
        ("session_nonce", None),
    ],
)
def test_bad_direct_records(field, bad):
    _, value = job()
    with pytest.raises(ValueError):
        replace(value, **{field: bad})


def test_policy_must_be_original_and_not_inferred_from_job():
    started, value = job()
    for policy in [
        NativeBulkTransportPolicy("mssql_python", "rows", 8 << 30),
        NativeBulkTransportPolicy("mssql_sqlclient", "arrow", 8 << 30),
        NativeBulkTransportPolicy("mssql_sqlclient", "rows", 9 << 30),
    ]:
        with pytest.raises(ValueError):
            validate(started, value, policy=policy)
    changed = replace(started, address_space_bytes=9 << 30)
    with pytest.raises(ValueError):
        validate(changed, replace(value, launch_sha256=launch_digest(changed)))


def test_full_attempt_and_input_binding_cannot_be_replaced():
    started, value = job()
    for field, changed in [
        ("identity", replace(value.identity, table="other")),
        ("input", replace(value.input, max_row_bytes=2048)),
        ("input", replace(value.input, columns=tuple(reversed(value.input.columns)))),
    ]:
        with pytest.raises(ValueError):
            validate(started, replace(value, **{field: changed}))
    changed_input = replace(value.input, fd=9)
    changed_launch = replace(started, input_binding_sha256=input_descriptor_digest(changed_input))
    with pytest.raises(ValueError):
        validate(changed_launch, replace(value, input=changed_input, launch_sha256=launch_digest(changed_launch)))


def test_post_startup_and_last_operation_instant_are_admitted():
    started, value = job()
    validate(started, value, now_ns=started.startup_deadline_ns + 1)
    validate(started, value, now_ns=started.operation_deadline_ns - 1)


def test_arrow_and_empty_admission():
    started, value = job()
    arrow = replace(value, input_mode="arrow")
    validate(started, arrow, policy=NativeBulkTransportPolicy("mssql_sqlclient", "arrow", 8 << 30))
    receipt = TdsInputReceipt(0, 0, sha256(b"").hexdigest())
    source = replace(value.input, expected=receipt, file_identity=replace(value.input.file_identity, size=0))
    attempt = replace(value.identity, file_sha256=receipt.file_sha256)
    started = replace(
        started, attempt_sha256=attempt_identity_digest(attempt), input_binding_sha256=input_descriptor_digest(source)
    )
    empty = replace(
        value,
        launch_sha256=launch_digest(started),
        input=source,
        identity=attempt,
        credentials=None,
        session_nonce=None,
    )
    validate(started, empty, session_nonce=None, tls_profile=None)


def test_secret_projection_does_not_even_serialize_credentials(monkeypatch):
    import dpone.contracts.mssql_sqlclient_job as module
    from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials

    started, value = job()
    original = module.asdict

    def guarded(record):
        assert type(record) is not SqlClientCredentials
        assert type(record) is not SqlClientJob
        return original(record)

    monkeypatch.setattr(module, "asdict", guarded)
    before = job_binding_digest(value)
    for changes in [
        dict(host="different"),
        dict(port=1444),
        dict(username="different"),
        dict(password="different"),
        dict(tls_profile="disposable_test"),
    ]:
        assert job_binding_digest(replace(value, credentials=replace(sample(), **changes))) == before


@pytest.mark.parametrize("raw", [b"\xef\xbb\xbf{}", b"\xff", b"{}", b"[]", b"null", b""])
def test_malformed_byte_bodies(raw):
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_job_invalid$"):
        decode_job(raw)
