"""Worker transport completion is distinct from SQL and process settlement."""

import json
from dataclasses import asdict, fields, replace

import pytest

from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_tds_result import TdsResultFrame, TdsWorkerResult, attempt_identity_digest, encode_result
from dpone.contracts.mssql_tds_worker import TdsAttemptError, TdsAttemptIdentity

H = "a" * 64
F = "b" * 64
RECEIPT = TdsInputReceipt(3, 27, F)


def test_attempt_digest_is_canonical_and_binds_every_identity_field():
    identity = TdsAttemptIdentity("target", "run", 0, 0, H, H, H, H, "db", "stage", "attempt", H)
    digest = attempt_identity_digest(identity)
    reordered = dict(reversed(list(asdict(identity).items())))
    assert attempt_identity_digest(TdsAttemptIdentity(**reordered)) == digest
    for field in fields(identity):
        original = getattr(identity, field.name)
        changed = original + 1 if type(original) is int else F if original == H else original + "_other"
        assert attempt_identity_digest(replace(identity, **{field.name: changed})) != digest


def success():
    return TdsWorkerResult(H, RECEIPT, None)


def frame(value):
    payload = json.dumps(value).encode()
    return len(payload).to_bytes(4, "big") + payload


def payload():
    return json.loads(encode_result(success())[4:])


def test_every_byte_fragmentation_requires_channel_eof():
    reader = TdsResultFrame()
    for byte in encode_result(success()):
        reader.feed(bytes([byte]))
    assert reader.finish(expected_attempt_sha256=H, expected_input=RECEIPT) == success()


@pytest.mark.parametrize("cut", [0, 1, 3, 4, 10, -1])
def test_truncated_result_rejected(cut):
    reader = TdsResultFrame()
    reader.feed(encode_result(success())[:cut])
    with pytest.raises(ValueError, match="tds_result_protocol"):
        reader.finish(expected_attempt_sha256=H, expected_input=RECEIPT)


@pytest.mark.parametrize("extra", [b"x", b"\0", encode_result(success())])
def test_extra_data_poisoned_even_when_first_frame_is_valid(extra):
    reader = TdsResultFrame()
    reader.feed(encode_result(success()))
    with pytest.raises(ValueError, match="tds_result_protocol"):
        reader.feed(extra)
    with pytest.raises(ValueError, match="tds_result_protocol"):
        reader.finish(expected_attempt_sha256=H, expected_input=RECEIPT)


@pytest.mark.parametrize("size", [0, 16385, 2**32 - 1])
def test_oversize_header_fails_without_payload(size):
    with pytest.raises(ValueError, match="tds_result_protocol"):
        TdsResultFrame().feed(size.to_bytes(4, "big"))


@pytest.mark.parametrize(
    "key,value",
    [
        ("schema_version", True),
        ("schema_version", 2),
        ("extra", "secret"),
        ("input_eof", False),
        ("status", "error"),
        ("status", None),
        ("error", "driver"),
        ("attempt_sha256", F),
    ],
)
def test_closed_schema_and_identity(key, value):
    data = payload()
    data[key] = value
    reader = TdsResultFrame()
    reader.feed(frame(data))
    with pytest.raises(ValueError, match="tds_result_protocol") as error:
        reader.finish(expected_attempt_sha256=H, expected_input=RECEIPT)
    assert "secret" not in str(error.value)


@pytest.mark.parametrize(
    "key,value",
    [
        ("rows", True),
        ("rows", -1),
        ("rows", 2**63),
        ("rows", 3.0),
        ("rows", 2),
        ("encoded_bytes", 28),
        ("file_sha256", H),
        ("extra", 0),
    ],
)
def test_receipt_fidelity(key, value):
    data = payload()
    data["receipt"][key] = value
    reader = TdsResultFrame()
    reader.feed(frame(data))
    with pytest.raises(ValueError, match="tds_result_protocol"):
        reader.finish(expected_attempt_sha256=H, expected_input=RECEIPT)


@pytest.mark.parametrize("body", [b'{"schema_version":1,"schema_version":1}', b'{"x":NaN}', b"[]", b"\xff"])
def test_malformed_json(body):
    reader = TdsResultFrame()
    reader.feed(len(body).to_bytes(4, "big") + body)
    with pytest.raises(ValueError, match="tds_result_protocol"):
        reader.finish(expected_attempt_sha256=H, expected_input=RECEIPT)


def test_failure_is_bounded_enum_without_success_receipt():
    result = TdsWorkerResult(H, None, TdsAttemptError.DRIVER)
    reader = TdsResultFrame()
    reader.feed(encode_result(result))
    assert reader.finish(expected_attempt_sha256=H, expected_input=RECEIPT) == result
    with pytest.raises(ValueError):
        TdsWorkerResult(H, RECEIPT, TdsAttemptError.DRIVER)
    with pytest.raises(ValueError):
        TdsWorkerResult(H, None, None)


def test_finish_seals_channel_against_further_bytes():
    reader = TdsResultFrame()
    reader.feed(encode_result(success()))
    reader.finish(expected_attempt_sha256=H, expected_input=RECEIPT)
    with pytest.raises(ValueError, match="tds_result_protocol"):
        reader.feed(b"x")
