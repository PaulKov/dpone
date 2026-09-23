"""Worker results cannot substitute another launch, input or bulk permission."""

import json
from hashlib import sha256

import pytest

from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_sqlclient_launch import launch_digest
from dpone.contracts.mssql_sqlclient_result import SqlClientResult, decode_sqlclient_result, encode_sqlclient_result
from dpone.contracts.mssql_tds_result import TdsWorkerResult
from dpone.contracts.mssql_tds_worker import TdsAttemptError
from tests.test_mssql_sqlclient_launch_contract import launch

GRANT = "44444444-4444-4444-8444-444444444444"
INPUT = TdsInputReceipt(1, 8, "a" * 64)
EMPTY = TdsInputReceipt(0, 0, sha256(b"").hexdigest())


def body(receipt=INPUT, grant_id=GRANT, error=None):
    value = launch()
    return encode_sqlclient_result(
        SqlClientResult(
            1,
            launch_digest(value),
            value.attempt_sha256,
            grant_id,
            TdsWorkerResult(value.attempt_sha256, receipt, error),
        )
    )


def decode(raw, expected=INPUT, grant_id=GRANT):
    return decode_sqlclient_result(raw, launch=launch(), expected_input=expected, expected_grant_id=grant_id)


def test_success_matches_original_grant_and_input():
    assert decode(body()).result.receipt == INPUT
    with pytest.raises(ValueError):
        decode(body(), grant_id=None)
    with pytest.raises(ValueError):
        decode(body(), expected=TdsInputReceipt(2, 16, "b" * 64))


def test_empty_success_needs_canonical_empty_input_and_no_grant():
    assert decode(body(EMPTY, None), EMPTY, None).result.receipt == EMPTY
    with pytest.raises(ValueError):
        decode(body(EMPTY, None), EMPTY, GRANT)
    with pytest.raises(ValueError):
        decode(body(TdsInputReceipt(0, 0, "a" * 64), None), TdsInputReceipt(0, 0, "a" * 64), None)


@pytest.mark.parametrize("accepted", [None, GRANT])
def test_error_after_possible_grant_delivery_retains_known_acceptance(accepted):
    result = decode(body(None, accepted, TdsAttemptError.DRIVER))
    assert result.result.receipt is None
    assert result.grant_id == accepted
    if accepted:
        with pytest.raises(ValueError):
            decode(body(None, accepted, TdsAttemptError.DRIVER), grant_id=None)


def test_pregrant_error_is_valid_without_grant_or_receipt():
    assert (
        decode(body(None, None, TdsAttemptError.CONNECTION), grant_id=None).result.error is TdsAttemptError.CONNECTION
    )


@pytest.mark.parametrize(
    "mutation",
    ["launch", "attempt", "nested_attempt", "grant", "extra", "nested_extra", "bool", "duplicate", "trailing"],
)
def test_forged_or_ambiguous_result_rejects_without_echo(mutation):
    raw = body()
    data = json.loads(raw)
    if mutation == "duplicate":
        raw = b'{"schema_version":1,' + raw[1:]
    elif mutation == "trailing":
        raw += b"{}"
    else:
        if mutation == "launch":
            data["launch_sha256"] = "b" * 64
        if mutation == "attempt":
            data["attempt_sha256"] = "b" * 64
        if mutation == "nested_attempt":
            data["result"]["attempt_sha256"] = "b" * 64
        if mutation == "grant":
            data["grant_id"] = "55555555-5555-4555-8555-555555555555"
        if mutation == "extra":
            data["private-canary"] = "private-canary"
        if mutation == "nested_extra":
            data["result"]["private-canary"] = 1
        if mutation == "bool":
            data["schema_version"] = True
        raw = json.dumps(data).encode()
    with pytest.raises(ValueError) as caught:
        decode(raw)
    assert str(caught.value) == "mssql_native.sqlclient_result_invalid"


def test_oversized_result_is_rejected_before_parsing(monkeypatch):
    import dpone.contracts.mssql_sqlclient_result as codec

    def forbidden(*args):
        pytest.fail("oversized result reached JSON parser")

    monkeypatch.setattr(codec, "strict_json_object", forbidden)
    with pytest.raises(ValueError):
        decode(b"x" * 16385)


@pytest.mark.parametrize("value", [True, "", "00000000-0000-0000-0000-000000000000"])
def test_invalid_expected_grant_is_not_treated_as_no_grant(value):
    with pytest.raises(ValueError):
        decode(body(None, None, TdsAttemptError.DRIVER), grant_id=value)


@pytest.mark.parametrize("name", ["success", "empty", "pregrant-error", "granted-error"])
def test_independent_wire_vectors(name):
    from pathlib import Path

    path = Path(__file__).parent / "fixtures" / "mssql_sqlclient" / f"result-{name}-v1.json"
    raw = path.read_bytes().rstrip(b"\n")
    value = decode(raw, EMPTY if name == "empty" else INPUT, None if name in ("empty", "pregrant-error") else GRANT)
    assert encode_sqlclient_result(value) == raw
