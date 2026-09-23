from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts.mssql_sqlclient_restricted_writer_verify import verify_request_digest
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_codec import (
    decode_probe_authorization,
    decode_verify_opening,
    decode_verify_request,
    decode_verify_result,
    encode_probe_authorization,
    encode_verify_opening,
    encode_verify_request,
    encode_verify_result,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_handshake import RestrictedWriterVerifyOpening
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_request, verify_result


def test_request_and_result_round_trip_canonically():
    request, result = verify_request(), verify_result()
    assert decode_verify_request(encode_verify_request(request)) == request
    assert decode_verify_result(encode_verify_result(result)) == result


def test_opening_and_exact_bound_authorization_round_trip():
    request, result = verify_request(), verify_result()
    opening = RestrictedWriterVerifyOpening(verify_request_digest(encode_verify_request(request)), result.opening)
    payload = encode_verify_opening(opening)
    authorization = encode_probe_authorization(payload)
    assert decode_verify_opening(payload) == opening
    assert (
        decode_probe_authorization(authorization, opening_payload=payload).opening_sha256 == sha256(payload).hexdigest()
    )
    with pytest.raises(ValueError):
        decode_probe_authorization(
            authorization, opening_payload=encode_verify_opening(replace(opening, request_sha256="f" * 64))
        )


@pytest.mark.parametrize("payload", (b"", b"{}", b'{"schema":"wrong"}', b'{"schema":"wrong"} '))
def test_malformed_or_noncanonical_payload_is_rejected(payload):
    with pytest.raises(ValueError):
        decode_verify_request(payload)
    with pytest.raises(ValueError):
        decode_verify_result(payload)


def test_nested_type_substitution_is_rejected():
    class PermissionAlias(type(verify_request().server_permissions[0])):
        pass

    request = verify_request()
    with pytest.raises(ValueError):
        replace(request, server_permissions=(PermissionAlias(None, None, "CONNECT SQL"),))


def test_result_wire_and_repr_never_retain_raw_catalog_or_principal_names():
    request, result = verify_request(), verify_result()
    encoded = encode_verify_result(result)
    names = {
        request.writer.name,
        request.writer_login.name,
        request.stage.database_name,
        request.stage.schema_name,
        request.stage.table_name,
        *(column.name for column in request.stage.columns),
        *(token.name for token in request.login_token + request.user_token),
    }
    leaves = []
    pending = [strict_json_object(encoded)]
    while pending:
        value = pending.pop()
        if type(value) is dict:
            pending.extend(value.values())
        elif type(value) is list:
            pending.extend(value)
        elif type(value) is str:
            leaves.append(value)
    assert names.isdisjoint(leaves)
    for forbidden_key in (
        b'"login_name":',
        b'"original_login_name":',
        b'"database_name":',
        b'"user_name":',
        b'"name":',
        b'"stage":',
        b'"closing_stage":',
    ):
        assert forbidden_key not in encoded


def test_result_decoder_rejects_legacy_raw_name_key():
    body = strict_json_object(encode_verify_result(verify_result()))
    opening = body["opening"]
    opening["login_name"] = "writer"
    del opening["login_name_sha256"]
    with pytest.raises(ValueError):
        decode_verify_result(canonical_json_bytes(body))
