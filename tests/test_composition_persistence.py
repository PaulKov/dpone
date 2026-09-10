"""Protected database documents cannot weaken immutable parent authority."""

import json

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import decode_activation_request, encode_activation_request
from tests.test_composition_activation_contract import digest, request


def test_roundtrip_retains_complete_parent_and_original_catalog_subject():
    value = request()
    assert decode_activation_request(encode_activation_request(value), value.request_sha256) == value


@pytest.mark.parametrize("change", ["foreign_digest", "unknown_field", "missing_workload", "changed_catalog"])
def test_protected_readback_rejects_tampered_documents(change):
    value = request()
    body = json.loads(encode_activation_request(value))
    expected = value.request_sha256
    if change == "foreign_digest":
        expected = digest("another request")
    elif change == "unknown_field":
        body["context"]["authority_override"] = True
    elif change == "missing_workload":
        body["workloads"].pop()
    else:
        body["resources"][0]["observation_sha256"] = digest("new catalog")
    with pytest.raises(CompositionAdmissionError):
        decode_activation_request(json.dumps(body).encode(), expected)


def test_duplicate_json_member_is_not_an_alternate_authority_encoding():
    value = request()
    body = encode_activation_request(value)
    duplicate = body[:-1] + b',"schema":"dpone.composition-activation-request.v1"}'
    with pytest.raises(CompositionAdmissionError):
        decode_activation_request(duplicate, value.request_sha256)


@pytest.mark.parametrize("body", [b"[]", b"null", b"{", b" " * (8 * 1024 * 1024 + 1)])
def test_malformed_or_unbounded_database_document_fails_safely(body):
    with pytest.raises(CompositionAdmissionError):
        decode_activation_request(body, digest("request"))
