"""Saved retirement scope is exact review material, never external admission."""

import json
from dataclasses import replace
from importlib import import_module

import pytest

from tests.test_mssql_publication_authority import BINDING
from tests.test_publication_retirement import plan


def codec():
    return import_module("dpone.contracts.publication_retirement_operator_plan")


def operator_plan():
    return codec().PublicationRetirementOperatorPlan(BINDING, "a" * 64, "b" * 64, "c" * 64, plan())


def test_roundtrip_keeps_inner_provenance_and_attempt_key_exact():
    original = operator_plan()
    decoded = codec().decode_retirement_operator_plan(original.payload.encode())
    assert decoded == original
    assert decoded.retirement.payload == plan().payload
    assert decoded.retirement.attempt_key == plan().attempt_key
    assert decoded.confirms(original.digest)
    assert replace(original, journal_identity="d" * 64).digest != original.digest
    assert replace(original, journal_identity="d" * 64).retirement.attempt_key == plan().attempt_key


@pytest.mark.parametrize("value", ["", "A" * 64, "é" * 64, "0" * 64, None, True])
def test_malformed_or_wrong_confirmation_is_false(value):
    assert not operator_plan().confirms(value)


@pytest.mark.parametrize("damage", ["extra", "binding", "journal", "inner", "whitespace", "duplicate"])
def test_rejects_scope_drift_and_noncanonical_bytes(damage):
    raw = operator_plan().payload.encode()
    document = json.loads(raw)
    if damage == "extra":
        document["bypass"] = True
    elif damage == "binding":
        document["binding"]["database"] = "Other_System"
    elif damage == "journal":
        document["journal_identity"] = "arbitrary"
    elif damage == "inner":
        document["retirement"] = {}
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    if damage == "whitespace":
        raw += b" "
    if damage == "duplicate":
        raw = b'{"journal_identity":"' + b"c" * 64 + b'",' + raw[1:]
    with pytest.raises(ValueError, match="^invalid retirement operator plan$"):
        codec().decode_retirement_operator_plan(raw)


@pytest.mark.parametrize(
    "raw",
    [b"", b"x" * (1024 * 1024 + 1), b"[]", b"\xff", "{}"],
    ids=["empty", "oversize", "array", "encoding", "text"],
)
def test_invalid_input_is_bounded_and_redacted(raw):
    with pytest.raises(ValueError, match="^invalid retirement operator plan$"):
        codec().decode_retirement_operator_plan(raw)
