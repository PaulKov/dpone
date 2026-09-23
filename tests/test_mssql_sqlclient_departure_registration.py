"""Closed helper setup evidence preserves originals before representation."""

import json
import math
from collections.abc import Callable
from dataclasses import replace
from typing import Any
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_departure_registration import (
    SqlClientDepartureCredentialIntent,
    SqlClientDepartureLaunchIntent,
    SqlClientDepartureRegistration,
    decode_credential_intent,
    decode_launch_intent,
    decode_registration,
    encode_credential_intent,
    encode_launch_intent,
    encode_registration,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from tests.test_mssql_sqlclient_departure_ipc import request

ERROR = r"^mssql_native\.sqlclient_departure_evidence_invalid$"


def setup_values(r=None):
    r = request() if r is None else r
    subject = r.plan.helper_id, attempt_identity_digest(r.plan.attempt)
    return (
        SqlClientDepartureLaunchIntent(r.plan),
        SqlClientDepartureRegistration(*subject, "1" * 64, r.startup, r.plan.admission_sha256),
        SqlClientDepartureCredentialIntent(*subject, "2" * 64, r),
    )


def test_setup_roundtrips():
    encoders: tuple[Callable[..., Any], ...] = (encode_launch_intent, encode_registration, encode_credential_intent)
    decoders = (decode_launch_intent, decode_registration, decode_credential_intent)
    for value, encode, decode in zip(setup_values(), encoders, decoders, strict=True):
        raw = encode(value)
        assert decode(raw) == value
        assert type(json.loads(raw)) is dict


def maximal_request(character):
    """Joint legal maxima, keeping catalog UTF-16 and attempt codepoints distinct."""
    from tests.test_mssql_sqlclient_create_departure_codec import maximum

    departure = maximum(character)
    r = request(departure)
    attempt = replace(
        r.plan.attempt,
        target_key=character * 256,
        run_id=character * 256,
        schema=character * 128,
        table=character * 128,
        ordinal=2**63 - 1,
        attempt=2,
    )
    owner = replace(r.plan.ownership, owner=character * 256, fence=2**63 - 1)
    operation = replace(r.plan.create_operation, parent=attempt, original_fence=owner.fence, slot_index=2**63 - 1)
    width = len(character.encode("utf8"))
    root = "/" + character * (4095 // width) + "x" * (4095 % width)
    deadline = math.nextafter((2**63 - 1) / 1e9, 0.0)
    process = replace(r.plan.create_process, pid=2**31 - 1, start_ticks=2**63 - 1)
    plan = replace(
        r.plan,
        attempt=attempt,
        ownership=owner,
        create_operation=operation,
        create_process=process,
        package_root=root,
        startup_deadline=deadline,
        operation_deadline=deadline,
        max_address_space_bytes=2**63 - 1,
    )
    r = replace(r, plan=plan, startup=replace(r.startup, package_root=root, process=replace(process, pid=2**31 - 2)))
    assert len(root.encode("utf8")) == 4096
    assert len(attempt.schema) == len(attempt.table) == 128
    assert len(departure.database.name.encode("utf-16le")) == 256
    return r, departure


@pytest.mark.parametrize("codec", ["launch", "registration", "credential"])
@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate"])
def test_every_setup_outer_field_is_closed(codec, mutation):
    from dpone.contracts.strict_json import canonical_json_bytes

    triples: dict[str, tuple[Any, Callable[..., Any], Callable[..., Any]]] = dict(
        zip(
            ("launch", "registration", "credential"),
            zip(
                setup_values(),
                (encode_launch_intent, encode_registration, encode_credential_intent),
                (decode_launch_intent, decode_registration, decode_credential_intent),
                strict=True,
            ),
            strict=True,
        )
    )
    value, encode, decode = triples[codec]
    raw = encode(value)
    original = json.loads(raw)
    for field in original:
        data = original.copy()
        if mutation == "missing":
            del data[field]
            payload = canonical_json_bytes(data)
        elif mutation == "extra":
            data[field + "_extra"] = "private-canary"
            payload = canonical_json_bytes(data)
        else:
            pair = canonical_json_bytes({field: data[field]})[1:-1]
            payload = raw[:-1] + b"," + pair + b"}"
        with pytest.raises(ValueError, match=ERROR):
            decode(payload)


@pytest.mark.parametrize(
    "field,alias",
    [
        ("pid", 124.0),
        ("pid", True),
        ("start_ticks", 456.0),
        ("host_sha256", b"d" * 64),
        ("boot_id", "22222222222242228222222222222222"),
    ],
)
def test_registration_rejects_original_process_aliases_before_encoding(field, alias, monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_registration as module

    value = setup_values()[1]
    object.__setattr__(value.startup.process, field, alias)
    calls = []
    monkeypatch.setattr(module, "encode_startup", lambda *args: calls.append(args))
    with pytest.raises(ValueError, match=ERROR):
        encode_registration(value)
    assert not calls


@pytest.mark.parametrize(
    "field,alias",
    [
        ("launch_nonce", bytearray(range(32))),
        ("package_root", b"/synthetic"),
        ("implementation_sha256", "E" * 64),
        ("process", {}),
    ],
)
def test_registration_revalidates_original_startup(field, alias):
    value = setup_values()[1]
    object.__setattr__(value.startup, field, alias)
    with pytest.raises(ValueError, match=ERROR):
        encode_registration(value)


@pytest.mark.parametrize(
    "path,field,alias",
    [
        (("plan",), "helper_id", "44444444-4444-4444-8444-444444444444"),
        (("plan", "attempt"), "ordinal", 0.0),
        (("plan", "ownership"), "fence", True),
        (("plan", "create_operation"), "command", "create"),
        (("plan", "create_operation"), "operation_id", "33333333-3333-4333-8333-333333333333"),
        (("plan", "original"), "session_id", 72.0),
        (("plan", "creator_admission", "login"), "is_sysadmin", 0),
        (("plan", "principal"), "principal_id", 5.0),
        (("plan",), "startup_deadline", 10),
    ],
)
@pytest.mark.parametrize("which", [0, 2])
def test_setup_forged_nested_original_never_normalized(path, field, alias, which):
    values = setup_values()
    nested = values[0] if which == 0 else values[2].request
    for key in path:
        nested = getattr(nested, key)
    object.__setattr__(nested, field, alias)
    with pytest.raises(ValueError, match=ERROR):
        (encode_launch_intent if which == 0 else encode_credential_intent)(values[which])


@pytest.mark.parametrize("field,value", [("helper_id", UUID(int=7)), ("attempt_sha256", "9" * 64)])
def test_credential_subject_matches_original_request(field, value):
    original = setup_values()[2]
    with pytest.raises(ValueError, match=ERROR):
        replace(original, **{field: value})
    object.__setattr__(original, field, value)
    with pytest.raises(ValueError, match=ERROR):
        encode_credential_intent(original)
