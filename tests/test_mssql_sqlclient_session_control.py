"""Reject forged control bindings before a SqlClient bulk grant can be used."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_launch import launch_digest
from dpone.contracts.mssql_sqlclient_session_control import (
    SqlClientBulkGrant,
    SqlClientDatabasePrincipal,
    SqlClientSessionAnnouncement,
    decode_bulk_grant,
    decode_session_announcement,
    encode_bulk_grant,
    encode_session_announcement,
    validate_bulk_grant,
    validate_session_announcement,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptOwnership, TdsObjectIdentity
from tests.test_mssql_sqlclient_launch_contract import launch
from tests.test_mssql_tds_session_codec import IDENTITY

OWNER = TdsAttemptOwnership("owner", 1, "33333333-3333-4333-8333-333333333333")
OBJECT = TdsObjectIdentity(123, "e" * 64)
PROOF = "f" * 64
PRINCIPAL = SqlClientDatabasePrincipal(7, "writer", "cd" * 16)


def grant():
    value = launch()
    return SqlClientBulkGrant(
        1,
        "44444444-4444-4444-8444-444444444444",
        launch_digest(value),
        value.attempt_sha256,
        OWNER,
        value.process,
        OBJECT,
        value.input_binding_sha256,
        value.build_sha256,
        IDENTITY,
        PROOF,
        value.operation_deadline_ns,
        PRINCIPAL,
    )


def validate(value, **changes):
    expected = dict(
        launch=launch(),
        ownership=OWNER,
        object_identity=OBJECT,
        remote_session=IDENTITY,
        writer_observation_sha256=PROOF,
        resolved_database_principal=PRINCIPAL,
        now_ns=1,
    )
    expected.update(changes)
    validate_bulk_grant(value, **expected)


def test_valid_bound_grant_and_announcement_roundtrip():
    value = grant()
    assert decode_bulk_grant(encode_bulk_grant(value)) == value
    validate(value)
    announcement = SqlClientSessionAnnouncement(
        1, value.launch_sha256, value.attempt_sha256, IDENTITY.session_id, IDENTITY.nonce.hex()
    )
    assert decode_session_announcement(encode_session_announcement(announcement)) == announcement
    validate_session_announcement(announcement, launch=launch(), nonce=IDENTITY.nonce, now_ns=1)


@pytest.mark.parametrize(
    "changes",
    [
        {"launch_sha256": "0" * 64},
        {"attempt_sha256": "0" * 64},
        {"input_binding_sha256": "0" * 64},
        {"build_sha256": "0" * 64},
        {"ownership": replace(OWNER, fence=2)},
        {"object_identity": replace(OBJECT, object_id=124)},
        {"process": replace(launch().process, start_ticks=457)},
        {"remote_session": replace(IDENTITY, session_id=73)},
        {"writer_observation_sha256": "0" * 64},
        {"operation_deadline_ns": 30_000_000_000},
    ],
)
def test_grant_rejects_every_original_binding_drift(changes):
    with pytest.raises(ValueError):
        validate(replace(grant(), **changes))


@pytest.mark.parametrize("now", [True, -1, 20_000_000_000, 20_000_000_001])
def test_deadline_is_original_strict_integer_and_not_renewed(now):
    with pytest.raises(ValueError):
        validate(grant(), now_ns=now)


@pytest.mark.parametrize(
    "mutation", ["extra", "missing", "duplicate", "trailing", "nested_extra", "bool", "uuid_alias"]
)
def test_closed_grant_rejects_ambiguous_bytes(mutation):
    raw = encode_bulk_grant(grant())
    data = json.loads(raw)
    if mutation == "duplicate":
        raw = b'{"schema_version":1,' + raw[1:]
    elif mutation == "trailing":
        raw += b"{}"
    else:
        if mutation == "extra":
            data["secret-canary"] = "secret-canary"
        if mutation == "missing":
            del data["build_sha256"]
        if mutation == "nested_extra":
            data["ownership"]["secret-canary"] = 1
        if mutation == "bool":
            data["operation_deadline_ns"] = True
        if mutation == "uuid_alias":
            data["grant_id"] = data["grant_id"].replace("-", "")
        raw = json.dumps(data).encode()
    with pytest.raises(ValueError) as caught:
        decode_bulk_grant(raw)
    assert str(caught.value) == "mssql_native.sqlclient_session_control_invalid"


def test_oversize_rejected_before_json_parser(monkeypatch):
    import dpone.contracts.mssql_sqlclient_session_control as module

    def forbidden(*args):
        pytest.fail("oversize bytes parsed")

    monkeypatch.setattr(module, "strict_json_object", forbidden)
    with pytest.raises(ValueError):
        decode_bulk_grant(b"x" * 16385)


def test_announcement_mismatch_is_not_writer_observation():
    value = SqlClientSessionAnnouncement(1, launch_digest(launch()), launch().attempt_sha256, 72, "ab" * 32)
    with pytest.raises(ValueError):
        validate_session_announcement(value, launch=launch(), nonce=IDENTITY.nonce, now_ns=1)


def test_independent_unicode_and_i64_vector_has_exact_canonical_digest():
    from pathlib import Path

    from dpone.contracts.mssql_sqlclient_session_control import bulk_grant_digest

    root = Path(__file__).parent / "fixtures" / "mssql_sqlclient"
    body = (root / "bulk-grant-principal-v1.json").read_bytes().rstrip(b"\n")
    parsed = decode_bulk_grant(body)
    assert parsed.ownership.owner == "владелец 🧪"
    assert parsed.ownership.fence == 2**63 - 1
    assert encode_bulk_grant(parsed) == body
    assert bulk_grant_digest(parsed) == (root / "bulk-grant-principal-v1.sha256").read_text().strip()
    body = (root / "session-announcement-v1.json").read_bytes().rstrip(b"\n")
    assert encode_session_announcement(decode_session_announcement(body)) == body
