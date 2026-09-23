"""SQL authority receipt is exact and cannot substitute a caller lock resource."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_coordinator_authority import (
    TdsCoordinatorAuthority,
    TdsDatabaseObservation,
    TdsLockObservation,
    TdsSchemaObservation,
    authority_digest,
    decode_authority,
    encode_authority,
)
from tests.test_mssql_tds_coordinator import PROCESS, SESSION
from tests.test_mssql_tds_directory_journal import OWNER


def authority():
    return TdsCoordinatorAuthority(
        "a" * 64,
        OWNER,
        PROCESS,
        "b" * 64,
        SESSION,
        TdsDatabaseObservation("db", 5, UUID(int=5)),
        TdsSchemaObservation(1, "schema"),
        TdsLockObservation(0),
    )


def test_full_receipt_roundtrip_and_digest_change():
    value = authority()
    assert decode_authority(encode_authority(value)) == value
    assert authority_digest(
        replace(value, database=replace(value.database, database_guid=UUID(int=6)))
    ) != authority_digest(value)


@pytest.mark.parametrize(
    "changes",
    [
        {"resource": "other"},
        {"owner": "Transaction"},
        {"principal": "dbo"},
        {"mode": "Shared"},
        {"acquisition_result": -1},
        {"acquisition_result": True},
    ],
)
def test_fixed_database_wide_lock_contract(changes):
    with pytest.raises(ValueError):
        replace(authority().lock, **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"transaction_count": True},
        {"transaction_count": 1},
        {"implicit_transactions": 0},
        {"session": None},
        {"database": None},
    ],
)
def test_incomplete_authority_rejected(changes):
    with pytest.raises(ValueError):
        replace(authority(), **changes)


@pytest.mark.parametrize("payload", [b"{}", b'{"schema":1,"schema":2}', b"x" * 16385])
def test_invalid_authority_codec(payload):
    with pytest.raises(ValueError):
        decode_authority(payload)


@pytest.mark.parametrize("control", ["\u0085", "\u009b"])
def test_database_controls_rejected_in_constructor_and_codec(control):
    from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

    with pytest.raises(ValueError):
        replace(authority().database, name="db" + control)
    body = strict_json_object(encode_authority(authority()))
    body["database"]["name"] = "db" + control
    with pytest.raises(ValueError):
        decode_authority(canonical_json_bytes(body))
