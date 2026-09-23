"""CREATE-session authority bytes remain compatible across adapter extraction."""

from uuid import UUID

import pytest

from dpone.adapters.mssql_tds_session import _authority_digest
from dpone.contracts.mssql_tds_session import coordinator_authority_digest


def authority():
    return [
        "server",
        "machine",
        "instance",
        "replica",
        "database",
        5,
        UUID("22222222-2222-2222-2222-222222222222"),
        "login",
        b"sid",
        "original",
        b"originalsid",
        "user",
        1,
        b"usersid",
    ]


def test_literal_pre_extraction_digest_and_adapter_parity():
    values = authority()
    expected = bytes.fromhex("d550b2d3218a6e45f605b99d575e0fa6e8bc7a072a80cd348fc0870285bf7b47")
    assert coordinator_authority_digest(values) == expected
    assert coordinator_authority_digest(tuple(values)) == expected
    assert _authority_digest([None] * 13 + values) == expected


@pytest.mark.parametrize("index", range(14))
def test_every_original_authority_field_changes_binding(index):
    original = authority()
    changed = authority()
    value = changed[index]
    if type(value) is str:
        changed[index] += "\u0416"
    elif type(value) is bytes:
        changed[index] += b"\0"
    elif type(value) is int:
        changed[index] += 1
    else:
        changed[index] = UUID(int=value.int + 1)
    assert coordinator_authority_digest(changed) != coordinator_authority_digest(original)
    assert coordinator_authority_digest(changed) == _authority_digest([None] * 13 + changed)


@pytest.mark.parametrize(
    "index,value",
    [
        (0, ""),
        (1, "a" * 129),
        (2, "\n"),
        (3, "\ud800"),
        (5, True),
        (5, 0),
        (5, 1.0),
        (12, 2**31),
        (6, str(UUID(int=1))),
        (6, UUID(int=0)),
        (8, bytearray(b"s")),
        (10, b""),
        (13, b"s" * 86),
    ],
)
def test_rejects_missing_or_coerced_authority_fields(index, value):
    changed = authority()
    changed[index] = value
    with pytest.raises((ValueError, UnicodeError)):
        coordinator_authority_digest(changed)
    with pytest.raises((ValueError, UnicodeError)):
        _authority_digest([None] * 13 + changed)


@pytest.mark.parametrize("values", [[], [None] * 13, [None] * 15])
def test_rejects_wrong_authority_width(values):
    with pytest.raises(ValueError):
        coordinator_authority_digest(values)
