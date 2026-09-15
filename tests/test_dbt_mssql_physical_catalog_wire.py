"""Detached transport tests; these do not certify SQL acquisition or visibility."""

from dataclasses import FrozenInstanceError
from datetime import datetime
from uuid import UUID

import pytest

from dpone.contracts.dbt_mssql_physical_catalog_wire import decode_catalog_result

STAMP = "2026-09-15T12:34:56.1234567"
DETAILS = {
    "HEADER": (1, UUID(int=1), STAMP, STAMP, "dbo", "orders", "U ", 1, 1, 1, 1, 1, 1),
    "TABLE": (1, "dbo", "orders", "U ", STAMP, STAMP, False, 0, 0, False, False, False, 0, 0, 0),
    "COLUMN": (
        1,
        "id",
        56,
        56,
        "sys",
        "int",
        -1,
        10,
        0,
        None,
        False,
        True,
        False,
        False,
        False,
        False,
        False,
        0,
        None,
        False,
        0,
        0,
    ),
    "INDEX": (0, None, 0, "HEAP", False, False, False, False, False, False, None, 1, None, None),
    "INDEX_COLUMN": (0, 1, 1, 0, 0, False, False, 0),
    "PARTITION": (0, 1, 123, 456, 0, "NONE", 1, None, None),
    "DEPENDENCY": ("INBOUND", 1, 0, None, 0, None, None, None, None, False, False, False),
    "FORBIDDEN_PROPERTY": ("UNREGISTERED_CODE", None, None),
    "COUNT": (0,),
}


def decode(kind, details=None, **kwargs):
    row = (1, kind, 42, 1, 1) + (DETAILS[kind] if details is None else details)
    return decode_catalog_result(
        (row,), expected_kind=kind, expected_object_id=42, max_rows=10, max_definition_bytes=8, **kwargs
    )


@pytest.mark.parametrize("kind", DETAILS)
def test_all_kinds_preserve_order_values_and_freeze(kind):
    from dataclasses import astuple

    (row,) = decode(kind)
    assert astuple(row) == (1, kind, 42, 1, 1) + DETAILS[kind]
    with pytest.raises(FrozenInstanceError):
        row.object_id = 99


@pytest.mark.parametrize("kind", DETAILS)
def test_empty_collection_marker_and_singletons(kind):
    marker = ((1, kind, 42, 0, 0) + (None,) * len(DETAILS[kind]),)
    args = dict(expected_kind=kind, expected_object_id=42, max_rows=1, max_definition_bytes=1)
    if kind in {"HEADER", "TABLE", "COUNT"}:
        with pytest.raises(ValueError):
            decode_catalog_result(marker, **args)
    else:
        assert decode_catalog_result(marker, **args) == ()


@pytest.mark.parametrize(
    "kind,index,value",
    [
        ("HEADER", 1, str(UUID(int=1))),
        ("HEADER", 2, datetime(2026, 9, 15)),
        ("HEADER", 2, "2026-02-29T12:34:56.1234567"),
        ("HEADER", 2, "2026-09-15T24:00:00.1234567"),
        ("HEADER", 2, "2026-09-15T12:34:56.123456"),
        ("HEADER", 2, "0000-01-01T00:00:00.0000000"),
        ("HEADER", 7, 11),
        ("HEADER", 7, -1),
        ("TABLE", 6, 0),
        ("TABLE", 7, 256),
        ("COLUMN", 6, -32769),
        ("COLUMN", 6, 32768),
        ("COLUMN", 1, "😀" * 65),
        ("COLUMN", 1, "\ud800"),
        ("COLUMN", 1, ""),
        ("COLUMN", 0, True),
        ("INDEX", 10, "😀😀x"),
        ("PARTITION", 2, 2**63),
        ("COUNT", 0, -1),
        ("COUNT", 0, True),
        ("DEPENDENCY", 0, "SIDEWAYS"),
        ("FORBIDDEN_PROPERTY", 0, "é"),
        ("TABLE", 3, "U"),
        ("TABLE", 3, "é "),
    ],
)
def test_rejects_individual_primitive_violations(kind, index, value):
    details = list(DETAILS[kind])
    details[index] = value
    with pytest.raises(ValueError):
        decode(kind, tuple(details))


def test_lossless_boundaries_and_nullable_values():
    (header,) = decode("HEADER")
    assert header.object_create_time == STAMP
    details = list(DETAILS["INDEX"])
    details[10] = "😀😀"
    assert decode("INDEX", tuple(details))[0].filter_definition == "😀😀"
    details = list(DETAILS["COLUMN"])
    details[1] = "😀" * 64
    assert decode("COLUMN", tuple(details))[0].name == "😀" * 64


@pytest.mark.parametrize(
    "rows",
    [
        (),
        [],
        ((1, "COUNT", 42, 1, 1),),
        ((1, "COUNT", 42, 1, 1, 0, 0),),
        ([1, "COUNT", 42, 1, 1, 0],),
        ((2, "COUNT", 42, 1, 1, 0),),
        ((1, "TABLE", 42, 1, 1, 0),),
        ((1, "COUNT", 43, 1, 1, 0),),
        ((1, "COUNT", 42, 2, 1, 0),),
        ((1, "COUNT", 42, 1, 2, 0),),
        ((True, "COUNT", 42, 1, 1, 0),),
    ],
)
def test_invalid_envelope(rows):
    with pytest.raises(ValueError):
        decode_catalog_result(rows, expected_kind="COUNT", expected_object_id=42, max_rows=10, max_definition_bytes=8)


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_rows", True),
        ("max_rows", 0),
        ("max_definition_bytes", -1),
        ("max_definition_bytes", 1.0),
        ("expected_kind", "UNKNOWN"),
        ("expected_object_id", True),
        ("expected_object_id", 2**31),
    ],
)
def test_invalid_decoder_arguments(field, value):
    args = dict(expected_kind="COUNT", expected_object_id=42, max_rows=10, max_definition_bytes=8)
    args[field] = value
    with pytest.raises(ValueError):
        decode_catalog_result(((1, "COUNT", 42, 1, 1, 0),), **args)


@pytest.mark.parametrize(
    "ordinals,counts",
    [((1, 1), (2, 2)), ((1, 3), (2, 2)), ((2, 1), (2, 2)), ((1, 2), (2, 3)), ((1, 2), (11, 11)), ((0, 1), (0, 1))],
)
def test_rejects_incomplete_reordered_or_mixed_collections(ordinals, counts):
    rows = tuple(
        (1, "FORBIDDEN_PROPERTY", 42, ordinal, count) + DETAILS["FORBIDDEN_PROPERTY"]
        for ordinal, count in zip(ordinals, counts, strict=True)
    )
    with pytest.raises(ValueError):
        decode_catalog_result(
            rows, expected_kind="FORBIDDEN_PROPERTY", expected_object_id=42, max_rows=10, max_definition_bytes=8
        )


@pytest.mark.parametrize(
    "kind,index",
    [(kind, i) for kind, values in DETAILS.items() for i, value in enumerate(values) if type(value) is bool],
)
def test_every_bit_rejects_integer_driver_coercion(kind, index):
    values = list(DETAILS[kind])
    values[index] = 1
    with pytest.raises(ValueError):
        decode(kind, tuple(values))


@pytest.mark.parametrize(
    "kind,index,low,high",
    [
        ("TABLE", 0, -(2**31), 2**31 - 1),
        ("TABLE", 7, 0, 255),
        ("COLUMN", 6, -32768, 32767),
        ("PARTITION", 2, -(2**63), 2**63 - 1),
        ("COUNT", 0, 0, 2**63 - 1),
    ],
)
def test_numeric_transport_limits_are_not_physical_admission(kind, index, low, high):
    for number in (low, high):
        values = list(DETAILS[kind])
        values[index] = number
        assert decode(kind, tuple(values))
    for number in (low - 1, high + 1, True, 1.0, "1"):
        values = list(DETAILS[kind])
        values[index] = number
        with pytest.raises(ValueError):
            decode(kind, tuple(values))


@pytest.mark.parametrize(
    "stamp", ["0001-01-01T00:00:00.0000000", "9999-12-31T23:59:59.9999999", "2024-02-29T00:00:00.0000001"]
)
def test_timestamp_precision_and_calendar_boundaries(stamp):
    values = list(DETAILS["HEADER"])
    values[2] = stamp
    assert decode("HEADER", tuple(values))[0].object_create_time == stamp


def test_complete_multirow_collection_and_partial_marker():
    rows = tuple(
        (1, "FORBIDDEN_PROPERTY", 42, ordinal, 2, code, None, None) for ordinal, code in ((1, "FIRST"), (2, "SECOND"))
    )
    args = dict(expected_kind="FORBIDDEN_PROPERTY", expected_object_id=42, max_rows=2, max_definition_bytes=1)
    result = decode_catalog_result(rows, **args)
    assert tuple(row.property_code for row in result) == ("FIRST", "SECOND")
    with pytest.raises(ValueError):
        decode_catalog_result(((1, "FORBIDDEN_PROPERTY", 42, 0, 0, "PARTIAL", None, None),), **args)
    with pytest.raises(ValueError):
        decode_catalog_result(rows + (rows[1],), **args)


@pytest.mark.parametrize(
    "kind,index,value",
    [
        ("COLUMN", 9, "Latin1_General_CI_AS"),
        ("COLUMN", 18, 1),
        ("INDEX", 1, "ix"),
        ("INDEX", 10, ""),
        ("INDEX", 12, "PRIMARY"),
        ("INDEX", 13, "FG"),
        ("PARTITION", 7, "PRIMARY"),
        ("PARTITION", 8, "FG"),
        ("DEPENDENCY", 3, 12),
        ("DEPENDENCY", 5, "server"),
        ("DEPENDENCY", 6, "database"),
        ("DEPENDENCY", 7, "dbo"),
        ("DEPENDENCY", 8, "target"),
        ("DEPENDENCY", 0, "OUTBOUND"),
        ("FORBIDDEN_PROPERTY", 1, 42),
        ("FORBIDDEN_PROPERTY", 2, 1),
    ],
)
def test_nullable_present_branch_and_outbound_direction(kind, index, value):
    from dataclasses import astuple

    values = list(DETAILS[kind])
    values[index] = value
    assert astuple(decode(kind, tuple(values))[0])[5 + index] == value


@pytest.mark.parametrize(
    "kind,index", [("COUNT", 0), ("TABLE", 0), ("HEADER", 1), ("COLUMN", 1), ("INDEX", 4), ("DEPENDENCY", 0)]
)
def test_nonnullable_values_cannot_be_synthesized(kind, index):
    values = list(DETAILS[kind])
    values[index] = None
    with pytest.raises(ValueError):
        decode(kind, tuple(values))
