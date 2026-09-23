"""Position-specific OBSERVE rows preserve raw SQL values without coercion."""

from datetime import UTC, datetime
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_observe_rows import decode_rows, encode_rows


@pytest.mark.parametrize(
    "opcode,row",
    [
        ("PRINCIPALS", (5, "writer", b"\x00\xff", "SQL_USER", "INSTANCE")),
        ("PERMISSIONS", (1, -2147483648, 0, 5, 1, "SL  ", "SELECT", "D")),
        ("MEMBER", (5, 1, "dbo", "table", "USER_TABLE", datetime(2026, 1, 1))),
        ("COLUMNS", (1, "name", "bigint", False, 8, 19, 0, None, 0, False, False)),
        ("BATCH_FEATURES", (5, *((0,) * 12))),
        ("BATCH_COLUMNS", (5, 1, "name", "bigint", False, 8, 19, 0, None, 0, False, False)),
    ],
)
def test_lossless_position_specific_roundtrip(opcode, row):
    assert decode_rows(opcode, encode_rows(opcode, [row, row])) == [row, row]


@pytest.mark.parametrize(
    "opcode,row",
    [
        ("PRINCIPALS", (True, "writer", b"x", "SQL_USER", "INSTANCE")),
        ("PRINCIPALS", (5, "writer", "AA", "SQL_USER", "INSTANCE")),
        ("MEMBER", (5, 1, "dbo", "table", "USER_TABLE", datetime.now(UTC))),
        ("COLUMNS", (1, "name", "bigint", 0, 8, 19, 0, None, 0, False, False)),
    ],
)
def test_original_aliases_reject_before_conversion(opcode, row):
    with pytest.raises(ValueError):
        encode_rows(opcode, [row])


def test_uuid_original_integer_alias_rejected():
    value = UUID(int=1)
    object.__setattr__(value, "int", True)
    row = ("s", "s", "s", "s", 1, "s", value, b"x", 1, "s", b"x", 1, "s")
    with pytest.raises(ValueError):
        encode_rows("WRITER_ADMISSION", [row])
