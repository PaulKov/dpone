"""Cross-platform SQL Server ``datetimeoffset`` ODBC conversion contract."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from struct import pack

import pytest

from dpone.runtime.connectors import mssql as mssql_module
from dpone.runtime.connectors.mssql import MSSQLConnector, MSSQLDatetimeOffsetDecodeError


def _raw(
    *,
    year: int = 2026,
    month: int = 8,
    day: int = 15,
    hour: int = 12,
    minute: int = 34,
    second: int = 56,
    fraction_ns: int = 123_456_000,
    offset_hours: int = 0,
    offset_minutes: int = 0,
) -> bytes:
    return pack(
        "<6hI2h",
        year,
        month,
        day,
        hour,
        minute,
        second,
        fraction_ns,
        offset_hours,
        offset_minutes,
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            _raw(offset_hours=5, offset_minutes=30),
            datetime(2026, 8, 15, 12, 34, 56, 123_456, tzinfo=timezone(timedelta(hours=5, minutes=30))),
        ),
        (
            _raw(offset_hours=-5, offset_minutes=-30),
            datetime(2026, 8, 15, 12, 34, 56, 123_456, tzinfo=timezone(-timedelta(hours=5, minutes=30))),
        ),
        (
            _raw(fraction_ns=0),
            datetime(2026, 8, 15, 12, 34, 56, tzinfo=UTC),
        ),
        (
            _raw(fraction_ns=999_999_000, offset_hours=14),
            datetime(2026, 8, 15, 12, 34, 56, 999_999, tzinfo=timezone(timedelta(hours=14))),
        ),
    ],
)
def test_decode_datetimeoffset_preserves_local_clock_microseconds_and_signed_offset(
    raw: bytes,
    expected: datetime,
) -> None:
    assert mssql_module._decode_datetimeoffset(raw) == expected
    assert mssql_module._decode_datetimeoffset(raw).utcoffset() == expected.utcoffset()


@pytest.mark.parametrize(
    "raw",
    [
        b"short",
        _raw(fraction_ns=1_000_000_000),
        _raw(fraction_ns=1),
        _raw(fraction_ns=100),
        _raw(fraction_ns=999_999_900),
        _raw(offset_hours=15),
        _raw(offset_hours=5, offset_minutes=-30),
        _raw(month=13),
    ],
)
def test_decode_datetimeoffset_rejects_malformed_vendor_payload_with_stable_type(raw: bytes) -> None:
    with pytest.raises(MSSQLDatetimeOffsetDecodeError) as exc_info:
        mssql_module._decode_datetimeoffset(raw)

    assert exc_info.value.code == "DPONE_MSSQL_DATETIMEOFFSET_DECODE_FAILED"
    assert str(exc_info.value).startswith("DPONE_MSSQL_DATETIMEOFFSET_DECODE_FAILED:")


def test_connector_registers_datetimeoffset_converter_on_every_new_session(monkeypatch: pytest.MonkeyPatch) -> None:
    class Connection:
        def __init__(self) -> None:
            self.converters: dict[int, object] = {}

        def add_output_converter(self, type_code: int, converter: object) -> None:
            self.converters[type_code] = converter

    class Pyodbc:
        def __init__(self) -> None:
            self.connections: list[Connection] = []

        def connect(self, *_args: object, **_kwargs: object) -> Connection:
            connection = Connection()
            self.connections.append(connection)
            return connection

    pyodbc = Pyodbc()
    monkeypatch.setattr(mssql_module, "_require_pyodbc", lambda: pyodbc)
    connector = MSSQLConnector(host="sql", port=1433, database="dwh", user="u", password="p")

    first = connector.connection
    assert first.converters[-155] is mssql_module._decode_datetimeoffset
    connector._connection = None
    second = connector.connection
    assert second is not first
    assert second.converters[-155] is mssql_module._decode_datetimeoffset
