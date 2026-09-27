"""Deterministic synthetic rows for MSSQL target-local differential checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.contracts.mssql_native_chunks import EncodedNativeFile
from dpone.runtime.mssql_native_chunks_files import encode_native_frame, native_multiset_digest
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_models import SourceNativeWireContract
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_target_digest import TargetDigest


@dataclass(frozen=True, slots=True)
class TargetLocalDigestCase:
    """One closed native schema with Python values and equivalent SQL literals."""

    name: str
    columns: tuple[tuple[str, str], ...]
    rows: tuple[tuple[Any, ...], ...]
    sql_rows: tuple[tuple[str, ...], ...]

    @property
    def contract(self) -> SourceNativeWireContract:
        return build_mssql_bcp_native_contract(schema=self.columns, query="SELECT synthetic")

    def python_digest(self) -> TargetDigest:
        encoder = MssqlNativeEncoder(self.contract, max_row_bytes=1 << 20)
        total = 0
        for row in self.rows:
            encoded = encoder.encode_row(row)
            total = (total + int.from_bytes(sha256(encoded).digest(), "big")) % (1 << 256)
        return TargetDigest(len(self.rows), native_multiset_digest(len(self.rows), total), total)

    def sealed_file(self, path: Path) -> EncodedNativeFile:
        return encode_native_frame(
            self.contract,
            self.rows,
            path,
            0,
            max_row_bytes=1 << 20,
            max_bytes=8 << 20,
        )


def _narrow() -> TargetLocalDigestCase:
    columns = (
        ("row_key", "bigint"),
        ("ratio", "float(53) nullable"),
        ("text_value", "nvarchar(max) nullable"),
        ("happened_at", "datetime2(6) nullable"),
    )
    rows = (
        (1, -1.5, "A\x00Б😀", datetime(1900, 1, 1, 0, 0, 0, 1)),
        (2, 0.0, "", datetime(2024, 2, 29, 23, 59, 59, 999999)),
        (3, None, None, None),
        (4, 1.25, "duplicate", datetime(2030, 6, 1, 12, 30, 45, 123456)),
        (4, 1.25, "duplicate", datetime(2030, 6, 1, 12, 30, 45, 123456)),
    )
    sql_rows = (
        ("1", "-1.5", "N'A'+NCHAR(0)+N'Б😀'", "CONVERT(datetime2(6),'1900-01-01T00:00:00.000001')"),
        ("2", "0.0", "N''", "CONVERT(datetime2(6),'2024-02-29T23:59:59.999999')"),
        ("3", "NULL", "NULL", "NULL"),
        ("4", "1.25", "N'duplicate'", "CONVERT(datetime2(6),'2030-06-01T12:30:45.123456')"),
        ("4", "1.25", "N'duplicate'", "CONVERT(datetime2(6),'2030-06-01T12:30:45.123456')"),
    )
    return TargetLocalDigestCase("narrow", columns, rows, sql_rows)


def _wide100() -> TargetLocalDigestCase:
    columns: list[tuple[str, str]] = []
    for index in range(25):
        columns.extend(
            (
                (f"i{index:02d}", "bigint nullable"),
                (f"f{index:02d}", "float(53) nullable"),
                (f"s{index:02d}", "nvarchar(max) nullable"),
                (f"t{index:02d}", "datetime2(6) nullable"),
            )
        )
    rows: list[tuple[Any, ...]] = []
    sql_rows: list[tuple[str, ...]] = []
    for row_index in range(4):
        values: list[Any] = []
        literals: list[str] = []
        for column_index in range(25):
            if row_index == 2 and column_index % 5 == 0:
                values.extend((None, None, None, None))
                literals.extend(("NULL", "NULL", "NULL", "NULL"))
                continue
            integer = (row_index - 2) * 100_000 + column_index
            floating = (row_index - 1) * 1.25 + column_index / 100
            text = "" if row_index == 1 else f"r{row_index}-c{column_index}-λ"
            moment = datetime(2020 + row_index, 1 + column_index % 12, 1 + column_index % 27, 12, 0, 0, column_index)
            values.extend((integer, floating, text, moment))
            literals.extend(
                (
                    str(integer),
                    repr(floating),
                    "N'" + text.replace("'", "''") + "'",
                    f"CONVERT(datetime2(6),'{moment.isoformat(timespec='microseconds')}')",
                )
            )
        rows.append(tuple(values))
        sql_rows.append(tuple(literals))
    return TargetLocalDigestCase("wide100", tuple(columns), tuple(rows), tuple(sql_rows))


def digest_cases() -> tuple[TargetLocalDigestCase, ...]:
    """Return fresh immutable narrow and exactly-100-business-column cases."""
    cases = (_narrow(), _wide100())
    assert len(cases[1].columns) == 100
    return cases


__all__ = ["TargetLocalDigestCase", "digest_cases"]
