"""Admitted native layout and exact scalar policy behind the TDS row boundary.

File ownership, byte identity and SDK batching belong to the injected consumer;
this decoder operates only on its supplied stream and does not import an SDK.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, BinaryIO

from dpone.ports.mssql_tds_input import TdsInputColumn
from dpone.runtime.native_wire_models import SourceNativeWireContract
from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder, validate_mssql_native_contract

_LAYOUTS = frozenset({"bigint", "float(53)", "nvarchar(max)", "datetime2(6)"})

_EPOCH = datetime(1970, 1, 1)


def validate_tds_layouts(contract: SourceNativeWireContract, *, input_mode: str) -> None:
    """Reject unsupported layouts before a source or retained file is opened."""
    if input_mode not in {"rows", "arrow"}:
        raise ValueError("mssql_native.tds_invalid_input_mode:select_rows_or_arrow")
    validate_mssql_native_contract(contract)
    for column in contract.columns:
        declared = column.source_type.removesuffix(" nullable")
        if declared not in _LAYOUTS:
            raise ValueError(
                f"mssql_native.tds_unsupported_layout:mode={input_mode}:layout={column.source_type}:select_bcp"
            )


def convert_tds_row(row: Mapping[str, Any], contract: SourceNativeWireContract, *, input_mode: str) -> tuple[Any, ...]:
    """Return ordered exact values; Arrow temporal values are integer microseconds.

    No business values appear in errors. Call admission once before consuming a
    stream; this standalone function also validates admission for direct callers.
    """
    validate_tds_layouts(contract, input_mode=input_mode)
    return _convert_admitted_row(row, contract, input_mode=input_mode)


def _convert_admitted_row(
    row: Mapping[str, Any], contract: SourceNativeWireContract, *, input_mode: str
) -> tuple[Any, ...]:
    if set(row) != {column.name for column in contract.columns}:
        raise ValueError("mssql_native.tds_invalid_row:column_identity")
    result: list[Any] = []
    for ordinal, column in enumerate(contract.columns):
        value = row[column.name]
        error = f"mssql_native.tds_invalid_value:ordinal={ordinal}"
        if value is None:
            if not column.nullable:
                raise ValueError(error)
        elif column.storage_type == "bigint":
            if type(value) is not int or not -(2**63) <= value < 2**63:
                raise ValueError(error)
        elif column.storage_type == "float":
            if type(value) is not float or not math.isfinite(value):
                raise ValueError(error)
        elif column.storage_type == "nvarchar":
            if type(value) is not str:
                raise ValueError(error)
            try:
                value.encode("utf-8", errors="strict")
            except UnicodeError:
                raise ValueError(error) from None
        else:
            if (
                not isinstance(value, datetime)
                or value.tzinfo is not None
                or getattr(value, "submicrosecond_100ns", 0) != 0
            ):
                raise ValueError(error)
            # Strip the native decoder's precision-carrying subclass for the SDK.
            value = datetime(
                value.year, value.month, value.day, value.hour, value.minute, value.second, value.microsecond
            )
            if input_mode == "arrow":
                delta = value - _EPOCH
                value = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
        result.append(value)
    return tuple(result)


@dataclass(frozen=True)
class MssqlTdsRowDecoder:
    """Immutable admission binding for rows or Arrow microsecond scalar input."""

    _contract: SourceNativeWireContract
    _input_mode: str

    def __init__(self, contract: SourceNativeWireContract, *, input_mode: str) -> None:
        validate_tds_layouts(contract, input_mode=input_mode)
        object.__setattr__(self, "_contract", contract)
        object.__setattr__(self, "_input_mode", input_mode)

    @property
    def columns(self) -> tuple[TdsInputColumn, ...]:
        """Expose the same immutable admitted physical column metadata."""
        return self._contract.columns

    @property
    def input_mode(self) -> str:
        return self._input_mode

    def iter_rows(
        self,
        stream: BinaryIO,
        *,
        max_row_bytes: int,
        on_bytes: Callable[[bytes], None],
    ) -> Iterator[tuple[Any, ...]]:
        """Read bounded native rows to natural EOF without closing the stream."""
        decoder = MssqlBcpNativeDecoder(self._contract)
        for row in decoder.iter_stream(stream, max_row_bytes=max_row_bytes, on_bytes=on_bytes):
            yield _convert_admitted_row(row, self._contract, input_mode=self._input_mode)
