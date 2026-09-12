"""Checked, bounded encoder for the existing finite MSSQL native BCP profile.

The caller supplies the exact staging-table schema via a native wire contract.
This service performs no I/O and never coerces arbitrary values to text. Its
bytes include native field prefixes and can be concatenated without row markers.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from dpone.runtime.mssql_native_encoder_values import encode_native_value, native_value_size
from dpone.runtime.native_wire_mssql import validate_mssql_native_contract

if TYPE_CHECKING:
    from dpone.runtime.native_wire_models import SourceNativeWireContract


class MssqlNativeEncoder:
    """Encode exact typed rows, failing before returning any partial row.

    ``max_row_bytes`` includes all field framing. Mappings must contain exactly
    the contract's names; sequences must have its exact positional arity. NULL
    is represented only by ``None``. Capacity and value errors are ``ValueError``
    with stable diagnostic prefixes and never include business values.
    """

    def __init__(self, contract: SourceNativeWireContract, *, max_row_bytes: int) -> None:
        validate_mssql_native_contract(contract)
        if type(max_row_bytes) is not int or max_row_bytes <= 0:
            raise ValueError("mssql_native_invalid_max_row_bytes")
        self._contract = contract
        self._max_row_bytes = max_row_bytes

    def _values(self, row: Sequence[Any] | Mapping[str, Any]) -> Iterator[Any]:
        columns = self._contract.columns
        if isinstance(row, Mapping):
            if row.keys() != {column.name for column in columns}:
                raise ValueError("mssql_native_columns_mismatch")
            return (row[column.name] for column in columns)
        elif isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
            if len(row) != len(columns):
                raise ValueError("mssql_native_columns_mismatch")
            return iter(row)
        else:
            raise ValueError("mssql_native_columns_mismatch")

    def encoded_row_size(self, row: Sequence[Any] | Mapping[str, Any]) -> int:
        """Size a row exactly without allocating a native payload.

        Checks shape, NULL and byte bounds. Fixed-width scalar domain checks are
        deferred to ``encode_row``; size alone does not certify valid values.
        """
        total = 0
        for column, value in zip(self._contract.columns, self._values(row), strict=True):
            total += column.prefix_width
            if total > self._max_row_bytes:
                raise ValueError("mssql_native_row_bytes_exceeded")
            if value is None:
                if not column.nullable or not column.prefix_width:
                    raise ValueError("mssql_native_unexpected_null")
            else:
                total += native_value_size(value, column, self._max_row_bytes - total)
        return total

    def encode_row(self, row: Sequence[Any] | Mapping[str, Any]) -> bytes:
        """Return one lossless native row; reject shape, range and precision loss."""
        columns = self._contract.columns
        values = self._values(row)
        result = bytearray()
        for column, value in zip(columns, values, strict=True):
            remaining = self._max_row_bytes - len(result) - column.prefix_width
            if remaining < 0:
                raise ValueError("mssql_native_row_bytes_exceeded")
            if value is None:
                if not column.nullable or not column.prefix_width:
                    raise ValueError("mssql_native_unexpected_null")
                result.extend(b"\xff" * column.prefix_width)
                continue
            payload = encode_native_value(value, column, remaining)
            if column.prefix_width:
                try:
                    result.extend(len(payload).to_bytes(column.prefix_width, "little", signed=True))
                except OverflowError:
                    raise ValueError("mssql_native_field_length_exceeded") from None
            result.extend(payload)
        return bytes(result)
