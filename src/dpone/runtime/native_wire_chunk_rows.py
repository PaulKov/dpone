"""Bounded, value-free row framing for an incremental MSSQL native stream."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from dpone.runtime.native_wire_models import SourceNativeWireContract
from dpone.runtime.native_wire_mssql import validate_mssql_native_contract
from dpone.runtime.native_wire_mssql_framing import validate_payload_length
from dpone.runtime.physical_chunk_policy import PhysicalRowLimitExceeded


class NativeWireRowFramer:
    """Preserve complete native rows without decoding individual scalar values.

    Working memory is O(max_row_bytes + input_segment_bytes), including row
    output copies. The caller must bound input segment size. Declared lengths are checked before waiting
    for payload bytes; malformed EOF can never become a successful short row.
    """

    def __init__(self, contract: SourceNativeWireContract, *, max_row_bytes: int) -> None:
        validate_mssql_native_contract(contract)
        if contract.blockers or not contract.columns or max_row_bytes <= 0:
            raise ValueError("native_wire_chunk_contract_invalid")
        self.columns = contract.columns
        self.max_row_bytes = max_row_bytes

    def rows(self, segments: Iterable[bytes]) -> Iterator[bytes]:
        buffer = bytearray()
        position = ordinal = row_start = 0
        pending: int | None = None
        for segment in segments:
            buffer.extend(segment)
            while position < len(buffer) or pending == 0:
                column = self.columns[ordinal]
                if pending is None:
                    width = column.prefix_width
                    if len(buffer) - position < width:
                        break
                    length = column.fixed_length
                    if width:
                        length = int.from_bytes(buffer[position : position + width], "little", signed=True)
                        position += width
                    if length == -1 and width:
                        if not column.nullable:
                            raise ValueError("native_wire_unexpected_null:physical_chunk")
                        length = 0
                    else:
                        validate_payload_length(column, length, "physical_chunk")
                    assert length is not None
                    row_bytes = position - row_start + length
                    if row_bytes > self.max_row_bytes:
                        raise PhysicalRowLimitExceeded(row_bytes=row_bytes, max_chunk_bytes=self.max_row_bytes)
                    pending = length
                if len(buffer) - position < pending:
                    break
                position += pending
                pending = None
                ordinal += 1
                if ordinal == len(self.columns):
                    yield bytes(memoryview(buffer)[row_start:position])
                    row_start = position
                    ordinal = 0
            if row_start:
                del buffer[:row_start]
                position -= row_start
                row_start = 0
        if buffer or ordinal or pending is not None:
            raise ValueError("native_wire_truncated_row:physical_chunk")
