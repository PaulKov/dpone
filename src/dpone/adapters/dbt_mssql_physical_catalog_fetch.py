"""Bounded incremental catalog detachment; no transaction or admission policy."""

from collections.abc import Callable
from dataclasses import dataclass
from math import ceil, isfinite
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_catalog_rows import CatalogRow
from dpone.contracts.dbt_mssql_physical_catalog_wire import decode_catalog_result
from dpone.ports.physical_catalog_connection import PhysicalCatalogCursor


@dataclass
class CatalogReadBudget:
    """One acquisition deadline and conservative retained payload-byte ceiling.

    Charge eight framing bytes per field plus UTF-16LE text bytes or 16 UUID
    bytes; scalar values fit in the framing allowance. This bounds detached
    payload, not Python object overhead or driver/network buffering. The signed
    producer must enforce its bounds before sending definitions or rowsets.
    """

    deadline: float
    remaining_bytes: int
    clock: Callable[[], float]

    def seconds(self) -> int:
        """Positive remaining driver timeout; expiry never becomes infinite 0."""
        remaining = self.deadline - self.clock()
        if not isfinite(remaining) or remaining <= 0:
            raise ValueError("catalog acquisition deadline exceeded")
        return max(1, ceil(remaining))

    def charge(self, row: tuple[object, ...], definition_limit: int) -> None:
        size = 8 * len(row)
        for index, value in enumerate(row):
            if type(value) is str:
                # Reject before encoding a potentially oversized driver value.
                available = self.remaining_bytes - size
                if row[1] == "INDEX" and index == 15:
                    available = min(definition_limit, available)
                length = 0
                for character in value:
                    code = ord(character)
                    if 0xD800 <= code <= 0xDFFF:
                        raise ValueError("catalog contains invalid UTF-16 text")
                    length += 4 if code > 0xFFFF else 2
                    if length > available:
                        raise ValueError("catalog acquisition text budget exceeded")
                size += length
            elif type(value) is UUID:
                size += 16
            elif value is not None and type(value) not in {int, bool}:
                raise ValueError("catalog scalar has unsupported driver representation")
            if size > self.remaining_bytes:
                raise ValueError("catalog acquisition byte budget exceeded")
        if size > self.remaining_bytes:
            raise ValueError("catalog acquisition byte budget exceeded")
        self.remaining_bytes -= size


def require_final_rowset(cursor: PhysicalCatalogCursor) -> None:
    """Reject any extra rowset, including empty diagnostics; never drain it."""
    result = cursor.nextset()
    if result is not None and result is not False:
        raise ValueError("catalog operation returned an extra resultset")


def fetch_catalog_result(
    cursor: PhysicalCatalogCursor,
    *,
    kind: str,
    object_id: int,
    row_limit: int,
    definition_limit: int,
    budget: CatalogReadBudget,
    header_count_limit: int,
) -> tuple[CatalogRow, ...]:
    """Fetch at most limit+1 rows and decode a complete, untruncated rowset.

    Only HEADER uniqueidentifier text is adapted to UUID. Bits, timestamp text,
    numeric values and object names are not coerced. Advertised counts are
    checked before retaining the first row; overflow stops without draining.
    """
    rows: list[tuple[object, ...]] = []
    while True:
        budget.seconds()
        observed = cursor.fetchone()
        if observed is None:
            break
        if len(rows) >= row_limit:
            raise ValueError("catalog acquisition row budget exceeded")
        row = tuple(observed)
        if len(row) < 5 or len(row) > 32:
            raise ValueError("catalog row has invalid width")
        count = row[4]
        if type(count) is not int or not 0 <= count <= row_limit:
            raise ValueError("catalog advertised count exceeds budget")
        if kind == "HEADER" and len(row) == 18 and type(row[6]) is str:
            row = (*row[:6], UUID(row[6]), *row[7:])
        budget.charge(row, definition_limit)
        rows.append(row)
    require_final_rowset(cursor)
    budget.seconds()
    return decode_catalog_result(
        tuple(rows),
        expected_kind=kind,
        expected_object_id=object_id,
        max_rows=header_count_limit if kind == "HEADER" else row_limit,
        max_definition_bytes=definition_limit,
    )
