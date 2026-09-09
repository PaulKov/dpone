"""ClickHouse JSON payload projection for typed CDC materialization."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from .typed_materialization_common import (
    base_type,
    datetime64_profile,
    decimal_scale,
    quote_identifier,
    quote_literal,
)

if TYPE_CHECKING:
    from dpone.ports.cdc_typed_materialization import ClickHouseCdcTypedColumnPort


class ClickHouseCdcPayloadProjector:
    """Render typed ClickHouse expressions from canonical CDC payload JSON."""

    def __init__(self, columns: Sequence[ClickHouseCdcTypedColumnPort]) -> None:
        if not columns:
            raise ValueError("Typed CDC materialization requires at least one projected column")
        self._columns = tuple(columns)

    @property
    def columns(self) -> tuple[ClickHouseCdcTypedColumnPort, ...]:
        return self._columns

    def select_expressions(self, *, payload_column: str) -> str:
        return ",\n    ".join(self._expression(column, payload_column=payload_column) for column in self._columns)

    @staticmethod
    def conversion_expression(column: ClickHouseCdcTypedColumnPort, payload_column: str) -> str:
        payload_key = quote_literal(column.resolved_payload_key)
        target_type = base_type(column.clickhouse_type)
        raw = f"JSONExtractRaw({payload_column}, {payload_key})"
        string = f"JSONExtractString({payload_column}, {payload_key})"
        if target_type == "String":
            return string
        if target_type == "Bool":
            return f"JSONExtractBool({payload_column}, {payload_key})"
        if target_type.startswith("Int") or target_type.startswith("UInt"):
            return f"to{target_type}OrNull({raw})"
        if target_type.startswith("Float"):
            return f"to{target_type}OrNull({raw})"
        if target_type.startswith("Decimal"):
            return f"toDecimal128OrNull({string}, {decimal_scale(target_type)})"
        if target_type == "Date":
            return f"toDateOrNull({string})"
        if target_type.startswith("DateTime64"):
            scale, timezone = datetime64_profile(target_type)
            return f"parseDateTime64BestEffortOrNull({string}, {scale}, {quote_literal(timezone)})"
        if target_type == "DateTime":
            return f"parseDateTimeBestEffortOrNull({string})"
        raise ValueError(f"Unsupported ClickHouse type: {column.clickhouse_type!r}")

    @staticmethod
    def parse_failure_predicate(column: ClickHouseCdcTypedColumnPort, payload_column: str) -> str | None:
        target_type = base_type(column.clickhouse_type)
        if target_type in {"String", "Bool"}:
            return None
        payload_key = quote_literal(column.resolved_payload_key)
        raw = f"JSONExtractRaw({payload_column}, {payload_key})"
        expression = ClickHouseCdcPayloadProjector.conversion_expression(column, payload_column)
        return f"{raw} != '' AND isNull({expression})"

    def _expression(self, column: ClickHouseCdcTypedColumnPort, *, payload_column: str) -> str:
        expression = self.conversion_expression(column, payload_column)
        return f"{expression} AS {quote_identifier(column.name)}"


__all__ = ["ClickHouseCdcPayloadProjector"]
