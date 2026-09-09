"""PostgreSQL test_decoding logical decoding parser."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from dpone.runtime.cdc.base import CDCChange, CDCOperation


class TestDecodingMessageParser:
    """Parser for PostgreSQL ``test_decoding`` text messages."""

    __test__ = False

    _message_re = re.compile(
        r"^table (?P<schema>[^.]+)\.(?P<table>[^:]+): (?P<operation>INSERT|UPDATE|DELETE): ?(?P<body>.*)$"
    )
    _int_re = re.compile(r"^-?\d+$")
    _decimal_re = re.compile(r"^-?\d+\.\d+$")

    def __init__(self, *, source_schema: str, source_table: str):
        self.source_schema = source_schema
        self.source_table = source_table

    def parse(self, lsn: str, xid: str | None, data: str) -> CDCChange | None:
        """Parse one logical decoding message into a ``CDCChange``."""

        match = self._message_re.match(data.strip())
        if not match:
            return None

        schema = self._unquote_identifier(match.group("schema"))
        table = self._unquote_identifier(match.group("table"))
        if schema != self.source_schema or table != self.source_table:
            return None

        raw_operation = match.group("operation")
        before: dict[str, Any] | None = None
        body = match.group("body")
        if raw_operation == "UPDATE" and body.startswith("old-key:"):
            before_text, _, after_text = body.partition(" new-tuple:")
            before = self._parse_tuple(before_text.removeprefix("old-key:").strip())
            body = after_text.strip()

        operation = {
            "INSERT": CDCOperation.INSERT,
            "UPDATE": CDCOperation.UPDATE,
            "DELETE": CDCOperation.DELETE,
        }[raw_operation]
        return CDCChange(
            operation=operation,
            data=self._parse_tuple(body),
            before=before,
            position=str(lsn),
            transaction_id=str(xid) if xid is not None else None,
            source_schema=schema,
            source_table=table,
            metadata={"plugin": "test_decoding"},
        )

    def _parse_tuple(self, body: str) -> dict[str, Any]:
        values: dict[str, Any] = {}
        index = 0
        length = len(body)
        while index < length:
            while index < length and body[index].isspace():
                index += 1
            if index >= length:
                break

            name_start = index
            while index < length and body[index] != "[":
                index += 1
            column_name = body[name_start:index]
            if index >= length:
                break
            index += 1
            while index < length and body[index] != "]":
                index += 1
            index += 1
            if index >= length or body[index] != ":":
                break
            index += 1

            raw_value, index = self._read_value(body, index)
            values[self._unquote_identifier(column_name)] = self._convert_value(raw_value)
        return values

    def _read_value(self, body: str, index: int) -> tuple[str, int]:
        if index < len(body) and body[index] == "'":
            index += 1
            chars: list[str] = []
            while index < len(body):
                char = body[index]
                if char == "'":
                    if index + 1 < len(body) and body[index + 1] == "'":
                        chars.append("'")
                        index += 2
                        continue
                    index += 1
                    break
                chars.append(char)
                index += 1
            return "".join(chars), index

        start = index
        while index < len(body) and not body[index].isspace():
            index += 1
        return body[start:index], index

    def _convert_value(self, raw: str) -> Any:
        lowered = raw.lower()
        if lowered in {"null", "<null>"}:
            return None
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        if self._int_re.match(raw):
            return int(raw)
        if self._decimal_re.match(raw):
            decimal = Decimal(raw)
            return int(decimal) if decimal == decimal.to_integral_value() else float(decimal)
        return raw

    def _unquote_identifier(self, value: str) -> str:
        value = value.strip()
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            return value[1:-1].replace('""', '"')
        return value


__all__ = ["TestDecodingMessageParser"]
