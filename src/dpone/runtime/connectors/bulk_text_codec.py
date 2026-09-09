"""Reversible text encoding for native bulk character files.

The SQL Server ``bcp`` character format is fast, but it is delimiter based. It
does not understand PostgreSQL-style ``NULL`` markers or CSV quotes. This small
codec keeps the fast path safe by encoding text values before the file reaches
``bcp`` and by rendering set-based SQL expressions that decode values during
the staging-to-target commit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

DEFAULT_MARKER_PREFIX = "\x1d"
DEFAULT_EMPTY_STRING_MARKER = f"{DEFAULT_MARKER_PREFIX}E"


_CONTROL_CODES = {
    DEFAULT_MARKER_PREFIX: "P",
    "\t": "T",
    "\r": "R",
    "\n": "N",
    "\x1f": "U",
    "\x1e": "S",
}
_MSSQL_CODEC_BINARY_COLLATION = "Latin1_General_100_BIN2"


@dataclass(frozen=True)
class BulkTextCodec:
    """Encode text values so delimiter based bulk files remain lossless."""

    codec_id: ClassVar[str] = "dpone.mssql-delimited.bulk-text"
    # Version 2 makes every CSV structural control explicit on the wire.  In
    # particular, PostgreSQL CSV quotes fields containing a bare carriage
    # return even when the configured record terminator is only ``\n``.  A
    # quoted field is not understood by character-mode bcp, so ``\r`` must be
    # encoded just like TAB/LF and the CSV quote sentinel (U+001F).
    codec_version: ClassVar[int] = 2

    marker_prefix: str = DEFAULT_MARKER_PREFIX
    empty_string_marker: str = DEFAULT_EMPTY_STRING_MARKER
    field_terminator: str = "\t"
    row_terminator: str = "\n"

    def encode(self, value: str) -> str:
        """Return a file-safe representation of a text value."""

        if value == "":
            return self.empty_string_marker

        encoded = value
        for char, code in self._ordered_replacements():
            encoded = encoded.replace(char, self.marker_prefix + code)
        return encoded

    def assert_file_safe(self, encoded_value: str) -> None:
        """Fail if an encoded value can still break the configured file shape."""

        if self.field_terminator and self.field_terminator in encoded_value:
            raise ValueError("Encoded bcp value still contains the configured field terminator")
        if self.row_terminator and self.row_terminator in encoded_value:
            raise ValueError("Encoded bcp value still contains the configured row terminator")

    def decode(self, encoded_value: str) -> str:
        """Reverse :meth:`encode` for local integrity/profile validation."""

        if encoded_value == self.empty_string_marker:
            return ""
        decoded = encoded_value
        for char, code in self._decode_replacements():
            decoded = decoded.replace(self.marker_prefix + code, char)
        return decoded

    def mssql_decode_expression(self, value_sql: str) -> str:
        """Render a SQL Server expression that decodes a staged text column."""

        expression = value_sql
        for char, code in self._decode_replacements():
            expression = (
                f"REPLACE({expression}, {self._mssql_literal(self.marker_prefix + code)}, {self._mssql_literal(char)})"
            )
        marker = self._mssql_literal(self.marker_prefix)
        binary_value = f"({value_sql}) COLLATE {_MSSQL_CODEC_BINARY_COLLATION}"
        return (
            f"CASE WHEN {binary_value} = {self._mssql_literal(self.empty_string_marker)} THEN N'' "
            f"WHEN CHARINDEX({marker}, {binary_value}) = 0 "
            f"THEN {value_sql} "
            f"ELSE {expression} END"
        )

    def mssql_encode_expression(self, value_sql: str) -> str:
        """Render a SQL Server expression that encodes a source text column."""

        expression = f"CONVERT(NVARCHAR(MAX), {value_sql})"
        for char, code in self._ordered_replacements():
            expression = (
                f"REPLACE({expression}, {self._mssql_literal(char)}, {self._mssql_literal(self.marker_prefix + code)})"
            )
        return (
            f"CASE WHEN {value_sql} IS NULL THEN NULL "
            f"WHEN {value_sql} = N'' THEN {self._mssql_literal(self.empty_string_marker)} "
            f"ELSE {expression} END"
        )

    def postgres_encode_expression(self, value_sql: str) -> str:
        """Render a PostgreSQL expression that encodes a source text column.

        Compare against ``''`` on ``::text`` so ``json``/``jsonb`` (and other
        non-text sources projected into nvarchar for MSSQL BCP) do not raise
        ``invalid input syntax for type json`` when PostgreSQL coerces the
        empty-string literal to the source type.
        """

        text_sql = f"({value_sql})::text"
        expression = text_sql
        for char, code in self._ordered_replacements():
            expression = (
                f"replace({expression}, "
                f"{self._postgres_literal(char)}, "
                f"{self._postgres_literal(self.marker_prefix + code)})"
            )
        # PostgreSQL's ``composite IS NULL`` is true both for a SQL NULL and
        # for a non-null row whose every attribute is NULL.  ``IS NOT
        # DISTINCT FROM NULL`` tests the scalar null identity and therefore
        # preserves the valid composite text value ``(,)``.
        return (
            f"CASE WHEN {value_sql} IS NOT DISTINCT FROM NULL THEN NULL "
            f"WHEN {text_sql} = '' THEN {self._postgres_literal(self.empty_string_marker)} "
            f"ELSE {expression} END"
        )

    def _ordered_replacements(self) -> list[tuple[str, str]]:
        # The PostgreSQL producer uses CSV with U+001F as QUOTE/ESCAPE while
        # SQL Server consumes the result as an unquoted character BCP file.
        # Therefore the codec must remove *every* character that could make
        # PostgreSQL add CSV framing, independently of the configured BCP row
        # terminator.  Keeping one fixed ordered alphabet also makes the
        # Python, PostgreSQL and MSSQL implementations formal inverses.
        return list(_CONTROL_CODES.items())

    def _decode_replacements(self) -> list[tuple[str, str]]:
        replacements = self._ordered_replacements()
        return [item for item in replacements if item[0] != self.marker_prefix] + [
            item for item in replacements if item[0] == self.marker_prefix
        ]

    @staticmethod
    def _mssql_literal(value: str) -> str:
        parts: list[str] = []
        text_buffer: list[str] = []
        for char in value:
            if ord(char) < 32:
                if text_buffer:
                    parts.append("N'" + "".join(text_buffer).replace("'", "''") + "'")
                    text_buffer = []
                parts.append(f"NCHAR({ord(char)})")
            else:
                text_buffer.append(char)
        if text_buffer:
            parts.append("N'" + "".join(text_buffer).replace("'", "''") + "'")
        return " + ".join(parts) if parts else "N''"

    @staticmethod
    def _postgres_literal(value: str) -> str:
        escaped = []
        for char in value:
            code = ord(char)
            if code < 32:
                escaped.append(f"\\x{code:02x}")
            elif char == "\\":
                escaped.append("\\\\")
            elif char == "'":
                escaped.append("''")
            else:
                escaped.append(char)
        return "E'" + "".join(escaped) + "'"


def is_bulk_text_type(dtype: str) -> bool:
    """Return true when a database type should be text-codec decoded."""

    normalized = dtype.lower().strip()
    return any(token in normalized for token in ("char", "text", "json", "xml", "string"))
