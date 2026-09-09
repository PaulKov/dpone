"""Type-aware serialization for SQL Server ``bcp`` character files.

Character ``bcp`` expects deterministic text for typed target columns. Python
``str(datetime)`` and lineage ISO-8601 timestamps (``2026-06-04T09:31:00+00:00``)
are not accepted by ``datetime2`` and produce SQLState ``22005``.
"""

from __future__ import annotations

import os
import re
import tempfile
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec, is_bulk_text_type
from dpone.runtime.pinned_directory import PinnedDirectory
from dpone.runtime.pinned_file_consumer import PinnedFileConsumer

_DATETIME2_SCALE = re.compile(r"datetime2\(\s*(\d+)\s*\)", re.IGNORECASE)


class UnsafeBulkValueError(ValueError):
    """Raised when a value cannot be safely written to a BCP character file."""


class MssqlSpoolCapacityError(RuntimeError):
    """Raised before a BCP spool write would exceed its admitted byte budget."""

    code = "mssql_spool_byte_limit_exceeded"

    def __init__(self, *, max_bytes: int, attempted_bytes: int) -> None:
        self.max_bytes = max_bytes
        self.attempted_bytes = attempted_bytes
        self.cleanup_error_code: str | None = None
        self.residue_possible = False
        super().__init__(self.code)


def format_mssql_bcp_scalar(
    value: object,
    *,
    mssql_type: str | None,
    text_codec: BulkTextCodec | None = None,
    null_marker: str = "",
    unsafe_text_policy: str = "fail",
    field_terminator: str = "\t",
) -> str:
    """Render one Python value for a typed MSSQL ``bcp -c`` field."""

    if value is None:
        return null_marker

    if isinstance(value, bytes | bytearray | memoryview):
        # Hex character wire for binary payloads (staging nvarchar or varbinary targets).
        return bytes(value).hex()

    normalized = str(mssql_type or "").strip().lower()
    if normalized == "bit" or isinstance(value, bool):
        return "1" if bool(value) else "0"

    if normalized == "date":
        return _format_bcp_date(value)

    if _is_temporal_type(normalized):
        return _format_bcp_datetime(value, scale=_datetime2_scale(normalized))

    if isinstance(value, datetime):
        return _format_bcp_datetime(value, scale=7)

    if isinstance(value, date):
        return _format_bcp_date(value)

    if normalized == "uniqueidentifier" or isinstance(value, uuid.UUID):
        return _format_bcp_uuid(value)

    if _is_numeric_type(normalized) or isinstance(value, (int, float, Decimal)):
        return _format_bcp_numeric(value)

    text = str(value)
    if text_codec is not None and (not normalized or is_bulk_text_type(normalized) or _is_text_type(normalized)):
        encoded = text_codec.encode(text)
        text_codec.assert_file_safe(encoded)
        return encoded

    if field_terminator in text or "\r" in text or "\n" in text:
        if unsafe_text_policy == "fallback":
            return text.replace(field_terminator, " ").replace("\r", " ").replace("\n", " ")
        raise UnsafeBulkValueError(
            "Value contains the configured bcp field or row terminator. "
            "Choose a safer terminator or set unsafe_text_policy='fallback'."
        )
    return text


def _is_temporal_type(mssql_type: str) -> bool:
    return mssql_type.startswith(("datetime2", "datetime", "smalldatetime"))


def _is_numeric_type(mssql_type: str) -> bool:
    return mssql_type.startswith(("decimal", "numeric", "int", "bigint", "smallint", "tinyint", "real", "float"))


def _is_text_type(mssql_type: str) -> bool:
    return any(token in mssql_type for token in ("char", "text", "json", "xml", "string"))


def _datetime2_scale(mssql_type: str) -> int:
    match = _DATETIME2_SCALE.match(mssql_type)
    if match is not None:
        return max(0, min(7, int(match.group(1))))
    if mssql_type.startswith("smalldatetime"):
        return 0
    return 7


def _format_bcp_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if "T" in text:
        text = text.split("T", 1)[0]
    if " " in text:
        text = text.split(" ", 1)[0]
    return text


def _format_bcp_datetime(value: object, *, scale: int) -> str:
    parsed = _coerce_datetime(value)
    if parsed.tzinfo is not None:
        # mypy validates the Python 3.10 compatibility floor, where datetime.UTC is absent.
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)  # noqa: UP017
    base = parsed.strftime("%Y-%m-%d %H:%M:%S")
    if scale <= 0:
        return base
    fraction = f"{parsed.microsecond:06d}".ljust(7, "0")[:scale]
    return f"{base}.{fraction}"


def _coerce_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


def _format_bcp_uuid(value: object) -> str:
    if isinstance(value, uuid.UUID):
        token = str(value)
    else:
        token = str(value).strip().strip("{}")
    return token.lower()


def _format_bcp_numeric(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


class DelimitedBulkFile:
    """Write rows to a UTF-8 delimited file for legacy character-mode BCP."""

    def __init__(
        self,
        *,
        field_terminator: str = "\t",
        row_terminator: str = "\n",
        null_marker: str = "",
        unsafe_text_policy: str = "fail",
        text_codec: BulkTextCodec | None = None,
        directory: str | None = None,
        pinned_directory: PinnedDirectory | None = None,
        max_bytes: int | None = None,
    ) -> None:
        if max_bytes is not None and (not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 0):
            raise ValueError("max_bytes must be a non-negative integer or None")
        self.field_terminator = field_terminator
        self.row_terminator = row_terminator
        self.null_marker = null_marker
        self.unsafe_text_policy = unsafe_text_policy
        self.text_codec = text_codec
        self.directory = directory
        if pinned_directory is not None and directory is None:
            raise ValueError("directory is required when pinned_directory is provided")
        self.pinned_directory = pinned_directory
        self.max_bytes = max_bytes

    def write_rows(
        self,
        rows: Iterable[Mapping[str, object]],
        columns: Sequence[str],
        *,
        column_types: Mapping[str, str] | None = None,
    ) -> tuple[str, int]:
        path, count, consumer = self._write_rows(
            rows,
            columns,
            column_types=column_types,
            pin_for_consumer=False,
        )
        if consumer is not None:  # pragma: no cover - internal return contract.
            raise RuntimeError("unexpected_pinned_file_consumer")
        return path, count

    def write_rows_for_consumer(
        self,
        rows: Iterable[Mapping[str, object]],
        columns: Sequence[str],
        *,
        column_types: Mapping[str, str] | None = None,
    ) -> tuple[str, int, PinnedFileConsumer]:
        """Write rows and retain the exact file identity for an external reader."""

        if self.pinned_directory is None:
            raise ValueError("pinned_directory is required for external consumer authority")
        path, count, consumer = self._write_rows(
            rows,
            columns,
            column_types=column_types,
            pin_for_consumer=True,
        )
        if consumer is None:  # pragma: no cover - internal return contract.
            raise RuntimeError("pinned_file_consumer_missing")
        return path, count, consumer

    def _write_rows(
        self,
        rows: Iterable[Mapping[str, object]],
        columns: Sequence[str],
        *,
        column_types: Mapping[str, str] | None,
        pin_for_consumer: bool,
    ) -> tuple[str, int, PinnedFileConsumer | None]:
        descriptor, reader_descriptor, entry_name, path, pending_cleanup = self._create_file(
            pin_for_consumer=pin_for_consumer,
        )
        count = 0
        bytes_written = 0
        consumer: PinnedFileConsumer | None = None
        try:
            handle = os.fdopen(descriptor, "wb")
            descriptor = -1
            with handle:
                for row in rows:
                    payload = (
                        self.field_terminator.join(
                            self._normalize_value(
                                row.get(column),
                                mssql_type=(column_types or {}).get(column),
                                typed_columns=column_types is not None,
                            )
                            for column in columns
                        )
                        + self.row_terminator
                    ).encode("utf-8")
                    attempted_bytes = bytes_written + len(payload)
                    if self.max_bytes is not None and attempted_bytes > self.max_bytes:
                        raise MssqlSpoolCapacityError(
                            max_bytes=self.max_bytes,
                            attempted_bytes=attempted_bytes,
                        )
                    handle.write(payload)
                    bytes_written = attempted_bytes
                    count += 1
                if pin_for_consumer:
                    handle.flush()
                    pinned_directory = self.pinned_directory
                    if pinned_directory is None:  # pragma: no cover - guarded by public method.
                        raise RuntimeError("pinned_directory_missing")
                    transferred_reader = reader_descriptor if reader_descriptor >= 0 else None
                    reader_descriptor = -1
                    consumer = pinned_directory.pin_file_for_consumer(
                        entry_name,
                        writer_descriptor=handle.fileno(),
                        reader_descriptor=transferred_reader,
                    )
                    pending_cleanup = None
            return path, count, consumer
        except BaseException as exc:
            if descriptor >= 0:
                os.close(descriptor)
            if reader_descriptor >= 0:
                os.close(reader_descriptor)
            if consumer is not None:
                try:
                    consumer.cleanup()
                except OSError:
                    mark_mssql_spool_cleanup_failure(exc)
            elif pending_cleanup is not None:
                try:
                    pending_cleanup()
                except OSError:
                    mark_mssql_spool_cleanup_failure(exc)
            else:
                try:
                    self._unlink(entry_name, path)
                except OSError:
                    mark_mssql_spool_cleanup_failure(exc)
            raise

    def _create_file(
        self,
        *,
        pin_for_consumer: bool,
    ) -> tuple[int, int, str, str, Callable[[], None] | None]:
        if self.pinned_directory is not None:
            if pin_for_consumer:
                return self.pinned_directory.create_consumer_file(
                    prefix="dpone_mssql_",
                    suffix=".bcp",
                )
            descriptor, entry_name, path = self.pinned_directory.create_file(
                prefix="dpone_mssql_",
                suffix=".bcp",
            )
            return descriptor, -1, entry_name, path, None
        descriptor, path = tempfile.mkstemp(
            prefix="dpone_mssql_",
            suffix=".bcp",
            dir=self.directory,
        )
        return descriptor, -1, Path(path).name, path, None

    def _unlink(self, entry_name: str, path: str) -> None:
        if self.pinned_directory is not None:
            self.pinned_directory.unlink(entry_name, missing_ok=True)
            return
        Path(path).unlink(missing_ok=True)

    def _normalize_value(
        self,
        value: object,
        *,
        mssql_type: str | None = None,
        typed_columns: bool = False,
    ) -> str:
        if typed_columns:
            return format_mssql_bcp_scalar(
                value,
                mssql_type=mssql_type,
                text_codec=self.text_codec,
                null_marker=self.null_marker,
                unsafe_text_policy=self.unsafe_text_policy,
                field_terminator=self.field_terminator,
            )
        if value is None:
            return self.null_marker
        if isinstance(value, bool):
            return "1" if value else "0"
        text = str(value)
        if self.text_codec is not None:
            text = self.text_codec.encode(text)
            self.text_codec.assert_file_safe(text)
            return text
        if self.field_terminator in text or "\r" in text or "\n" in text:
            if self.unsafe_text_policy == "fallback":
                return text.replace(self.field_terminator, " ").replace("\r", " ").replace("\n", " ")
            raise UnsafeBulkValueError(
                "Value contains the configured bcp field or row terminator. "
                "Choose a safer terminator or set unsafe_text_policy='fallback'."
            )
        return text


def mark_mssql_spool_cleanup_failure(error: BaseException) -> None:
    """Attach path-free residue evidence without replacing a primary error."""

    try:
        setattr(error, "cleanup_error_code", "mssql_spool_cleanup_failed")
        setattr(error, "residue_possible", True)
    except (AttributeError, TypeError):  # pragma: no cover - immutable foreign exception.
        pass
    notes = getattr(error, "__notes__", ())
    add_note = getattr(error, "add_note", None)
    if callable(add_note) and "mssql_spool_cleanup_failed" not in notes:
        add_note("mssql_spool_cleanup_failed")


__all__ = [
    "DelimitedBulkFile",
    "MssqlSpoolCapacityError",
    "UnsafeBulkValueError",
    "format_mssql_bcp_scalar",
    "mark_mssql_spool_cleanup_failure",
]
