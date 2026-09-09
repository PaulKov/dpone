"""Synchronous binary ingestion capability for isolated window attempts."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Protocol


class WindowBinaryIngest(Protocol):
    """Validate physical endpoint/format and settle an identified HTTP write."""

    def insert_window_stream(
        self, database: str, table: str, columns: Sequence[str], chunks: Iterable[bytes], query_id: str
    ) -> object:
        """Require synchronous RowBinary, validate database and bind query identity."""
