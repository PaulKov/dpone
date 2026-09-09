"""Shared bulk text codec facade for runtime sources and sinks."""

from __future__ import annotations

from dpone.runtime.connectors.bulk_text_codec import (
    DEFAULT_EMPTY_STRING_MARKER,
    BulkTextCodec,
    is_bulk_text_type,
)

__all__ = ["DEFAULT_EMPTY_STRING_MARKER", "BulkTextCodec", "is_bulk_text_type"]
