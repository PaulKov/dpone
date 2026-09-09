"""Legacy source-key profile compatibility API.

The governed XMin snapshot path performs relational validation in MSSQL native
staging. This small in-memory adapter remains importable only for direct legacy
callers and is not part of extraction composition.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any


class PostgresMssqlKeyProfile:
    """Legacy explicit key profiler; not used by snapshot extraction."""

    def __init__(self, directory: str | Path) -> None:
        del directory
        self._identities: set[bytes] = set()

    def observe(
        self,
        values: Sequence[bytes],
        *,
        key_columns: Sequence[str],
        text_key_columns: frozenset[str],
        codec: Any,
    ) -> None:
        """Admit one exact composite key under the legacy local policy."""

        frames: list[bytes] = []
        for column, raw in zip(key_columns, values, strict=True):
            if raw == b"":
                raise ValueError("postgres_xmin_source_null_key")
            if column in text_key_columns:
                decoded = _decode_text(codec, raw)
                if decoded.endswith(" "):
                    raise ValueError("postgres_xmin_text_key_trailing_space_unsupported")
                canonical = decoded.encode("utf-16le")
                tag = b"T"
            else:
                canonical = raw
                tag = b"B"
            frames.append(tag + len(canonical).to_bytes(8, "big") + canonical)
        identity = b"".join(frames)
        if identity in self._identities:
            raise ValueError("postgres_xmin_source_key_collision")
        self._identities.add(identity)

    def close(self) -> None:
        """Release compatibility state."""

        self._identities.clear()

    def __enter__(self) -> PostgresMssqlKeyProfile:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _decode_text(codec: Any, raw: bytes) -> str:
    encoded = raw.decode("utf-8")
    decoder = getattr(codec, "decode", None)
    if callable(decoder):
        return str(decoder(encoded))
    marker = str(getattr(codec, "marker_prefix", "\x1d"))
    empty = str(getattr(codec, "empty_string_marker", f"{marker}E"))
    if encoded == empty:
        return ""
    replacements = (("T", "\t"), ("R", "\r"), ("N", "\n"), ("U", "\x1f"), ("S", "\x1e"), ("P", marker))
    for code, value in replacements:
        encoded = encoded.replace(marker + code, value)
    return encoded


__all__ = ["PostgresMssqlKeyProfile"]
