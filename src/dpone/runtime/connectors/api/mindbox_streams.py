"""Stream helpers for Mindbox export downloads."""

from __future__ import annotations


class _PrependedBytesIO:
    """A lightweight wrapper that prepends bytes to a stream."""

    __slots__ = ("_prepended", "_stream", "_prepended_pos")

    def __init__(self, prepended: bytes, stream):
        self._prepended = prepended
        self._stream = stream
        self._prepended_pos = 0

    def read(self, size: int = -1) -> bytes:
        if self._prepended_pos < len(self._prepended):
            if size == -1:
                result = self._prepended[self._prepended_pos :] + self._stream.read()
                self._prepended_pos = len(self._prepended)
                return result
            remaining = len(self._prepended) - self._prepended_pos
            if size <= remaining:
                result = self._prepended[self._prepended_pos : self._prepended_pos + size]
                self._prepended_pos += size
                return result
            result = self._prepended[self._prepended_pos :]
            self._prepended_pos = len(self._prepended)
            return result + self._stream.read(size - remaining)
        return self._stream.read(size)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def flush(self) -> None:
        if hasattr(self._stream, "flush"):
            self._stream.flush()

    def close(self) -> None:
        if hasattr(self._stream, "close"):
            self._stream.close()

    @property
    def closed(self) -> bool:
        return bool(getattr(self._stream, "closed", False))


__all__ = ["_PrependedBytesIO"]
