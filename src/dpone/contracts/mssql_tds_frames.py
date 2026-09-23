"""Single bounded private TDS frame; EOF observation belongs to its caller."""

_ERROR = "mssql_native.tds_result_protocol"


class TdsMessageFrame:
    """Share exact framing across startup, private work and completion channels."""

    def __init__(self, *, max_payload: int) -> None:
        if type(max_payload) is not int or not 0 < max_payload <= 1024 * 1024:
            raise ValueError(_ERROR)
        self._max_payload = max_payload
        self._header = bytearray()
        self._body = bytearray()
        self._length: int | None = None
        self._sealed = False

    def feed(self, data: bytes) -> None:
        """Accept a bounded channel fragment; never truncate excess bytes."""
        try:
            if self._sealed or type(data) is not bytes:
                raise ValueError(_ERROR)
            offset = min(4 - len(self._header), len(data))
            self._header.extend(data[:offset])
            if len(self._header) < 4:
                return
            if self._length is None:
                self._length = int.from_bytes(self._header, "big")
                if not 0 < self._length <= self._max_payload:
                    raise ValueError(_ERROR)
            if len(data) - offset > self._length - len(self._body):
                raise ValueError(_ERROR)
            self._body.extend(memoryview(data)[offset:])
        except Exception:
            self._sealed = True
            raise ValueError(_ERROR) from None

    def finish(self) -> bytes:
        """Seal after observed EOF; reject incomplete or previously poisoned input."""
        try:
            if self._sealed or self._length is None or len(self._body) != self._length:
                raise ValueError(_ERROR)
            return bytes(self._body)
        finally:
            self._sealed = True


def encode_message(payload: bytes, *, max_payload: int) -> bytes:
    """Prefix an admitted nonempty payload with a four-byte big-endian length."""
    if type(max_payload) is not int or not 0 < max_payload <= 1024 * 1024:
        raise ValueError(_ERROR)
    if type(payload) is not bytes or not 0 < len(payload) <= max_payload:
        raise ValueError(_ERROR)
    return len(payload).to_bytes(4, "big") + payload
