"""Internal bounded decoding and masking for completed dbt subprocess output."""

from __future__ import annotations

import codecs


def _sanitize(
    value: bytes,
    *,
    total_bytes: int,
    limit_bytes: int,
    secrets: tuple[str, ...],
) -> tuple[str, bool]:
    retained_truncated = total_bytes > len(value)
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text = decoder.decode(value, final=not retained_truncated)
    protected = bytearray(len(text))
    for secret in secrets:
        _mask_secret(text, secret, protected, retained_truncated=retained_truncated)
    encoded = _redacted_output(text, protected, limit_bytes=limit_bytes)
    truncated = total_bytes > limit_bytes or len(encoded) > limit_bytes
    return encoded[:limit_bytes].decode("utf-8", errors="ignore"), truncated


def _mask_secret(value: str, secret: str, protected: bytearray, *, retained_truncated: bool) -> None:
    """Mark overlapping matches and the possible unfinished suffix in linear time.

    KMP prefix lengths avoid rescanning a long secret for every overlapping
    occurrence. Each input character is visited a bounded number of times per
    secret, and only newly covered ranges are written. The final matched length
    also identifies the longest secret prefix ending at the retained boundary.
    """

    prefixes = _prefix_lengths(secret)
    matched = 0
    covered_until = 0
    for index, character in enumerate(value):
        while matched and character != secret[matched]:
            matched = prefixes[matched - 1]
        if character == secret[matched]:
            matched += 1
        if matched == len(secret):
            end = index + 1
            start = max(end - len(secret), covered_until)
            protected[start:end] = b"\x01" * (end - start)
            covered_until = end
            matched = prefixes[matched - 1]
    if retained_truncated and matched:
        protected[-matched:] = b"\x01" * matched


def _prefix_lengths(secret: str) -> list[int]:
    """Compute the longest proper prefix ending at each secret character."""

    prefixes = [0] * len(secret)
    matched = 0
    for index in range(1, len(secret)):
        while matched and secret[index] != secret[matched]:
            matched = prefixes[matched - 1]
        if secret[index] == secret[matched]:
            matched += 1
        prefixes[index] = matched
    return prefixes


def _redacted_output(value: str, protected: bytearray, *, limit_bytes: int) -> bytes:
    """Emit covered runs as markers, retaining one byte to detect expansion."""

    output = bytearray()
    position = 0
    while position < len(protected) and len(output) <= limit_bytes:
        masked = protected[position]
        end = protected.find(b"\x00" if masked else b"\x01", position)
        if end < 0:
            end = len(protected)
        segment = b"[REDACTED]" if masked else value[position:end].encode("utf-8")
        output.extend(segment[: limit_bytes + 1 - len(output)])
        position = end
    return bytes(output)
