"""ClickHouse calendar bounds: reject values the server would silently clamp."""

from __future__ import annotations

import struct


def encode_temporal_integer(value: int, root: str, scale: int = 0) -> bytes:
    """Validate semantic calendar bounds as well as integer storage capacity."""
    factor = 10**scale
    bounds = {
        "date": (0, 65536, "H"),
        "date32": (-25567, 120530, "i"),
        "datetime": (0, 2**32, "I"),
        "datetime64": (-2208988800 * factor, min(10413792000 * factor, 2**63), "q"),
    }
    low, high, fmt = bounds[root]
    if not 0 <= scale <= 9 or not low <= value < high:
        raise ValueError("clickhouse_binary_temporal_out_of_range")
    return struct.pack("<" + fmt, value)
