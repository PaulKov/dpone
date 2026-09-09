"""Connector-neutral sink binary encoding contracts and evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

SINK_BINARY_SCHEMA_VERSION = "dpone.native_transfer.sink_binary.v1"
SinkBinaryFormat = Literal["rowbinary", "native"]


@dataclass(slots=True)
class SinkBinaryEvidence:
    """Runtime evidence for one sink binary encoding stream."""

    format: SinkBinaryFormat
    input_format: str
    block_count: int = 0
    rows: int = 0
    encoded_bytes: int = 0
    failure_code: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["schema_version"] = SINK_BINARY_SCHEMA_VERSION
        return payload


__all__ = ["SINK_BINARY_SCHEMA_VERSION", "SinkBinaryEvidence", "SinkBinaryFormat"]
