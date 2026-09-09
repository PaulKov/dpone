from __future__ import annotations

from typing import Any

from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.native_acceleration import NativeAccelerationEvidence, NativeAccelerationPolicy
from dpone.runtime.native_wire_models import NativeWireEvidence
from dpone.runtime.sink_binary import SinkBinaryEvidence


def bytes_value(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    units = {"kib": 1024, "mib": 1024**2, "gib": 1024**3, "kb": 1000, "mb": 1000**2, "gb": 1000**3}
    suffix = "".join(ch for ch in text.lower() if ch.isalpha())
    number = text[: len(text) - len(suffix)] if suffix else text
    return int(float(number.strip()) * units.get(suffix, 1))


def build_acceleration_policy(bulk_wire: Any) -> NativeAccelerationPolicy:
    decision = getattr(bulk_wire, "acceleration", None)
    mode = getattr(decision, "requested_mode", None)
    if mode is not None:
        return NativeAccelerationPolicy(mode=str(mode))
    return NativeAccelerationPolicy()


def attach_stream_contracts(
    stream: ByteStreamArtifact,
    *,
    bulk_wire_contract: Any,
    native_wire_evidence: NativeWireEvidence,
    sink_binary_evidence: SinkBinaryEvidence,
    native_acceleration_evidence: NativeAccelerationEvidence,
) -> None:
    setattr(stream, "bulk_wire_contract", bulk_wire_contract)
    setattr(stream, "native_wire_evidence", native_wire_evidence)
    setattr(stream, "sink_binary_evidence", sink_binary_evidence)
    setattr(stream, "native_acceleration_evidence", native_acceleration_evidence)
