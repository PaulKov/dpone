"""Dependency boundary for immutable columnar range contracts."""

from importlib import import_module

from dpone.contracts.columnar_range_parallelism import (
    ColumnarRangeDescriptor,
    ColumnarRangePlan,
    RangeChunkReceipt,
    RangeEvidenceItem,
    RangeParallelismPolicy,
    RangeStageReceipt,
    columnar_range_fingerprint,
)


def __getattr__(name: str) -> object:
    """Lazily re-export evidence without creating a base-import cycle."""

    if name == "ColumnarRangeExecutionEvidence":
        return import_module("dpone.contracts.columnar_range_evidence").ColumnarRangeExecutionEvidence
    raise AttributeError(name)


__all__ = [
    "ColumnarRangeDescriptor",
    "ColumnarRangeExecutionEvidence",  # noqa: F822 - resolved by module __getattr__
    "ColumnarRangePlan",
    "RangeChunkReceipt",
    "RangeEvidenceItem",
    "RangeParallelismPolicy",
    "RangeStageReceipt",
    "columnar_range_fingerprint",
]
